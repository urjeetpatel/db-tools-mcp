# mcp_server.py
"""
Database Metadata MCP Server with Starlette mounting
Run with: uvicorn mcp_server:app --host 127.0.0.1 --port 8002 --reload
"""

import json, os, contextlib
from typing import List, Dict, Any
from starlette.applications import Starlette
from starlette.routing import Mount
from mcp.server.fastmcp import FastMCP

METADATA_PATH = os.environ.get("MCP_METADATA_PATH", "./metadata_cache/metadata.json")

# Create the MCP server with stateless HTTP support
mcp = FastMCP("Metadata-MCP", stateless_http=True, json_response=True)


def _load() -> Dict[str, Any]:
    with open(METADATA_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


@mcp.tool()
def list_sources() -> List[str]:
    """List available sources (e.g., sqlserver, snowflake)."""
    return list(_load().get("sources", {}).keys())


@mcp.tool()
def list_schemas(source: str) -> List[str]:
    """List schemas for a source."""
    return list(_load()["sources"][source]["schemas"].keys())


@mcp.tool()
def list_tables(source: str, schema: str) -> List[str]:
    """List tables for a schema."""
    return list(_load()["sources"][source]["schemas"][schema]["tables"].keys())


@mcp.tool()
def get_table(source: str, schema: str, table: str) -> Dict[str, Any]:
    """Get columns & constraints for a table."""
    data = _load()["sources"][source]["schemas"][schema]
    t = data["tables"][table]
    # include inbound/outbound FK summaries
    inbound, outbound = [], []
    for fk in data["foreign_keys"]:
        if fk["parent"]["schema"] == schema and fk["parent"]["table"] == table:
            inbound.append(fk)
        if fk["child"]["schema"] == schema and fk["child"]["table"] == table:
            outbound.append(fk)
    return {
        "columns": t["columns"],
        "foreign_keys_inbound": inbound,
        "foreign_keys_outbound": outbound,
    }


@mcp.tool()
def find_direct_joins(source: str, table_a: str, table_b: str) -> List[Dict[str, Any]]:
    """Return FK-defined direct joins (either direction) between two tables within the same source."""
    res = []
    for sch, sdata in _load()["sources"][source]["schemas"].items():
        for fk in sdata["foreign_keys"]:
            c = f"{fk['child']['schema']}.{fk['child']['table']}"
            p = f"{fk['parent']['schema']}.{fk['parent']['table']}"
            if {c, p} == {table_a, table_b}:
                res.append(fk)
    return res


@mcp.tool()
def suggest_joins(
    source: str, table_a: str, table_b: str, max_hops: int = 2
) -> List[Dict[str, Any]]:
    """
    Suggest join paths (FK graph + heuristic pairs when FKs are absent).
    Output ordered by descending confidence.
    """
    data = _load()["sources"][source]["schemas"]
    # Build graph edges (FKs)
    edges = []
    heuristics = []
    for sch, sdata in data.items():
        for fk in sdata["foreign_keys"]:
            edges.append(
                {
                    "from": f"{fk['child']['schema']}.{fk['child']['table']}",
                    "to": f"{fk['parent']['schema']}.{fk['parent']['table']}",
                    "kind": "fk",
                    "pairs": fk["pairs"],
                    "name": fk["name"],
                    "score": 0.95,
                }
            )
        for (c1, c2, c3), (p1, p2, p3), score, reason in sdata.get("heuristics", []):
            edges.append(
                {
                    "from": f"{c1}.{c2}",
                    "to": f"{p1}.{p2}",
                    "kind": "heuristic",
                    "pairs": [{"child_col": c3, "parent_col": p3}],
                    "name": f"heur_{c2}_{p2}",
                    "score": score,
                    "reason": reason,
                }
            )
            heuristics.append(edges[-1])

    # Tiny BFS up to max_hops
    from collections import deque

    def neighbors(node):
        return [e for e in edges if e["from"] == node or e["to"] == node]

    paths = []
    q = deque([([table_a], [])])
    seen = {table_a}
    while q:
        nodes, es = q.popleft()
        cur = nodes[-1]
        if len(nodes) - 1 > max_hops:  # hops == edges count
            continue
        if cur == table_b and es:
            # score: product-like; prefer FK over heuristic
            score = 1.0
            for e in es:
                score *= e["score"]
            paths.append(
                {"tables": nodes[:], "edges": es[:], "confidence": round(score, 3)}
            )
            continue
        for e in neighbors(cur):
            nxt = e["to"] if e["from"] == cur else e["from"]
            if nxt in nodes:  # avoid cycles
                continue
            q.append((nodes + [nxt], es + [e]))
    # sort by confidence desc, FK-first within similar scores
    paths.sort(
        key=lambda x: (
            -x["confidence"],
            -sum(1 for e in x["edges"] if e["kind"] == "fk"),
        )
    )
    return paths[:10]


# Create lifespan to manage session manager
@contextlib.asynccontextmanager
async def lifespan(app: Starlette):
    async with mcp.session_manager.run():
        yield


# Create the Starlette app and mount the MCP server
app = Starlette(
    routes=[
        Mount("/", mcp.streamable_http_app()),
    ],
    lifespan=lifespan,
)

# Note: Clients connect to http://localhost:8002/
# The MCP protocol endpoints will be available at the root path

if __name__ == "__main__":
    import uvicorn

    # Start the server on port 8002
    uvicorn.run(app, host="127.0.0.1", port=8002)
