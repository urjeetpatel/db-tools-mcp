# mcp_server_v2.py
"""
Database Metadata MCP Server (FastMCP v2, stdio transport)
Run with: python mcp_server_v2.py
"""

import json
import os
import glob
from collections import deque
from typing import List, Dict, Any

from fastmcp import FastMCP
from loguru import logger

METADATA_DIR = os.environ.get("MCP_METADATA_DIR", "./metadata_cache")

mcp = FastMCP(name="Metadata-MCP")


def _load() -> Dict[str, Any]:
    """Load all per-source JSON files from METADATA_DIR and return as {sources: {name: data}}."""
    sources = {}
    pattern = os.path.join(METADATA_DIR, "*.json")
    for path in glob.glob(pattern):
        source_name = os.path.splitext(os.path.basename(path))[0]
        with open(path, "r", encoding="utf-8") as f:
            sources[source_name] = json.load(f)
        logger.info(f"Loaded metadata for source '{source_name}' from {path}")
    return {"sources": sources}


@mcp.tool
def list_sources() -> List[str]:
    """List available sources (e.g., sqlserver, snowflake)."""
    return list(_load().get("sources", {}).keys())


@mcp.tool
def list_schemas(source: str) -> List[str]:
    """List schemas for a source."""
    return list(_load()["sources"][source]["schemas"].keys())


@mcp.tool
def list_tables(source: str, schema: str) -> List[str]:
    """List tables for a schema."""
    return list(_load()["sources"][source]["schemas"][schema]["tables"].keys())


@mcp.tool
def get_table(source: str, schema: str, table: str) -> Dict[str, Any]:
    """Get columns & constraints for a table, including inbound/outbound FK summaries."""
    data = _load()["sources"][source]["schemas"][schema]
    t = data["tables"][table]
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


@mcp.tool
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


@mcp.tool
def suggest_joins(
    source: str, table_a: str, table_b: str, max_hops: int = 2
) -> List[Dict[str, Any]]:
    """
    Suggest join paths (FK graph + heuristic pairs when FKs are absent).
    Output ordered by descending confidence.
    """
    data = _load()["sources"][source]["schemas"]
    edges = []
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

    def neighbors(node):
        return [e for e in edges if e["from"] == node or e["to"] == node]

    paths = []
    q = deque([([table_a], [])])
    seen = {table_a}
    while q:
        nodes, es = q.popleft()
        cur = nodes[-1]
        if len(nodes) - 1 > max_hops:
            continue
        if cur == table_b and es:
            score = 1.0
            for e in es:
                score *= e["score"]
            paths.append(
                {"tables": nodes[:], "edges": es[:], "confidence": round(score, 3)}
            )
            continue
        for e in neighbors(cur):
            nxt = e["to"] if e["from"] == cur else e["from"]
            if nxt in seen:
                continue
            seen.add(nxt)
            q.append((nodes + [nxt], es + [e]))

    paths.sort(
        key=lambda x: (
            -x["confidence"],
            -sum(1 for e in x["edges"] if e["kind"] == "fk"),
        )
    )
    return paths[:10]


if __name__ == "__main__":
    mcp.run()  # stdio transport by default
