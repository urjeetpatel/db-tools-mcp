# Activate the virtual environment and run the refresh script
Set-Location $PSScriptRoot
& "$PSScriptRoot\venv\Scripts\Activate.ps1"
& uvicorn mcp_server:app --host 127.0.0.1 --port 8002 --reload
