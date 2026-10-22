$ErrorActionPreference = 'Stop'
Push-Location (Join-Path $PSScriptRoot '../..')
try {
    docker compose -f compose.yaml -f compose.datanodes.yaml --profile tools run --rm --pull never --no-deps graphify extract . --code-only --no-cluster --force
    if ($LASTEXITCODE -ne 0) { throw 'Graphify extraction failed' }
    docker compose -f compose.yaml -f compose.datanodes.yaml --profile tools run --rm --pull never --no-deps --entrypoint python graphify /workspace/tools/graphify/enrich_graph.py
    if ($LASTEXITCODE -ne 0) { throw 'Graph enrichment failed' }
    docker compose -f compose.yaml -f compose.datanodes.yaml --profile tools run --rm --pull never --no-deps graphify cluster-only . --no-label
    if ($LASTEXITCODE -ne 0) { throw 'Graphify clustering/export failed' }
} finally {
    Pop-Location
}
