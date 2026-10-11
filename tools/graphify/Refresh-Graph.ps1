$ErrorActionPreference = 'Stop'
Push-Location (Join-Path $PSScriptRoot '../..')
try {
    docker compose --profile tools run --rm --no-deps graphify extract . --code-only --no-cluster --force
    if ($LASTEXITCODE -ne 0) { throw 'Graphify extraction failed' }
    docker compose --profile tools run --rm --no-deps --entrypoint python graphify /workspace/tools/graphify/enrich_graph.py
    if ($LASTEXITCODE -ne 0) { throw 'Graph enrichment failed' }
    docker compose --profile tools run --rm --no-deps graphify cluster-only . --no-label
    if ($LASTEXITCODE -ne 0) { throw 'Graphify clustering/export failed' }
} finally {
    Pop-Location
}
