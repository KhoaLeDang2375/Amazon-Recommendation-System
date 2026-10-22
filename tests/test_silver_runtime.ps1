# Host-only fault checks. Every Docker command is mocked; no real container/raw access.
$ErrorActionPreference = 'Stop'
$sourceRepo = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$testDirectory = Join-Path $sourceRepo ('outputs/validation/silver-runtime-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $testDirectory -Force | Out-Null
$repo = Join-Path $testDirectory 'TestRepo'
foreach ($file in @('tools/silver/Run-Silver.ps1', 'ingestion/scripts/Get-ExperimentScope.ps1', 'ingestion/manifests/experiment-scope.json')) {
    $target = Join-Path $repo $file
    New-Item -ItemType Directory -Path (Split-Path -Parent $target) -Force | Out-Null
    Copy-Item -LiteralPath (Join-Path $sourceRepo $file) -Destination $target
}
$fakeDisk = Join-Path $testDirectory 'fixture.vhdx'
Set-Content -LiteralPath $fakeDisk -Value 'fixture, not a real disk'
$global:silverFault = ''
$global:silverCalls = [Collections.Generic.List[string]]::new()
function docker {
    $global:LASTEXITCODE = 0
    $global:silverCalls.Add(($args -join ' '))
    if ($args[0] -eq 'ps' -and $global:silverFault -eq 'active') { 'existing-notebook' }
    if ($args -contains '-safemode') { 'Safe mode is OFF' }
    if ($args -contains 'fsck') {
        if ($global:silverFault -eq 'fsck') { 'is CORRUPT' } else { 'is HEALTHY' }
    }
    if ($args -contains '-du') { "1 1 $($args[-1])" }
    if ($args -contains '--detach') {
        $runId = $args[[array]::IndexOf($args, '--run-id') + 1]
        $folder = Join-Path $repo "outputs/processing/silver/$runId"
        @{ status = $(if ($global:silverFault -eq 'manifest') { 'running' } else { 'complete' });
           category = 'All_Beauty'; run_id = $runId; final_path = '/fixture/final' } |
            ConvertTo-Json | Set-Content -LiteralPath (Join-Path $folder 'run.json') -Encoding UTF8
        'a' * 64
    }
    if ($args[0] -eq 'inspect') {
        @{ Running = $false; ExitCode = $(if ($global:silverFault -eq 'exit') { 137 } else { 0 }); OOMKilled = ($global:silverFault -eq 'exit') } | ConvertTo-Json
    }
    if ($args[0] -eq 'logs') {
        if ($global:silverFault -eq 'logs') { $global:LASTEXITCODE = 7; return }
        & $env:ComSpec /d /c 'echo fixture stdout & echo fixture stderr 1>&2 & exit /b 0'
        $global:LASTEXITCODE = $LASTEXITCODE
    }
    if ($args[0] -eq 'rm' -and $global:silverFault -eq 'rm') { $global:LASTEXITCODE = 9 }
}

function Invoke-Case {
    param([string]$Fault, [string]$ExpectedError = '', [int]$Reserve = 20)
    $global:silverFault = $Fault
    $global:silverCalls.Clear()
    $message = ''
    try { $result = & (Join-Path $repo 'tools/silver/Run-Silver.ps1') -DockerDiskPath $fakeDisk -MinFreeGiB $Reserve }
    catch { $message = $_.Exception.Message }
    if ($ExpectedError -and $message -notmatch $ExpectedError) { throw "Fault=$Fault did not report expected error; got: $message" }
    if (-not $ExpectedError -and $message) { throw "Success failed: $message" }
    $created = @($global:silverCalls | Where-Object { $_ -match ' --detach ' })
    if ($created.Count) {
        if (@($global:silverCalls | Where-Object { $_ -eq ('rm ' + ('a' * 64)) }).Count -ne 1) { throw 'Cleanup did not target the owned ID exactly once' }
    }
    $handle = [IO.File]::Open((Join-Path $repo 'outputs/eda/.eda.lock'), [IO.FileMode]::OpenOrCreate, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
    $handle.Dispose()
    if ($Fault -in @('active', 'fsck', 'space') -and $created.Count) { throw 'Preflight failure created a container' }
    if (-not $ExpectedError) {
        if ($created[0] -notmatch 'compose.silver-light.yaml.*python -m processing.silver.*--cores 2 --shuffle-partitions 64') { throw 'Silver job/overlay/config not selected' }
        if ((Get-Content -LiteralPath $result.LogPath -Raw) -notmatch 'fixture stderr') { throw 'Native stderr log lost' }
        $samples = @(Get-Content -LiteralPath (Join-Path $result.ReportDirectory 'host.resources.jsonl') | ForEach-Object { $_ | ConvertFrom-Json })
        if ($samples.Count -lt 2 -or $samples[0].vhdx_bytes -le 0) { throw 'Host samples absent' }
    }
}

try {
    Invoke-Case ''
    Invoke-Case active 'already running'
    Invoke-Case fsck 'not HEALTHY'
    Invoke-Case space 'below Silver budget' 999999
    Invoke-Case exit 'exit=137'
    Invoke-Case manifest 'did not confirm'
    Invoke-Case logs 'cleanup failed'
    Invoke-Case rm 'cleanup failed'
    $global:silverFault = ''
    $held = [IO.File]::Open((Join-Path $repo 'outputs/eda/.eda.lock'), [IO.FileMode]::OpenOrCreate, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
    try {
        $global:silverCalls.Clear()
        $rejected = $false
        try { & (Join-Path $repo 'tools/silver/Run-Silver.ps1') -DockerDiskPath $fakeDisk } catch { $rejected = $true }
        if (-not $rejected -or $global:silverCalls.Count) { throw 'Shared EDA lock was bypassed' }
    } finally { $held.Dispose() }
    Write-Output 'PASS: Silver runtime success, native stderr, shared lock, active job, fsck, disk reserve, OOM/exit, manifest, log and cleanup failures.'
} finally {
    Remove-Item Function:\docker -ErrorAction SilentlyContinue
}
