# Host-only fault checks. Every Docker command is mocked; no real container/raw access.
$ErrorActionPreference = 'Stop'
$sourceRepo = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$testDirectory = Join-Path $sourceRepo ('outputs/validation/gold-runtime-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $testDirectory -Force | Out-Null
$repo = Join-Path $testDirectory 'TestRepo'
foreach ($file in @('tools/gold/Run-Gold.ps1', 'ingestion/scripts/Get-ExperimentScope.ps1', 'ingestion/manifests/experiment-scope.json', 'processing/gold_protocol.json')) {
    $target = Join-Path $repo $file
    New-Item -ItemType Directory -Path (Split-Path -Parent $target) -Force | Out-Null
    Copy-Item -LiteralPath (Join-Path $sourceRepo $file) -Destination $target
}
$fakeDisk = Join-Path $testDirectory 'fixture.vhdx'
Set-Content -LiteralPath $fakeDisk -Value 'fixture, not a real disk'
$global:goldFault = ''
$global:goldCalls = [Collections.Generic.List[string]]::new()
function docker {
    $global:LASTEXITCODE = 0
    $global:goldCalls.Add(($args -join ' '))
    if ($args[0] -eq 'ps' -and $global:goldFault -eq 'active') { 'existing-notebook' }
    if ($args -contains '-safemode') { 'Safe mode is OFF' }
    if ($args -contains 'fsck') {
        if ($global:goldFault -eq 'fsck') { 'is CORRUPT' } else { 'is HEALTHY' }
    }
    if ($args -contains '-du') { "1 1 $($args[-1])" }
    if ($args -contains '--detach') {
        $runId = $args[[array]::IndexOf($args, '--run-id') + 1]
        $folder = Join-Path $repo "outputs/processing/gold/$runId"
        @{ status = $(if ($global:goldFault -eq 'manifest') { 'running' } else { 'complete' });
           category = 'All_Beauty'; run_id = $runId; final_path = '/fixture/final' } |
            ConvertTo-Json | Set-Content -LiteralPath (Join-Path $folder 'run.json') -Encoding UTF8
        'a' * 64
    }
    if ($args[0] -eq 'inspect') {
        @{ Running = $false; ExitCode = $(if ($global:goldFault -eq 'exit') { 137 } else { 0 }); OOMKilled = ($global:goldFault -eq 'exit') } | ConvertTo-Json
    }
    if ($args[0] -eq 'logs') {
        if ($global:goldFault -eq 'logs') { $global:LASTEXITCODE = 7; return }
        & $env:ComSpec /d /c 'echo fixture stdout & echo fixture stderr 1>&2 & exit /b 0'
        $global:LASTEXITCODE = $LASTEXITCODE
    }
    if ($args[0] -eq 'rm' -and $global:goldFault -eq 'rm') { $global:LASTEXITCODE = 9 }
}

function Invoke-Case {
    param([string]$Fault, [string]$ExpectedError = '', [int]$Reserve = 20, [string]$ParentRun = '')
    $global:goldFault = $Fault
    $global:goldCalls.Clear()
    $message = ''
    try {
        $options = @{ DockerDiskPath = $fakeDisk; MinFreeGiB = $Reserve }
        if ($ParentRun) { $options.TextOnlyFromRun = $ParentRun }
        $result = & (Join-Path $repo 'tools/gold/Run-Gold.ps1') @options
    }
    catch { $message = $_.Exception.Message }
    if ($ExpectedError -and $message -notmatch $ExpectedError) { throw "Fault=$Fault did not report expected error; got: $message" }
    if (-not $ExpectedError -and $message) { throw "Success failed: $message" }
    $created = @($global:goldCalls | Where-Object { $_ -match ' --detach ' })
    if ($created.Count) {
        if (@($global:goldCalls | Where-Object { $_ -eq ('rm ' + ('a' * 64)) }).Count -ne 1) { throw 'Cleanup did not target the owned ID exactly once' }
    }
    $handle = [IO.File]::Open((Join-Path $repo 'outputs/eda/.eda.lock'), [IO.FileMode]::OpenOrCreate, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
    $handle.Dispose()
    if ($Fault -in @('active', 'fsck', 'space') -and $created.Count) { throw 'Preflight failure created a container' }
    if (-not $ExpectedError) {
        if ($created[0] -notmatch 'compose.gold-light.yaml.*python -m processing.gold.*--cores 2 --shuffle-partitions 64') { throw 'Gold job/overlay/config not selected' }
        if ($ParentRun -and $created[0] -notmatch "--text-only-from-run $ParentRun") { throw 'G2 parent run not forwarded' }
        if ((Get-Content -LiteralPath $result.LogPath -Raw) -notmatch 'fixture stderr') { throw 'Native stderr log lost' }
        $samples = @(Get-Content -LiteralPath (Join-Path $result.ReportDirectory 'host.resources.jsonl') | ForEach-Object { $_ | ConvertFrom-Json })
        if ($samples.Count -lt 2 -or $samples[0].vhdx_bytes -le 0) { throw 'Host samples absent' }
    }
}

try {
    Invoke-Case ''
    Invoke-Case '' -ParentRun 'run_parent_12345678'
    Invoke-Case active 'already running'
    Invoke-Case fsck 'not HEALTHY'
    Invoke-Case space 'below Gold budget' 999999
    Invoke-Case exit 'exit=137'
    Invoke-Case manifest 'did not confirm'
    Invoke-Case logs 'cleanup failed'
    Invoke-Case rm 'cleanup failed'
    $global:goldFault = ''
    $held = [IO.File]::Open((Join-Path $repo 'outputs/eda/.eda.lock'), [IO.FileMode]::OpenOrCreate, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
    try {
        $global:goldCalls.Clear()
        $rejected = $false
        try { & (Join-Path $repo 'tools/gold/Run-Gold.ps1') -DockerDiskPath $fakeDisk } catch { $rejected = $true }
        if (-not $rejected -or $global:goldCalls.Count) { throw 'Shared EDA lock was bypassed' }
    } finally { $held.Dispose() }
    Write-Output 'PASS: Gold runtime success, native stderr, shared lock, active job, fsck, disk reserve, OOM/exit, manifest, log and cleanup failures.'
} finally {
    Remove-Item Function:\docker -ErrorAction SilentlyContinue
}
