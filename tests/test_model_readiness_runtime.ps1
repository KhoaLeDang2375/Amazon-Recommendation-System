# Host-only fault checks. Every Docker command is mocked; no real container/raw access.
$ErrorActionPreference = 'Stop'
$sourceRepo = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$testDirectory = Join-Path $sourceRepo ('outputs/validation/readiness-runtime-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $testDirectory -Force | Out-Null
$repo = Join-Path $testDirectory 'TestRepo'
foreach ($file in @('tools/eda/Run-ModelReadiness.ps1', 'ingestion/scripts/Get-ExperimentScope.ps1', 'ingestion/manifests/experiment-scope.json')) {
    $target = Join-Path $repo $file
    New-Item -ItemType Directory -Path (Split-Path -Parent $target) -Force | Out-Null
    Copy-Item -LiteralPath (Join-Path $sourceRepo $file) -Destination $target
}
$fakeDisk = Join-Path $testDirectory 'fixture.vhdx'
Set-Content -LiteralPath $fakeDisk -Value 'fixture, not a real disk'
$global:readinessFault = ''
$global:readinessCalls = [Collections.Generic.List[string]]::new()
function docker {
    $global:LASTEXITCODE = 0
    $global:readinessCalls.Add(($args -join ' '))
    if ($args[0] -eq 'ps' -and $global:readinessFault -eq 'active') { 'existing-notebook' }
    if ($args -contains '-safemode') { 'Safe mode is OFF' }
    if ($args -contains 'fsck') {
        if ($global:readinessFault -eq 'fsck') { 'is CORRUPT' } else { 'is HEALTHY' }
    }
    if ($args -contains '-du') { "1 1 $($args[-1])" }
    if ($args -contains '--detach') {
        $runId = $args[[array]::IndexOf($args, '--run-id') + 1]
        $folder = Join-Path $repo "outputs/eda_model_readiness/$runId"
        New-Item -ItemType Directory -Path $folder -Force | Out-Null
        @{ status = $(if ($global:readinessFault -eq 'manifest') { 'running' } else { 'complete' });
           category = 'All_Beauty'; run_id = $runId; silver_run = 'run_20261016T021632209982Z';
           kind = $(if ($args -contains '--test-start') { 'cutoff_test_diagnostic' } else { 'cutoff_selection' }) } |
            ConvertTo-Json | Set-Content -LiteralPath (Join-Path $folder 'run.json') -Encoding UTF8
        'a' * 64
    }
    if ($args[0] -eq 'inspect') {
        @{ Running = $false; ExitCode = $(if ($global:readinessFault -eq 'exit') { 137 } else { 0 }); OOMKilled = ($global:readinessFault -eq 'exit') } | ConvertTo-Json
    }
    if ($args[0] -eq 'logs') {
        if ($global:readinessFault -eq 'logs') { $global:LASTEXITCODE = 7; return }
        & $env:ComSpec /d /c 'echo fixture stdout & echo fixture stderr 1>&2 & exit /b 0'
        $global:LASTEXITCODE = $LASTEXITCODE
    }
    if ($args[0] -eq 'rm' -and $global:readinessFault -eq 'rm') { $global:LASTEXITCODE = 9 }
}

function Invoke-Case {
    param([string]$Fault, [string]$ExpectedError = '', [int]$Reserve = 20)
    $global:readinessFault = $Fault
    $global:readinessCalls.Clear()
    $message = ''
    try { $result = & (Join-Path $repo 'tools/eda/Run-ModelReadiness.ps1') -Category All_Beauty -SilverRun run_20261016T021632209982Z -DockerDiskPath $fakeDisk -MinFreeGiB $Reserve }
    catch { $message = $_.Exception.Message }
    if ($ExpectedError -and $message -notmatch $ExpectedError) { throw "Fault=$Fault did not report expected error; got: $message" }
    if (-not $ExpectedError -and $message) { throw "Success failed: $message" }
    $created = @($global:readinessCalls | Where-Object { $_ -match ' --detach ' })
    if ($created.Count) {
        if (@($global:readinessCalls | Where-Object { $_ -eq ('rm ' + ('a' * 64)) }).Count -ne 1) { throw 'Cleanup did not target the owned ID exactly once' }
    }
    $handle = [IO.File]::Open((Join-Path $repo 'outputs/eda/.eda.lock'), [IO.FileMode]::OpenOrCreate, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
    $handle.Dispose()
    if ($Fault -in @('active', 'fsck', 'space') -and $created.Count) { throw 'Preflight failure created a container' }
    if (-not $ExpectedError) {
        if ($created[0] -notmatch 'compose.eda-light.yaml.*eda_model_readiness.py.*--cores 2 --shuffle-partitions 64') { throw 'Readiness job/overlay/config not selected' }
        if ((Get-Content -LiteralPath $result.LogPath -Raw) -notmatch 'fixture stderr') { throw 'Native stderr log lost' }
        $samples = @(Get-Content -LiteralPath (Join-Path $repo "outputs/eda_model_readiness/$($result.RunId).host.resources.jsonl") | ForEach-Object { $_ | ConvertFrom-Json })
        if ($samples.Count -lt 2 -or $samples[0].vhdx_bytes -le 0) { throw 'Host samples absent' }
    }
}

try {
    Invoke-Case ''
    $global:readinessCalls.Clear()
    & (Join-Path $repo 'tools/eda/Run-ModelReadiness.ps1') -Category All_Beauty -SilverRun run_20261016T021632209982Z -DockerDiskPath $fakeDisk -CutoffStudy -TestStart '2022-01-01' -Months 6 | Out-Null
    $cutoffCall = @($global:readinessCalls | Where-Object { $_ -match ' --detach ' })
    if ($cutoffCall.Count -ne 1 -or $cutoffCall[0] -notmatch 'eda_cutoff.py.*--test-start 2022-01-01 --months 6') { throw 'Cutoff mode arguments lost' }
    Invoke-Case active 'already running'
    Invoke-Case fsck 'not HEALTHY'
    Invoke-Case space 'below threshold' 999999
    Invoke-Case exit 'exit=137'
    Invoke-Case manifest 'did not confirm'
    Invoke-Case logs 'cleanup failed'
    Invoke-Case rm 'cleanup failed'
    $global:readinessFault = ''
    $held = [IO.File]::Open((Join-Path $repo 'outputs/eda/.eda.lock'), [IO.FileMode]::OpenOrCreate, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
    try {
        $global:readinessCalls.Clear()
        $rejected = $false
        try { & (Join-Path $repo 'tools/eda/Run-ModelReadiness.ps1') -Category All_Beauty -SilverRun run_20261016T021632209982Z -DockerDiskPath $fakeDisk } catch { $rejected = $true }
        if (-not $rejected -or $global:readinessCalls.Count) { throw 'Shared EDA lock was bypassed' }
    } finally { $held.Dispose() }
    Write-Output 'PASS: Readiness runtime success, native stderr, shared lock, active job, fsck, disk reserve, OOM/exit, manifest, log and cleanup failures.'
} finally {
    Remove-Item Function:\docker -ErrorAction SilentlyContinue
}
