# Offline integration: replace Docker with a fake finalized HDFS listing.
$ErrorActionPreference = 'Stop'
$root = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
. (Join-Path $root 'ingestion/scripts/Get-ExperimentScope.ps1')
$global:scopeTestCalls = [Collections.Generic.List[string]]::new()
$global:scopeTestExitCode = 0
$global:scopeTestFault = ''
$global:scopeTestSource = Join-Path $root 'data/staging/fixture-virtual-review.jsonl'
$fakeDiskPath = Join-Path $PSScriptRoot ('fixture-disk-' + [guid]::NewGuid().ToString('N') + '.vhdx')
Set-Content -LiteralPath $fakeDiskPath -Value 'fixture, not a real disk'
$global:scopeTestScope = Get-ExperimentScope
$legacy = Get-Content -LiteralPath (Join-Path $root 'ingestion/manifests/video_games_metadata.v1.json') -Raw -Encoding UTF8 | ConvertFrom-Json
$global:scopeTestManifest = @{ files = @(
    foreach ($category in $global:scopeTestScope.categories) {
        foreach ($kind in $global:scopeTestScope.kinds) {
            $name = if ($kind -eq 'metadata') { "meta_$category.jsonl" } else { "$category.jsonl" }
            @{ filename = $name; size = $(if($name -eq 'meta_Video_Games.jsonl') { 4 } else { 1 }); status = 'downloaded_verified'; sha256 = $legacy.files[0].sha256 }
        }
    }
) }
function Get-Content {
    if ($args -contains 'data/staging/amazon-reviews-2023/download-manifest.json') {
        $global:scopeTestManifest | ConvertTo-Json -Depth 5
    } else { Microsoft.PowerShell.Management\Get-Content @args }
}
function Test-Path {
    if ($args -contains $global:scopeTestSource) { return $true }
    Microsoft.PowerShell.Management\Test-Path @args
}
function Get-Item {
    if ($args -contains $global:scopeTestSource) { return [pscustomobject]@{ Length = 1 } }
    Microsoft.PowerShell.Management\Get-Item @args
}
function Get-FileHash {
    if ($args -contains $global:scopeTestSource) { return [pscustomobject]@{ Hash = ('b' * 64) } }
    Microsoft.PowerShell.Utility\Get-FileHash @args
}
function docker {
    $global:scopeTestCalls.Add(($args -join ' '))
    $global:LASTEXITCODE = 0
    if ($args[0] -eq 'ps' -and $global:scopeTestFault -eq 'active-spark') { 'existing-spark' }
    if ($args -contains '-safemode') { if($global:scopeTestFault -eq 'safemode') { 'Safe mode is ON' } else { 'Safe mode is OFF' } }
    if ($args -contains '--detach') { 'a' * 64 }
    if ($args[0] -eq 'inspect') { @{ Running = $false; ExitCode = $global:scopeTestExitCode; OOMKilled = $false } | ConvertTo-Json }
    if ($args[0] -eq 'logs') {
        if($global:scopeTestFault -eq 'logs') { throw 'fixture log failure' }
        if($global:scopeTestFault -in @('native-stderr', 'native-log-exit')) {
            # Exercise native stderr handling in Windows PowerShell 5.1, not just a mocked string.
            $exitCode = if($global:scopeTestFault -eq 'native-log-exit') { 7 } else { 0 }
            & $env:ComSpec /d /c "echo fixture stdout & echo fixture stderr 1>&2 & exit /b $exitCode"
            $global:LASTEXITCODE = $LASTEXITCODE
        } else { 'fixture log, no real container' }
    }
    if ($args -contains '-test' -and $args[-1] -eq '/project/raw/all_beauty_reviews/v1' -and $global:scopeTestFault -eq 'missing-target') { $global:LASTEXITCODE = 1 }
    if ($args -contains '-ls') {
        $path = $args[-1]
        if ($path -eq '/project/raw/video_games_metadata/v1') {
            1..4 | ForEach-Object {
                $bytes = if($global:scopeTestFault -eq 'flume-bytes') { 2 } else { 1 }
                $suffix = if($_ -eq 4 -and $global:scopeTestFault -eq 'flume-tmp') { '.tmp' } else { '.jsonl' }
                "-rw-r--r-- 1 hadoop supergroup $bytes 2026-10-11 00:00 $path/meta_Video_Games_batch$_$suffix"
            }
        } else {
            $match = [regex]::Match($path, '/([^/]+)_(reviews|metadata)/v1$')
            if (-not $match.Success) { throw "Unexpected dataset listing: $path" }
            $category = @($global:scopeTestScope.categories | Where-Object { $_.ToLowerInvariant() -eq $match.Groups[1].Value })[0]
            $name = if ($match.Groups[2].Value -eq 'metadata') { "meta_$category.jsonl" } else { "$category.jsonl" }
            $bytes = if($path -eq '/project/raw/all_beauty_reviews/v1' -and $global:scopeTestFault -eq 'target-size') { 2 } else { 1 }
            "-rw-r--r-- 1 hadoop supergroup $bytes 2026-10-11 00:00 $path/$name"
            if($path -eq '/project/raw/all_beauty_reviews/v1' -and $global:scopeTestFault -eq 'extra-file') {
                "-rw-r--r-- 1 hadoop supergroup 1 2026-10-11 00:00 $path/extra.jsonl"
            }
        }
    }
}
function Assert-PutRejected {
    param([string]$Message)
    $global:scopeTestCalls.Clear()
    $errorMessage = ''
    try { & (Join-Path $root 'ingestion/scripts/Put-AmazonRaw.ps1') -Apply } catch { $errorMessage = $_.Exception.Message }
    if ($errorMessage -notmatch $Message -or @($global:scopeTestCalls | Where-Object { $_ -match ' -put | -mkdir ' }).Count) { throw "Put did not reject before writes: $Message; actual: $errorMessage" }
}
try {
    & (Join-Path $root 'ingestion/scripts/Put-AmazonRaw.ps1')
    $listings = @($global:scopeTestCalls | Where-Object { $_ -match ' -ls ' })
    if ($listings.Count -ne 6 -or @($listings | Where-Object { $_ -match 'sports|toys|health| -R /project/raw$' }).Count) { throw 'Put preview scanned outside selected scope' }
    if (@($global:scopeTestCalls | Where-Object { $_ -match ' -put | -mkdir ' }).Count) { throw 'Preview mutated HDFS' }
    $allEntries = $global:scopeTestManifest.files
    $global:scopeTestManifest.files = @($allEntries | Where-Object { $_.filename -ne 'All_Beauty.jsonl' })
    Assert-PutRejected 'Missing or duplicate manifest entry'
    $global:scopeTestManifest.files = $allEntries
    $allEntries[0].status = 'failed'
    Assert-PutRejected 'Download not verified'
    $allEntries[0].status = 'downloaded_verified'
    $allEntries[0].size = 1.5
    Assert-PutRejected 'Invalid source size/checksum'
    $allEntries[0].size = 1
    $originalChecksum = $allEntries[0].sha256
    $allEntries[0].sha256 = 'invalid-checksum'
    Assert-PutRejected 'Invalid source size/checksum'
    $allEntries[0].sha256 = $originalChecksum
    foreach($fault in @('target-size','extra-file','flume-bytes','flume-tmp','safemode')) {
        $global:scopeTestFault = $fault
        Assert-PutRejected $(if($fault -eq 'safemode') { 'HDFS is not ready' } elseif($fault -like 'flume-*') { 'Dataset contains other files' } else { 'Existing dataset differs' })
    }
    $global:scopeTestFault = 'missing-target'
    $allEntries[0].path = 'C:/outside-staging/source.jsonl'
    Assert-PutRejected 'Source must be inside data/staging'
    $allEntries[0].path = $global:scopeTestSource
    Assert-PutRejected 'Source SHA256 mismatch'
    $global:scopeTestFault = ''
    $global:scopeTestCalls.Clear()
    $successResult = & (Join-Path $root 'tools/eda/Run-Eda.ps1') -DockerDiskPath $fakeDiskPath
    $samples = @(Get-Content -LiteralPath $successResult.ResourcePath | ForEach-Object { $_ | ConvertFrom-Json })
    if ($samples.Count -lt 2 -or $samples[0].vhdx_bytes -le 0) { throw 'Host JSONL samples missing' }
    $runs = @($global:scopeTestCalls | Where-Object { $_ -match ' --detach ' })
    if ($runs.Count -ne 1 -or $runs[0] -notmatch '--categories All_Beauty --version v1' -or $runs[0] -notmatch '--cores 2 --shuffle-partitions 64') { throw 'EDA wrapper must select only the first category with two Spark workers' }
    if (@($global:scopeTestCalls | Where-Object { $_ -eq ('rm ' + ('a' * 64)) }).Count -ne 1) { throw 'Owned container was not removed' }
    $global:scopeTestFault = 'native-stderr'
    $global:scopeTestCalls.Clear()
    $nativeResult = & (Join-Path $root 'tools/eda/Run-Eda.ps1') -DockerDiskPath $fakeDiskPath -Cores 1
    $nativeLog = Get-Content -LiteralPath $nativeResult.LogPath -Raw
    if($nativeResult.ExitCode -ne 0 -or $nativeLog -notmatch 'fixture stdout' -or $nativeLog -notmatch 'fixture stderr') { throw 'Native stdout/stderr logs were not saved on successful job' }
    if(@($global:scopeTestCalls | Where-Object { $_ -match ' --detach ' -and $_ -match '--cores 1 --shuffle-partitions 64' }).Count -ne 1) { throw 'Explicit single-worker override was not honored' }
    foreach($fault in @('logs', 'native-log-exit')) {
    $global:scopeTestFault = $fault
    $global:scopeTestCalls.Clear()
    $failed = $false
    $successObjects = 0
    try { & (Join-Path $root 'tools/eda/Run-Eda.ps1') -DockerDiskPath $fakeDiskPath | ForEach-Object { if($_.Category) { $successObjects++ } } } catch { $failed = $_.Exception.Message -match 'cleanup failed' }
    if (-not $failed -or $successObjects -ne 0 -or @($global:scopeTestCalls | Where-Object { $_ -eq ('rm ' + ('a' * 64)) }).Count -ne 1) { throw 'Log failure skipped container cleanup or emitted success' }
    }
    $lockProbe = [IO.File]::Open((Join-Path $root 'outputs/eda/.eda.lock'),[IO.FileMode]::Open,[IO.FileAccess]::ReadWrite,[IO.FileShare]::None)
    $lockProbe.Dispose()
    $global:scopeTestFault = 'active-spark'
    $global:scopeTestCalls.Clear()
    $failed = $false
    try { & (Join-Path $root 'tools/eda/Run-Eda.ps1') -DockerDiskPath $fakeDiskPath } catch { $failed = $_.Exception.Message -match 'already running' }
    if (-not $failed -or @($global:scopeTestCalls | Where-Object { $_ -match ' --detach ' }).Count) { throw 'Concurrent Spark guard failed' }
    $global:scopeTestFault = ''
    $global:scopeTestExitCode = 7
    $global:scopeTestCalls.Clear()
    $failed = $false
    try { & (Join-Path $root 'tools/eda/Run-Eda.ps1') -DockerDiskPath $fakeDiskPath } catch { $failed = $true }
    if (-not $failed -or @($global:scopeTestCalls | Where-Object { $_ -eq ('rm ' + ('a' * 64)) }).Count -ne 1) { throw 'Failed batch did not propagate error and clean up' }
    $global:scopeTestCalls.Clear()
    $failed = $false
    try { & (Join-Path $root 'tools/eda/Run-Eda.ps1') -DockerDiskPath $fakeDiskPath -MinFreeGiB 2147483647 } catch { $failed = $true }
    if (-not $failed -or $global:scopeTestCalls.Count) { throw 'Host disk reserve must reject before Docker' }
    foreach ($scriptName in @('tools/eda/Run-Eda.ps1', 'ingestion/scripts/Put-AmazonRaw.ps1')) {
        $global:scopeTestCalls.Clear()
        $rejected = $false
        try {
            if ($scriptName -like 'tools/*') { & (Join-Path $root $scriptName) -Category Sports_and_Outdoors }
            else { & (Join-Path $root $scriptName) -Categories Sports_and_Outdoors }
        } catch { $rejected = $true }
        if (-not $rejected -or $global:scopeTestCalls.Count) { throw "Out-of-scope selection reached Docker: $scriptName" }
    }
    'PASS: ingest failure gates, Flume size/finalization, batch cleanup including log failure, concurrent Spark and host guards'
} finally {
    Remove-Item Function:\docker
    Remove-Item Function:\Get-Content
    Remove-Item Function:\Get-Item
    Remove-Item Function:\Test-Path
    Remove-Item Function:\Get-FileHash
    Remove-Variable -Name scopeTestCalls,scopeTestScope,scopeTestManifest,scopeTestExitCode,scopeTestFault,scopeTestSource -Scope Global
    Remove-Item -LiteralPath $fakeDiskPath
}
