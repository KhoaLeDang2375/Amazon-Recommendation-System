param(
    [Parameter(Mandatory=$true)][string]$Category,
    [Parameter(Mandatory=$true)][ValidatePattern('^run_[A-Za-z0-9_]{8,64}$')][string]$SilverRun,
    [string]$DockerDiskPath = (Join-Path $env:LOCALAPPDATA 'Docker/wsl/disk/docker_data.vhdx'),
    [ValidateRange(1, 2147483647)][int]$MinFreeGiB = 20,
    [ValidateSet(1, 2)][int]$Cores = 2,
    [ValidateRange(1, 2147483647)][int]$ShufflePartitions = 64
)
$ErrorActionPreference = 'Stop'
$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
$containerId = $null
$lockHandle = $null
$batch = $null
$complete = $false
Push-Location $repoRoot
function Invoke-ReadinessDocker {
    param([string[]]$Arguments)
    $lines = & { $ErrorActionPreference = 'Continue'; docker @Arguments 2>&1; $script:readinessExit = $LASTEXITCODE }
    if ($script:readinessExit -ne 0) { throw "Docker failed ($script:readinessExit): $($Arguments[0]); $($lines -join ' ')" }
    $lines | ForEach-Object { $_.ToString() }
}
try {
    . (Join-Path $repoRoot 'ingestion/scripts/Get-ExperimentScope.ps1')
    $scope = Get-ExperimentScope
    if ($Category -cnotin $scope.categories) { throw "Category outside experiment scope: $Category" }
    $disk = Get-Item -LiteralPath $DockerDiskPath
    if ($disk.PSIsContainer -or $disk.Extension -ine '.vhdx' -or ($disk.Attributes -band [IO.FileAttributes]::ReparsePoint)) {
        throw 'DockerDiskPath must be an existing VHDX file, not a symlink'
    }
    $lockDirectory = Join-Path $repoRoot 'outputs/eda'
    New-Item -ItemType Directory -Path $lockDirectory -Force | Out-Null
    $lockHandle = [IO.File]::Open((Join-Path $lockDirectory '.eda.lock'), [IO.FileMode]::OpenOrCreate, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
    $active = @(Invoke-ReadinessDocker -Arguments @('ps', '--filter', 'label=com.docker.compose.project=hadoop-demo', '--filter', 'label=com.docker.compose.service=spark-notebook', '--format', '{{.Names}}'))
    if ($active.Count) { throw 'A Spark notebook/batch container is already running' }
    Invoke-ReadinessDocker -Arguments @('image', 'inspect', 'bigdata-spark-notebook:3.5.6', '--format', '{{.Id}}') | Out-Null
    $safe = Invoke-ReadinessDocker -Arguments @('compose', 'exec', '-T', 'namenode', 'hdfs', 'dfsadmin', '-safemode', 'get')
    if (($safe -join ' ') -notmatch 'Safe mode is OFF') { throw 'HDFS is not ready' }
    $source = "/project/processed/silver/$($Category.ToLowerInvariant())/v1/$SilverRun"
    $fsck = Invoke-ReadinessDocker -Arguments @('compose', 'exec', '-T', 'namenode', 'hdfs', 'fsck', $source)
    if (($fsck -join ' ') -notmatch 'is HEALTHY') { throw 'Selected Silver input is not HEALTHY' }
    $runId = 'run_' + [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssffffffZ')
    $outputRoot = Join-Path $repoRoot 'outputs/eda_model_readiness'
    New-Item -ItemType Directory -Path $outputRoot -Force | Out-Null
    $batchPath = Join-Path $outputRoot "$runId.batch.json"
    $logPath = Join-Path $outputRoot "$runId.log"
    $resourcePath = Join-Path $outputRoot "$runId.host.resources.jsonl"
    $batch = [ordered]@{ run_id=$runId; category=$Category; silver_run=$SilverRun; status='running'; docker_disk_path=$disk.FullName }
    $batch | ConvertTo-Json | Set-Content -LiteralPath $batchPath -Encoding UTF8
    function Save-ReadinessResources {
        $outputFree = [IO.DriveInfo]::new([IO.Path]::GetPathRoot($outputRoot)).AvailableFreeSpace
        $dockerFree = [IO.DriveInfo]::new([IO.Path]::GetPathRoot($disk.FullName)).AvailableFreeSpace
        [ordered]@{ utc=[DateTime]::UtcNow.ToString('o'); output_free_bytes=$outputFree; docker_host_free_bytes=$dockerFree; vhdx_bytes=(Get-Item -LiteralPath $disk.FullName).Length } |
            ConvertTo-Json -Compress | Add-Content -LiteralPath $resourcePath -Encoding UTF8
        if ([Math]::Min($outputFree, $dockerFree) -lt ([long]$MinFreeGiB * 1GB)) { throw 'Host disk reserve below threshold' }
    }
    Save-ReadinessResources
    $created = @(Invoke-ReadinessDocker -Arguments @('compose', '-f', 'compose.yaml', '-f', 'compose.datanodes.yaml', '-f', 'compose.eda-light.yaml',
        'run', '--detach', '--pull', 'never', '--no-deps', '--name', "hadoop-demo-readiness-$runId", 'spark-notebook',
        'python', '/workspace/notebooks/eda_model_readiness.py', '--category', $Category, '--silver-run', $SilverRun,
        '--run-id', $runId, '--cores', "$Cores", '--shuffle-partitions', "$ShufflePartitions", '--min-free-gb', "$MinFreeGiB"))
    $ids = @($created | Where-Object { $_ -match '^[a-f0-9]{64}$' })
    if ($ids.Count -ne 1) { throw 'Docker did not return one owned container ID' }
    $containerId = $ids[0]
    $batch.container_id = $containerId
    $batch | ConvertTo-Json | Set-Content -LiteralPath $batchPath -Encoding UTF8
    Write-Host "Readiness $Category started; run=$runId; logs=$logPath"
    while ($true) {
        Save-ReadinessResources
        $state = (Invoke-ReadinessDocker -Arguments @('inspect', '--format', '{{json .State}}', $containerId)) | ConvertFrom-Json
        if (-not $state.Running) { break }
        Start-Sleep -Seconds 5
    }
    if ($state.ExitCode -ne 0 -or $state.OOMKilled) { throw "Readiness exit=$($state.ExitCode), OOM=$($state.OOMKilled); inspect $logPath" }
    $job = Get-Content -LiteralPath (Join-Path $outputRoot "$runId/run.json") -Raw -Encoding UTF8 | ConvertFrom-Json
    if ($job.status -cne 'complete' -or $job.category -cne $Category -or $job.silver_run -cne $SilverRun -or $job.run_id -cne $runId) { throw 'Job manifest did not confirm completion' }
    $complete = $true
    $result = [pscustomobject]@{ Category=$Category; RunId=$runId; ReportDirectory=(Join-Path $outputRoot "$runId/$Category"); LogPath=$logPath }
} catch {
    if ($batch) { $batch.error = $_.Exception.Message }
    throw
} finally {
    $cleanupFailed = $false
    try {
        if ($containerId) {
            try { Invoke-ReadinessDocker -Arguments @('stop', '--timeout', '30', $containerId) | Out-Null } catch { $cleanupFailed=$true; Write-Warning "Could not stop owned container $containerId" }
            try { Invoke-ReadinessDocker -Arguments @('logs', $containerId) | Set-Content -LiteralPath $logPath -Encoding UTF8 -ErrorAction Stop } catch { $cleanupFailed=$true; Write-Warning "Could not save logs for $containerId" }
            try { Invoke-ReadinessDocker -Arguments @('rm', $containerId) | Out-Null } catch { $cleanupFailed=$true; Write-Warning "Could not remove owned container $containerId" }
        }
        if ($batch) {
            $batch.status = if (-not $complete) { 'failed' } elseif ($cleanupFailed) { 'complete_cleanup_failed' } else { 'complete' }
            $batch.finished_at = [DateTime]::UtcNow.ToString('o')
            $batch | ConvertTo-Json | Set-Content -LiteralPath $batchPath -Encoding UTF8
        }
    } finally {
        if ($lockHandle) { $lockHandle.Dispose() }
        Pop-Location
    }
    if ($cleanupFailed -and $complete) { throw 'Job completed but log/container cleanup failed' }
}
$result
