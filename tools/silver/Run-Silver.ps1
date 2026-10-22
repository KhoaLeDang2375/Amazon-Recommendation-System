param(
    [string]$Category = 'All_Beauty',
    [string]$DockerDiskPath = (Join-Path $env:LOCALAPPDATA 'Docker/wsl/disk/docker_data.vhdx'),
    [ValidateRange(1, 2147483647)][int]$MinFreeGiB = 20,
    [ValidateSet(1, 2)][int]$Cores = 2,
    [ValidateRange(1, 2147483647)][int]$ShufflePartitions = 64
)
$ErrorActionPreference = 'Stop'
$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
$containerId = $null
$lockHandle = $null
$complete = $false
$failed = $true
$runPath = $null
Push-Location $repoRoot

function Invoke-SilverDocker {
    param([string[]]$Arguments)
    $lines = & { $ErrorActionPreference = 'Continue'; docker @Arguments 2>&1; $script:silverDockerExit = $LASTEXITCODE }
    if ($script:silverDockerExit -ne 0) { throw "Docker failed ($script:silverDockerExit): $($Arguments[0]); $($lines -join ' ')" }
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
    $active = @(Invoke-SilverDocker -Arguments @('ps', '--filter', 'label=com.docker.compose.project=hadoop-demo', '--filter', 'label=com.docker.compose.service=spark-notebook', '--format', '{{.Names}}'))
    if ($active.Count) { throw 'A Spark notebook/batch container is already running; finish it before Silver' }
    Invoke-SilverDocker -Arguments @('image', 'inspect', 'bigdata-spark-notebook:3.5.6', '--format', '{{.Id}}') | Out-Null
    $safeMode = Invoke-SilverDocker -Arguments @('compose', 'exec', '-T', 'namenode', 'hdfs', 'dfsadmin', '-safemode', 'get')
    if (($safeMode -join ' ') -notmatch 'Safe mode is OFF') { throw 'HDFS is not ready; preserve the existing NameNode/DataNodes and volumes' }
    $inputPaths = @($scope.kinds | ForEach-Object { "$($scope.hdfs_base)/$($Category.ToLowerInvariant())_$_/$($scope.version)" })
    $inputBytes = [long]0
    foreach ($path in $inputPaths) {
        $fsck = Invoke-SilverDocker -Arguments @('compose', 'exec', '-T', 'namenode', 'hdfs', 'fsck', $path)
        if (($fsck -join ' ') -notmatch 'is HEALTHY') { throw "Selected input is not HEALTHY: $path" }
        $du = @(Invoke-SilverDocker -Arguments @('compose', 'exec', '-T', 'namenode', 'hdfs', 'dfs', '-du', '-s', $path))
        $measurements = @($du | ForEach-Object { if ($_ -match '^\s*(\d+)\s+(\d+)\s+(\S+)\s*$' -and $Matches[3] -ceq $path) { [long]$Matches[1] } })
        if ($measurements.Count -ne 1 -or $measurements[0] -le 0) { throw "Cannot measure selected input: $path" }
        $inputBytes += $measurements[0]
    }
    $runId = 'run_' + [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssffffffZ')
    $outputRoot = Join-Path $repoRoot "outputs/processing/silver/$runId"
    New-Item -ItemType Directory -Path $outputRoot -ErrorAction Stop | Out-Null
    $runPath = Join-Path $outputRoot 'batch.json'
    $logPath = Join-Path $outputRoot 'batch.log'
    $resourcesPath = Join-Path $outputRoot 'host.resources.jsonl'
    $outputBudget = [long](2 * $inputBytes)
    $scratchBudget = [long](4 * $inputBytes)
    $dockerRequired = [long]$MinFreeGiB * 1GB + $outputBudget + $scratchBudget
    $batch = [ordered]@{ run_id = $runId; category = $Category; status = 'running'; input_bytes = $inputBytes; output_budget_bytes = $outputBudget; scratch_budget_bytes = $scratchBudget; docker_disk_path = $disk.FullName }
    $batch | ConvertTo-Json | Set-Content -LiteralPath $runPath -Encoding UTF8
    function Save-SilverHostResources {
        $outputFree = [IO.DriveInfo]::new([IO.Path]::GetPathRoot($outputRoot)).AvailableFreeSpace
        $dockerFree = [IO.DriveInfo]::new([IO.Path]::GetPathRoot($disk.FullName)).AvailableFreeSpace
        [ordered]@{ utc = [DateTime]::UtcNow.ToString('o'); output_free_bytes = $outputFree; docker_host_free_bytes = $dockerFree; vhdx_bytes = (Get-Item -LiteralPath $disk.FullName).Length } |
            ConvertTo-Json -Compress | Add-Content -LiteralPath $resourcesPath -Encoding UTF8
        $required = if ($containerId) { [long]$MinFreeGiB * 1GB } else { $dockerRequired }
        if ($dockerFree -lt $required -or $outputFree -lt ([long]$MinFreeGiB * 1GB)) { throw 'Host free space below Silver budget/reserve' }
    }
    Save-SilverHostResources
    $createArguments = @('compose', '-f', 'compose.yaml', '-f', 'compose.datanodes.yaml', '-f', 'compose.silver-light.yaml',
                        'run', '--detach', '--pull', 'never', '--no-deps', '--name', "hadoop-demo-silver-$runId", 'spark-notebook',
                        'python', '-m', 'processing.silver', '--category', $Category, '--run-id', $runId,
                        '--cores', "$Cores", '--shuffle-partitions', "$ShufflePartitions", '--min-free-gib', "$MinFreeGiB")
    $created = @(Invoke-SilverDocker -Arguments $createArguments)
    $ids = @($created | Where-Object { $_ -match '^[a-f0-9]{64}$' })
    if ($ids.Count -ne 1) { throw 'Docker did not return one verified owned container ID' }
    $containerId = $ids[0]
    $batch.container_id = $containerId
    $batch | ConvertTo-Json | Set-Content -LiteralPath $runPath -Encoding UTF8
    Write-Host "Silver $Category started; run=$runId; resources=$resourcesPath"
    while ($true) {
        Save-SilverHostResources
        $state = (Invoke-SilverDocker -Arguments @('inspect', '--format', '{{json .State}}', $containerId)) | ConvertFrom-Json
        if (-not $state.Running) { break }
        Start-Sleep -Seconds 5
    }
    if ($state.ExitCode -ne 0 -or $state.OOMKilled) { throw "Silver exit=$($state.ExitCode), OOM=$($state.OOMKilled); inspect $outputRoot" }
    $job = Get-Content -LiteralPath (Join-Path $outputRoot 'run.json') -Raw -Encoding UTF8 | ConvertFrom-Json
    if ($job.status -cne 'complete' -or $job.category -cne $Category -or $job.run_id -cne $runId) { throw 'Silver manifest did not confirm completion' }
    $complete = $true
    $failed = $false
    $result = [pscustomobject]@{ Category = $Category; RunId = $runId; FinalPath = $job.final_path; LogPath = $logPath; ReportDirectory = $outputRoot }
} catch {
    if ($runPath) { $batch.error = $_.Exception.Message }
    throw
} finally {
    $cleanupFailed = $false
    try {
        if ($containerId) {
            try { Invoke-SilverDocker -Arguments @('stop', '--timeout', '30', $containerId) | Out-Null } catch { $cleanupFailed = $true; Write-Warning "Could not stop owned container: $containerId" }
            try {
                $lines = Invoke-SilverDocker -Arguments @('logs', $containerId)
                $lines | Set-Content -LiteralPath $logPath -Encoding UTF8 -ErrorAction Stop
            } catch { $cleanupFailed = $true; Write-Warning "Could not save Silver logs for owned container: $containerId" }
            try { Invoke-SilverDocker -Arguments @('rm', $containerId) | Out-Null } catch { $cleanupFailed = $true; Write-Warning "Could not remove owned container: $containerId" }
        }
        if ($runPath) {
            $batch.status = if ($failed) { 'failed' } elseif ($cleanupFailed) { 'complete_cleanup_failed' } else { 'complete' }
            $batch.finished_at = [DateTime]::UtcNow.ToString('o')
            $batch | ConvertTo-Json | Set-Content -LiteralPath $runPath -Encoding UTF8
        }
    } finally {
        if ($lockHandle) { $lockHandle.Dispose() }
        Pop-Location
    }
    if ($cleanupFailed -and $complete) { throw 'Silver completed but log/container cleanup failed; inspect warnings' }
}
$result
