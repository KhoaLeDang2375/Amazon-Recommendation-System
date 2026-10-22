param(
    [string]$Category,
    [string]$DockerDiskPath = (Join-Path $env:LOCALAPPDATA 'Docker/wsl/disk/docker_data.vhdx'),
    [ValidateRange(1, 2147483647)][int]$MinFreeGiB = 20,
    [ValidateSet(1, 2)][int]$Cores = 2,
    [ValidateRange(1, 2147483647)][int]$ShufflePartitions = 64
)
$ErrorActionPreference = 'Stop'
$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
$containerId = $null
$lockHandle = $null
$jobComplete = $false
Push-Location $repoRoot
try {
    . (Join-Path $repoRoot 'ingestion/scripts/Get-ExperimentScope.ps1')
    $scope = Get-ExperimentScope
    if (-not $Category) { $Category = $scope.categories[0] }
    if ($Category -cnotin $scope.categories) { throw "Category outside experiment scope: $Category" }
    $disk = Get-Item -LiteralPath $DockerDiskPath
    if ($disk.PSIsContainer -or $disk.Extension -ine '.vhdx' -or ($disk.Attributes -band [IO.FileAttributes]::ReparsePoint)) {
        throw 'DockerDiskPath must be an existing VHDX file, not a symlink'
    }
    $outputRoot = Join-Path $repoRoot 'outputs/eda'
    New-Item -ItemType Directory -Path $outputRoot -Force | Out-Null
    $lockHandle = [IO.File]::Open((Join-Path $outputRoot '.eda.lock'), [IO.FileMode]::OpenOrCreate, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
    $batchId = 'batch_' + [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfffffffZ')
    $logPath = Join-Path $outputRoot "$batchId.log"
    $resourcePath = Join-Path $outputRoot "$batchId.resources.jsonl"
    function Save-HostResources {
        $outputFree = [IO.DriveInfo]::new([IO.Path]::GetPathRoot($outputRoot)).AvailableFreeSpace
        $dockerFree = [IO.DriveInfo]::new([IO.Path]::GetPathRoot($disk.FullName)).AvailableFreeSpace
        [ordered]@{ utc = [DateTime]::UtcNow.ToString('o'); output_free_bytes = $outputFree; docker_host_free_bytes = $dockerFree; vhdx_bytes = (Get-Item -LiteralPath $disk.FullName).Length } |
            ConvertTo-Json -Compress | Add-Content -LiteralPath $resourcePath -Encoding UTF8
        if ([Math]::Min($outputFree, $dockerFree) -lt ([long]$MinFreeGiB * 1GB)) {
            throw "Host disk reserve below $MinFreeGiB GiB; stop EDA and inspect $resourcePath"
        }
    }
    Save-HostResources
    $active = @(docker ps --filter label=com.docker.compose.project=hadoop-demo --filter label=com.docker.compose.service=spark-notebook --format '{{.Names}}')
    if ($LASTEXITCODE -ne 0) { throw 'Docker is unavailable; start Docker Desktop before running EDA' }
    if ($active.Count) { throw 'A Spark notebook/batch container is already running; stop it before starting another EDA' }
    docker image inspect bigdata-spark-notebook:3.5.6 --format '{{.Id}}' | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Spark image is missing; build it separately before running EDA' }
    $safeMode = docker compose exec -T namenode hdfs dfsadmin -safemode get
    if ($LASTEXITCODE -ne 0 -or ($safeMode -join ' ') -notmatch 'Safe mode is OFF') { throw 'HDFS is not ready; start the existing NameNode/DataNodes without recreating volumes' }
    $created = @(docker compose -f compose.yaml -f compose.datanodes.yaml -f compose.eda-light.yaml run --detach --pull never --no-deps --name hadoop-demo-eda-batch spark-notebook python /workspace/notebooks/eda_recommendation.py --categories $Category --version $scope.version --base "hdfs://namenode:8020$($scope.hdfs_base)" --min-free-gb $MinFreeGiB --cores $Cores --shuffle-partitions $ShufflePartitions)
    if ($LASTEXITCODE -ne 0) { throw 'Could not create batch container; build the Spark image separately if missing' }
    $ids = @($created | Where-Object { $_ -match '^[a-f0-9]{64}$' })
    if ($ids.Count -ne 1) { throw 'Docker did not return one verified container ID' }
    $containerId = $ids[0]
    Write-Host "EDA $Category started; resources: $resourcePath"
    while ($true) {
        Save-HostResources
        $stateJson = docker inspect --format '{{json .State}}' $containerId
        if ($LASTEXITCODE -ne 0) { throw 'Cannot inspect the owned EDA container' }
        $state = $stateJson | ConvertFrom-Json
        if (-not $state.Running) { break }
        Start-Sleep -Seconds 5
    }
    if ($state.ExitCode -ne 0) { throw "EDA failed/interrupted (exit=$($state.ExitCode), OOM=$($state.OOMKilled)); inspect $logPath and run.json" }
    $jobComplete = $true
    $result = [pscustomobject]@{ Category = $Category; LogPath = $logPath; ResourcePath = $resourcePath; ExitCode = $state.ExitCode }
} finally {
    $cleanupFailed = $false
    try {
        if ($containerId) {
            # Each cleanup step is independent; a log error must not skip rm or lock release.
            try {
                docker stop --time 30 $containerId | Out-Null
                if ($LASTEXITCODE -ne 0) { throw 'Docker stop failed' }
            } catch { $cleanupFailed = $true; Write-Warning "Could not stop owned container: $containerId" }
            try {
                # Spark logs normally use stderr; Windows PowerShell 5.1 turns native stderr into ErrorRecords.
                & { $ErrorActionPreference = 'Continue'; docker logs $containerId 2>&1 } |
                    ForEach-Object { $_.ToString() } | Set-Content -LiteralPath $logPath -Encoding UTF8 -ErrorAction Stop
                if ($LASTEXITCODE -ne 0) { throw 'Docker logs failed' }
            } catch { $cleanupFailed = $true; Write-Warning "Could not save owned container log: $containerId; $($_.Exception.Message)" }
            try {
                docker rm $containerId | Out-Null
                if ($LASTEXITCODE -ne 0) { throw 'Docker rm failed' }
            } catch { $cleanupFailed = $true; Write-Warning "Container cleanup failed: $containerId; inspect before running another batch" }
        }
    } finally {
        if ($lockHandle) { $lockHandle.Dispose() }
        Pop-Location
    }
    if ($cleanupFailed -and $jobComplete) { throw 'EDA finished but container/log cleanup failed; inspect warnings before another run' }
}
Write-Host "EDA $Category complete; log: $logPath"
$result
