param(
    [string]$Category = 'All_Beauty',
    [string]$DockerDiskPath = (Join-Path $env:LOCALAPPDATA 'Docker/wsl/disk/docker_data.vhdx'),
    [ValidateRange(1, 2147483647)][int]$MinFreeGiB = 20,
    [ValidateSet(1, 2)][int]$Cores = 2,
    [ValidateRange(1, 2147483647)][int]$ShufflePartitions = 64,
    [ValidatePattern('^run_[A-Za-z0-9_]{8,64}$')][string]$TextOnlyFromRun
)
$ErrorActionPreference = 'Stop'
$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
$containerId = $null
$lockHandle = $null
$complete = $false
$failed = $true
$runPath = $null
Push-Location $repoRoot

function Invoke-GoldDocker {
    param([string[]]$Arguments)
    $lines = & { $ErrorActionPreference = 'Continue'; docker @Arguments 2>&1; $script:goldDockerExit = $LASTEXITCODE }
    if ($script:goldDockerExit -ne 0) { throw "Docker failed ($script:goldDockerExit): $($Arguments[0]); $($lines -join ' ')" }
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
    $active = @(Invoke-GoldDocker -Arguments @('ps', '--filter', 'label=com.docker.compose.project=hadoop-demo', '--filter', 'label=com.docker.compose.service=spark-notebook', '--format', '{{.Names}}'))
    if ($active.Count) { throw 'A Spark notebook/batch container is already running; finish it before Gold' }
    Invoke-GoldDocker -Arguments @('image', 'inspect', 'bigdata-spark-notebook:3.5.6', '--format', '{{.Id}}') | Out-Null
    $safeMode = Invoke-GoldDocker -Arguments @('compose', 'exec', '-T', 'namenode', 'hdfs', 'dfsadmin', '-safemode', 'get')
    if (($safeMode -join ' ') -notmatch 'Safe mode is OFF') { throw 'HDFS is not ready; preserve the existing NameNode/DataNodes and volumes' }
    $protocol = Get-Content -LiteralPath (Join-Path $repoRoot 'processing/gold_protocol.json') -Raw -Encoding UTF8 | ConvertFrom-Json
    if ($protocol.status -cne 'locked') { throw 'Gold protocol is not locked' }
    $selected = @($protocol.evidence | Where-Object { $_.category -ceq $Category })
    if ($selected.Count -ne 1) { throw 'Missing pinned Silver source' }
    $inputPaths = @("/project/processed/silver/$($Category.ToLowerInvariant())/v1/$($selected[0].silver_run)")
    $inputBytes = [long]0
    foreach ($path in $inputPaths) {
        $fsck = Invoke-GoldDocker -Arguments @('compose', 'exec', '-T', 'namenode', 'hdfs', 'fsck', $path)
        if (($fsck -join ' ') -notmatch 'is HEALTHY') { throw "Selected input is not HEALTHY: $path" }
        $du = @(Invoke-GoldDocker -Arguments @('compose', 'exec', '-T', 'namenode', 'hdfs', 'dfs', '-du', '-s', $path))
        $measurements = @($du | ForEach-Object { if ($_ -match '^\s*(\d+)\s+(\d+)\s+(\S+)\s*$' -and $Matches[3] -ceq $path) { [long]$Matches[1] } })
        if ($measurements.Count -ne 1 -or $measurements[0] -le 0) { throw "Cannot measure selected input: $path" }
        $inputBytes += $measurements[0]
    }
    $runId = 'run_' + [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssffffffZ')
    $outputRoot = Join-Path $repoRoot "outputs/processing/gold/$runId"
    New-Item -ItemType Directory -Path $outputRoot -ErrorAction Stop | Out-Null
    $runPath = Join-Path $outputRoot 'batch.json'
    $logPath = Join-Path $outputRoot 'batch.log'
    $resourcesPath = Join-Path $outputRoot 'host.resources.jsonl'
    $outputBudget = [long](4 * $inputBytes)
    $scratchBudget = [long](8 * $inputBytes)
    $dockerRequired = [long]$MinFreeGiB * 1GB + $outputBudget + $scratchBudget + [long](2 * $inputBytes)
    $batch = [ordered]@{ run_id = $runId; category = $Category; status = 'running'; input_bytes = $inputBytes; output_budget_bytes = $outputBudget; scratch_budget_bytes = $scratchBudget; docker_disk_path = $disk.FullName }
    $batch | ConvertTo-Json | Set-Content -LiteralPath $runPath -Encoding UTF8
    function Save-GoldHostResources {
        $outputFree = [IO.DriveInfo]::new([IO.Path]::GetPathRoot($outputRoot)).AvailableFreeSpace
        $dockerFree = [IO.DriveInfo]::new([IO.Path]::GetPathRoot($disk.FullName)).AvailableFreeSpace
        [ordered]@{ utc = [DateTime]::UtcNow.ToString('o'); output_free_bytes = $outputFree; docker_host_free_bytes = $dockerFree; vhdx_bytes = (Get-Item -LiteralPath $disk.FullName).Length } |
            ConvertTo-Json -Compress | Add-Content -LiteralPath $resourcesPath -Encoding UTF8
        $required = if ($containerId) { [long]$MinFreeGiB * 1GB } else { $dockerRequired }
        if ($dockerFree -lt $required -or $outputFree -lt ([long]$MinFreeGiB * 1GB)) { throw 'Host free space below Gold budget/reserve' }
    }
    Save-GoldHostResources
    $createArguments = @('compose', '-f', 'compose.yaml', '-f', 'compose.datanodes.yaml', '-f', 'compose.gold-light.yaml',
                        'run', '--detach', '--pull', 'never', '--no-deps', '--name', "hadoop-demo-gold-$runId", 'spark-notebook',
                        'python', '-m', 'processing.gold', '--category', $Category, '--run-id', $runId,
                        '--cores', "$Cores", '--shuffle-partitions', "$ShufflePartitions", '--min-free-gib', "$MinFreeGiB",
                        '--output-budget-gib', ([string]([double]$outputBudget / 1GB)), '--scratch-budget-gib', ([string]([double]$scratchBudget / 1GB)))
    if ($TextOnlyFromRun) { $createArguments += @('--text-only-from-run', $TextOnlyFromRun) }
    $created = @(Invoke-GoldDocker -Arguments $createArguments)
    $ids = @($created | Where-Object { $_ -match '^[a-f0-9]{64}$' })
    if ($ids.Count -ne 1) { throw 'Docker did not return one verified owned container ID' }
    $containerId = $ids[0]
    $batch.container_id = $containerId
    $batch | ConvertTo-Json | Set-Content -LiteralPath $runPath -Encoding UTF8
    Write-Host "Gold $Category started; run=$runId; resources=$resourcesPath"
    while ($true) {
        Save-GoldHostResources
        $state = (Invoke-GoldDocker -Arguments @('inspect', '--format', '{{json .State}}', $containerId)) | ConvertFrom-Json
        if (-not $state.Running) { break }
        Start-Sleep -Seconds 5
    }
    if ($state.ExitCode -ne 0 -or $state.OOMKilled) { throw "Gold exit=$($state.ExitCode), OOM=$($state.OOMKilled); inspect $outputRoot" }
    $job = Get-Content -LiteralPath (Join-Path $outputRoot 'run.json') -Raw -Encoding UTF8 | ConvertFrom-Json
    if ($job.status -cne 'complete' -or $job.category -cne $Category -or $job.run_id -cne $runId) { throw 'Gold manifest did not confirm completion' }
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
            try { Invoke-GoldDocker -Arguments @('stop', '--timeout', '30', $containerId) | Out-Null } catch { $cleanupFailed = $true; Write-Warning "Could not stop owned container: $containerId" }
            try {
                $lines = Invoke-GoldDocker -Arguments @('logs', $containerId)
                $lines | Set-Content -LiteralPath $logPath -Encoding UTF8 -ErrorAction Stop
            } catch { $cleanupFailed = $true; Write-Warning "Could not save Gold logs for owned container: $containerId" }
            try { Invoke-GoldDocker -Arguments @('rm', $containerId) | Out-Null } catch { $cleanupFailed = $true; Write-Warning "Could not remove owned container: $containerId" }
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
    if ($cleanupFailed -and $complete) { throw 'Gold completed but log/container cleanup failed; inspect warnings' }
}
$result
