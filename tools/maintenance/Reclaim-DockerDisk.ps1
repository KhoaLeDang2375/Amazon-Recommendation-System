# Manual maintenance only. Requires Administrator and stops all Docker/WSL sessions.
param(
    [string]$DockerDiskPath = (Join-Path $env:LOCALAPPDATA 'Docker/wsl/disk/docker_data.vhdx')
)
$ErrorActionPreference = 'Stop'
$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
if (-not ([Security.Principal.WindowsPrincipal]$identity).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw 'Windows requires Administrator privileges for DiskPart compact vdisk'
}
$repoPath = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
$disk = Get-Item -LiteralPath $DockerDiskPath
if ($disk.PSIsContainer -or $disk.Name -ine 'docker_data.vhdx') { throw 'Select the Docker data disk named docker_data.vhdx' }
for ($entry = $disk; $null -ne $entry; $entry = if ($entry -is [IO.FileInfo]) { $entry.Directory } else { $entry.Parent }) {
    if ($entry.Attributes -band [IO.FileAttributes]::ReparsePoint) { throw 'Refusing a VHDX path through a symlink/junction' }
}
$diskPath = $disk.FullName
$diskDrive = [IO.DriveInfo]::new([IO.Path]::GetPathRoot($diskPath))
$outputPath = Join-Path $repoPath 'outputs'
New-Item -ItemType Directory -Path $outputPath -Force | Out-Null
$diskpartFile = Join-Path $outputPath 'eda-compact-diskpart.txt'
$diskpartLog = Join-Path $outputPath 'eda-compact-diskpart.log'
$resultFile = Join-Path $outputPath 'eda-reclaim-result.json'
$beforeSize = $disk.Length
$beforeFree = $diskDrive.AvailableFreeSpace
Push-Location $repoPath
try {
    $activeNames = @(docker ps --format '{{.Names}}')
    if ($LASTEXITCODE -ne 0) { throw 'Cannot inspect active Docker containers' }
    $allowed = @('hadoop-demo-namenode-1', 'hadoop-demo-datanode1-1', 'hadoop-demo-datanode2-1', 'hadoop-demo-datanode3-1')
    if (@($activeNames | Where-Object { $_ -notin $allowed }).Count) {
        throw 'Other containers, Spark or Flume are running; stop jobs and drain Flume before maintenance'
    }
    Write-Host "Maintenance will stop Docker and ALL WSL sessions; compact disk: $diskPath"
    if ($activeNames.Count) {
        docker stop --time 30 @activeNames
        if ($LASTEXITCODE -ne 0) { throw 'Container shutdown failed' }
    }
    wsl -d docker-desktop -u root -- /sbin/fstrim -v /mnt/docker-desktop-disk
    if ($LASTEXITCODE -ne 0) { throw 'Docker filesystem trim failed; compaction was not started' }
    docker desktop stop
    if ($LASTEXITCODE -ne 0) { throw 'Docker Desktop shutdown failed' }
    wsl --shutdown
    if ($LASTEXITCODE -ne 0) { throw 'WSL shutdown failed' }
    $diskHandle = [IO.File]::Open($diskPath, [IO.FileMode]::Open, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
    $diskHandle.Dispose()
    @("select vdisk file=`"$diskPath`"", 'compact vdisk', 'exit') | Set-Content -LiteralPath $diskpartFile -Encoding Unicode
    diskpart /s $diskpartFile | Tee-Object -FilePath $diskpartLog
    if ($LASTEXITCODE -ne 0) { throw 'DiskPart failed; inspect eda-compact-diskpart.log' }
    $afterSize = (Get-Item -LiteralPath $diskPath).Length
    if ($afterSize -gt $beforeSize) { throw 'Unexpected disk growth during compaction' }
    [ordered]@{
        disk_path = $diskPath
        host_drive = $diskDrive.Name
        before_bytes = $beforeSize
        after_bytes = $afterSize
        disk_reclaimed_gib = [Math]::Round(($beforeSize - $afterSize) / 1GB, 2)
        host_free_before_gib = [Math]::Round($beforeFree / 1GB, 2)
        host_free_after_gib = [Math]::Round($diskDrive.AvailableFreeSpace / 1GB, 2)
        docker_and_wsl = 'stopped'
        outcome = if ($afterSize -lt $beforeSize) { 'disk_shrank' } else { 'no_observed_shrink; inspect diskpart log' }
    } | ConvertTo-Json | Set-Content -LiteralPath $resultFile -Encoding UTF8
    Get-Content -LiteralPath $resultFile
} finally {
    Pop-Location
}
