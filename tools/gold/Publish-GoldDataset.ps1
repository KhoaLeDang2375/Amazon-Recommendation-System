param(
    [switch]$Export,
    [switch]$Upload,
    [switch]$Login,
    [switch]$NewVersion,
    [string]$DatasetHandle = 'thoandanh/gold-training-v2',
    [string]$PackagePath,
    [string]$Python = 'python',
    [ValidateRange(1, 2147483647)][int]$MinFreeGiB = 20
)
$ErrorActionPreference = 'Stop'
$repoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
$lockHandle = $null
$containerId = $null
$logPath = $null
Push-Location $repoRoot

function Invoke-TrainingDocker {
    param([string[]]$Arguments)
    $lines = & { $ErrorActionPreference = 'Continue'; docker @Arguments 2>&1; $script:trainingExit = $LASTEXITCODE }
    if ($script:trainingExit -ne 0) { throw "Docker failed ($script:trainingExit): $($Arguments[0])" }
    $lines | ForEach-Object { $_.ToString() }
}

function Invoke-TrainingPython {
    param([string[]]$Arguments)
    & $Python @Arguments
    if ($LASTEXITCODE -ne 0) { throw 'Gold training Python command failed; see output above' }
}

try {
    if ($PackagePath) {
        if ($Export) { throw '-PackagePath selects an existing package for upload; omit -Export' }
        $package = (Get-Item -LiteralPath $PackagePath -ErrorAction Stop).FullName
        if (-not (Test-Path -LiteralPath (Join-Path $package 'dataset_manifest.json') -PathType Leaf)) { throw 'PackagePath must contain dataset_manifest.json' }
    } else {
        $planLines = & $Python -m processing.gold_kaggle plan
        if ($LASTEXITCODE -ne 0) { throw 'Cannot build pinned Gold plan' }
        $plan = ($planLines -join "`n") | ConvertFrom-Json
        $package = Join-Path $repoRoot "outputs/processing/gold_training/$($plan.bundle_id)"
        $containerPackage = "/workspace/outputs/processing/gold_training/$($plan.bundle_id)"
    }
    Write-Host "Dataset: $DatasetHandle (PRIVATE); categories: All_Beauty, Video_Games, Industrial_and_Scientific"
    Write-Host "Package: $package"
    if (-not $Export -and -not $Upload) {
        Write-Host 'PLAN ONLY. -Export builds/validates local Parquet; -Upload submits the complete package through KaggleHub.'
        return
    }
    $lockDirectory = Join-Path $repoRoot 'outputs/eda'
    New-Item -ItemType Directory -Path $lockDirectory -Force | Out-Null
    $lockHandle = [IO.File]::Open((Join-Path $lockDirectory '.eda.lock'), [IO.FileMode]::OpenOrCreate, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
    if ($Export) {
        $active = @(Invoke-TrainingDocker -Arguments @('ps','--filter','label=com.docker.compose.project=hadoop-demo','--filter','label=com.docker.compose.service=spark-notebook','--format','{{.Names}}'))
        if ($active.Count) { throw 'A Spark notebook/batch container is active. Stop an idle notebook or finish its job before export.' }
        $safeMode = Invoke-TrainingDocker -Arguments @('compose','exec','-T','namenode','hdfs','dfsadmin','-safemode','get')
        if (($safeMode -join ' ') -notmatch 'Safe mode is OFF') { throw 'HDFS must be ready; preserve existing DataNodes/volumes' }
        $logRoot = Join-Path $repoRoot "outputs/validation/gold-training/$($plan.bundle_id)"
        New-Item -ItemType Directory -Path $logRoot -Force | Out-Null
        foreach ($category in @('All_Beauty','Video_Games','Industrial_and_Scientific')) {
            $entry = $plan.categories.$category
            foreach ($uri in @($entry.gold_path,$entry.fold_path,$entry.silver_path)) {
                $path = ([uri]$uri).AbsolutePath
                $fsck = Invoke-TrainingDocker -Arguments @('compose','exec','-T','namenode','hdfs','fsck',$path)
                if (($fsck -join ' ') -notmatch 'is HEALTHY') { throw "Source fsck failed: $path" }
            }
            $stamp = [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssffffffZ')
            $logPath = Join-Path $logRoot ($category + '_' + $stamp + '.log')
            $arguments = @('compose','-f','compose.yaml','-f','compose.datanodes.yaml','-f','compose.gold-light.yaml',
                'run','--detach','--pull','never','--no-deps','--name',"hadoop-demo-training-$stamp",'spark-notebook',
                'python','-m','processing.gold_kaggle','export','--category',$category,'--package',$containerPackage,'--min-free-gib',"$MinFreeGiB")
            $created = @(Invoke-TrainingDocker -Arguments $arguments)
            $ids = @($created | Where-Object { $_ -match '^[a-f0-9]{64}$' })
            if ($ids.Count -ne 1) { throw 'No verified owned export container ID' }
            $containerId = $ids[0]
            Write-Host "Exporting $category; log: $logPath"
            try {
                while ($true) {
                    $free = [IO.DriveInfo]::new([IO.Path]::GetPathRoot($package)).AvailableFreeSpace
                    if ($free -lt ([long]$MinFreeGiB * 1GB)) { throw 'Host disk below reserve' }
                    $state = (Invoke-TrainingDocker -Arguments @('inspect','--format','{{json .State}}',$containerId)) | ConvertFrom-Json
                    if (-not $state.Running) { break }
                    Start-Sleep -Seconds 5
                }
                if ($state.ExitCode -ne 0 -or $state.OOMKilled) { throw "Export failed: exit=$($state.ExitCode), OOM=$($state.OOMKilled)" }
            } finally {
                try { Invoke-TrainingDocker -Arguments @('stop','--timeout','30',$containerId) | Out-Null } finally {
                    try { Invoke-TrainingDocker -Arguments @('logs',$containerId) | Set-Content -LiteralPath $logPath -Encoding UTF8 } finally {
                        Invoke-TrainingDocker -Arguments @('rm',$containerId) | Out-Null
                        $containerId = $null
                    }
                }
            }
        }
        Invoke-TrainingPython -Arguments @('-m','processing.gold_kaggle','finalize','--package',$package)
    }
    if ($Upload) {
        $arguments = @('-m','processing.gold_kaggle','upload','--package',$package,'--handle',$DatasetHandle,'--execute')
        if ($Login) { $arguments += '--login' }
        if ($NewVersion) { $arguments += '--new-version' }
        Invoke-TrainingPython -Arguments $arguments
    }
} finally {
    if ($lockHandle) { $lockHandle.Dispose() }
    Pop-Location
}
