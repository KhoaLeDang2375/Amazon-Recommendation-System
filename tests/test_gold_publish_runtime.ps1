# Host-only runner checks: all Docker/Python calls are mocked, no real upload.
$ErrorActionPreference = 'Stop'
$sourceRepo = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$fixture = Join-Path $sourceRepo ('outputs/validation/gold-publish-' + [guid]::NewGuid().ToString('N'))
$repo = Join-Path $fixture 'repo'
$target = Join-Path $repo 'tools/gold/Publish-GoldDataset.ps1'
New-Item -ItemType Directory -Path (Split-Path -Parent $target) -Force | Out-Null
Copy-Item -LiteralPath (Join-Path $sourceRepo 'tools/gold/Publish-GoldDataset.ps1') -Destination $target
$global:publishCalls = [Collections.Generic.List[string]]::new()
$global:publishFault = ''
$global:publishOwned = $false

function Invoke-MockGoldPython {
    $global:LASTEXITCODE = 0
    $global:publishCalls.Add('python ' + ($args -join ' '))
    if ($args -contains 'plan') {
        $categories = @{}
        foreach ($category in @('All_Beauty','Video_Games','Industrial_and_Scientific')) {
            $categories[$category] = @{ gold_path="hdfs://namenode:8020/project/testing/$category/refs"; fold_path="hdfs://namenode:8020/project/testing/$category/folds"; silver_path="hdfs://namenode:8020/project/testing/$category/silver" }
        }
        @{ bundle_id='fixture'; categories=$categories } | ConvertTo-Json -Depth 8
    }
}

function docker {
    $global:LASTEXITCODE = 0
    $global:publishCalls.Add('docker ' + ($args -join ' '))
    if ($args[0] -eq 'ps' -and $global:publishFault -eq 'active') { 'active-notebook' }
    if ($args -contains '-safemode') { 'Safe mode is OFF' }
    if ($args -contains 'fsck') { if ($global:publishFault -eq 'fsck') { 'is CORRUPT' } else { 'is HEALTHY' } }
    if ($args -contains '--detach') {
        if ($global:publishOwned) { throw 'Two simultaneous owned Spark containers' }
        $global:publishOwned = $true
        'a' * 64
    }
    if ($args[0] -eq 'inspect') { @{ Running=$false; ExitCode=$(if ($global:publishFault -eq 'oom') {137} else {0}); OOMKilled=($global:publishFault -eq 'oom') } | ConvertTo-Json }
    if ($args[0] -eq 'logs') { 'fixture log' }
    if ($args[0] -eq 'rm') { $global:publishOwned = $false }
}

try {
    & $target -Python Invoke-MockGoldPython
    if (@($global:publishCalls | Where-Object { $_ -like 'docker *' }).Count) { throw 'Plan invoked Docker' }
    $global:publishCalls.Clear()
    & $target -Python Invoke-MockGoldPython -Export -Upload
    $created = @($global:publishCalls | Where-Object { $_ -match ' --detach ' })
    if ($created.Count -ne 3 -or $global:publishOwned) { throw 'Did not run/clean exactly three sequential exports' }
    $finalize = @($global:publishCalls | Where-Object { $_ -match '^python .* finalize ' })
    $upload = @($global:publishCalls | Where-Object { $_ -match '^python .* upload ' })
    if ($finalize.Count -ne 1 -or $upload.Count -ne 1 -or $upload[0] -notmatch '--handle thoandanh/gold-training-v2 --execute') { throw 'Combined Dataset upload arguments wrong' }
    if ($global:publishCalls.IndexOf($finalize[0]) -ge $global:publishCalls.IndexOf($upload[0])) { throw 'Upload before finalize' }
    $savedPackage = Join-Path $repo 'outputs/processing/gold_training/previous-code-bundle'
    New-Item -ItemType Directory -Path $savedPackage -Force | Out-Null
    Set-Content -LiteralPath (Join-Path $savedPackage 'dataset_manifest.json') -Value '{}'
    $global:publishCalls.Clear()
    & $target -Python Invoke-MockGoldPython -Upload -Login -PackagePath $savedPackage
    if (@($global:publishCalls | Where-Object { $_ -match 'docker| plan$| finalize ' }).Count) { throw 'Existing package retry rebuilt plan/export' }
    if (@($global:publishCalls | Where-Object { $_ -match '^python .* upload .*--execute --login' }).Count -ne 1) { throw 'Existing package retry arguments wrong' }
    foreach ($fault in @('active','fsck','oom')) {
        $global:publishFault = $fault
        $global:publishCalls.Clear()
        $rejected = $false
        try { & $target -Python Invoke-MockGoldPython -Export -Upload } catch { $rejected = $true }
        if (-not $rejected -or $global:publishOwned) { throw "Failed to reject/clean $fault" }
        if (@($global:publishCalls | Where-Object { $_ -match '^python .* upload ' }).Count) { throw 'Upload after failed export' }
        if ($fault -ne 'oom' -and @($global:publishCalls | Where-Object { $_ -match ' --detach ' }).Count) { throw 'Created container after failed preflight' }
    }
    Write-Output 'PASS: plan has no Docker, three sequential exports, finalize before one upload, active/fsck/OOM rejection and cleanup.'
} finally {
    Remove-Item Function:\docker -ErrorAction SilentlyContinue
    Remove-Item Function:\Invoke-MockGoldPython -ErrorAction SilentlyContinue
}
