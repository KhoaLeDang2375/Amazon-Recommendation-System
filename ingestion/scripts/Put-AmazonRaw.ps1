param([switch]$Apply)
$ErrorActionPreference = 'Stop'
$projectRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
Push-Location $projectRoot
try {
    function Invoke-Hdfs {
        param([string[]]$Arguments)
        $result = & docker compose exec -T namenode hdfs dfs @Arguments
        if ($LASTEXITCODE -ne 0) { throw "HDFS command failed: $($Arguments -join ' ')" }
        return $result
    }
    $manifest = Get-Content -Raw -Encoding UTF8 'data/staging/amazon-reviews-2023/download-manifest.json' | ConvertFrom-Json
    $categories = @('All_Beauty', 'Video_Games', 'Sports_and_Outdoors', 'Toys_and_Games', 'Industrial_and_Scientific', 'Health_and_Personal_Care')
    $stagingRoot = [IO.Path]::GetFullPath((Join-Path $projectRoot 'data/staging'))
    $existing = @{}
    foreach ($line in @(Invoke-Hdfs -Arguments @('-ls', '-R', '/project/raw'))) {
        if ($line -match '^-' ) {
            $fields = $line.Trim() -split '\s+', 8
            if ($fields.Count -ne 8) { throw "Unexpected HDFS listing: $line" }
            $existing[$fields[7]] = [long]$fields[4]
        }
    }
    $plan = @()
    foreach ($category in $categories) {
        foreach ($kind in @('reviews', 'metadata')) {
            $name = if ($kind -eq 'metadata') { "meta_$category.jsonl" } else { "$category.jsonl" }
            $directory = "/project/raw/$($category.ToLowerInvariant())_${kind}/v1"
            $target = "$directory/$name"
            $entries = @($manifest.files | Where-Object { ($_.filename -split '/')[-1] -eq $name })
            if ($entries.Count -ne 1) { throw "Missing or duplicate manifest entry: $name" }
            $entry = $entries[0]
            if ($entry.status -notin @('downloaded_verified', 'skipped_verified')) { throw "Download not verified: $name" }
            $datasetFiles = @($existing.Keys | Where-Object { $_.StartsWith("$directory/", [StringComparison]::Ordinal) })
            if ($existing.ContainsKey($target)) {
                if ($existing[$target] -ne [long]$entry.size -or $datasetFiles.Count -ne 1) {
                    throw "Existing dataset differs from expected file; inspect manually: $directory"
                }
                Write-Host "SKIP existing target (size matches): $target"
                continue
            }
            if ($datasetFiles.Count -gt 0) {
                # Preserve the known finalized Flume ingestion instead of adding duplicate source rows.
                $legacy = Get-Content -Raw -Encoding UTF8 'ingestion/manifests/video_games_metadata.v1.json' | ConvertFrom-Json
                $batchPrefix = "$directory/meta_Video_Games_"
                $unexpected = @($datasetFiles | Where-Object { -not $_.StartsWith($batchPrefix, [StringComparison]::Ordinal) })
                if ($category -eq 'Video_Games' -and $kind -eq 'metadata' -and
                    $legacy.ingestion.status -eq 'finalized' -and $legacy.hdfs_path -eq $directory -and
                    $legacy.files[0].sha256 -eq $entry.sha256 -and
                    $datasetFiles.Count -eq $legacy.ingestion.output_files -and $unexpected.Count -eq 0) {
                    Write-Host "SKIP known finalized Flume dataset: $directory"
                    continue
                }
                throw "Dataset contains other files; refusing duplicate ingestion: $directory"
            }
            $local = [IO.Path]::GetFullPath([string]$entry.path)
            if (-not $local.StartsWith($stagingRoot + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
                throw "Source must be inside data/staging: $name"
            }
            if (-not (Test-Path -LiteralPath $local -PathType Leaf)) {
                # A user may move completed files after download; manifest paths can be stale.
                $candidates = @((Join-Path $stagingRoot "amazon-reviews-2023/$name"), (Join-Path $stagingRoot $name))
                $found = @($candidates | Where-Object { Test-Path -LiteralPath $_ -PathType Leaf })
                if ($found.Count -eq 0) { throw "Source missing from manifest path and staging locations: $name" }
                $local = [IO.Path]::GetFullPath($found[0])
            }
            if ((Get-Item -LiteralPath $local).Length -ne [long]$entry.size) { throw "Source size mismatch: $name" }
            $hash = (Get-FileHash -LiteralPath $local -Algorithm SHA256).Hash.ToLowerInvariant()
            if ($hash -ne $entry.sha256) { throw "Source SHA256 mismatch: $name" }
            $relative = $local.Substring($stagingRoot.Length).Replace('\', '/')
            $source = '/ingestion/staging' + $relative
            $plan += [pscustomobject]@{ Source = $source; Directory = $directory; Target = $target; Bytes = [long]$entry.size }
            Write-Host "PLAN replication=1: $source -> $target"
        }
    }
    $bytes = ($plan | Measure-Object -Property Bytes -Sum).Sum
    Write-Host ("Remaining: {0} files, {1:N3} GB" -f $plan.Count, ($bytes / 1e9))
    if (-not $Apply) { Write-Host 'Preview only. Use -Apply to put the missing files.'; return }
    foreach ($item in $plan) {
        Invoke-Hdfs -Arguments @('-mkdir', '-p', $item.Directory) | Out-Host
        # No -f: never overwrite. Hadoop keeps an unfinished transfer as ._COPYING_.
        Invoke-Hdfs -Arguments @('-D', 'dfs.replication=1', '-put', $item.Source, $item.Target) | Out-Host
        $actual = Invoke-Hdfs -Arguments @('-stat', '%b', $item.Target)
        if ([long]$actual -ne $item.Bytes) { throw "Uploaded size mismatch: $($item.Target)" }
        Write-Host "OK $($item.Target)"
    }
    Write-Host 'Complete. Verify raw blocks with: docker compose exec -T namenode hdfs fsck /project/raw'
} finally {
    Pop-Location
}
