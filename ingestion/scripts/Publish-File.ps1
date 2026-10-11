param(
    [Parameter(Mandatory = $true)][string]$File
)
$ErrorActionPreference = 'Stop'
$projectRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
$stagingRoot = [IO.Path]::GetFullPath((Join-Path $projectRoot 'data/staging'))
$spoolRoot = [IO.Path]::GetFullPath((Join-Path $projectRoot 'data/spool'))
$sourcePath = (Resolve-Path -LiteralPath $File).Path
if (-not $sourcePath.StartsWith($stagingRoot + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'Input must be a completed file inside data/staging.'
}
if (-not (Test-Path -LiteralPath $sourcePath -PathType Leaf)) { throw 'Input must be a file.' }
$extension = [IO.Path]::GetExtension($sourcePath).ToLowerInvariant()
if ($extension -notin @('.jsonl', '.ndjson', '.log', '.txt')) {
    throw 'Only uncompressed .jsonl, .ndjson, .log, .txt files are accepted. Use hdfs dfs -put for other formats.'
}
$utf8 = New-Object System.Text.UTF8Encoding($false, $true)
$reader = New-Object System.IO.StreamReader($sourcePath, $utf8, $false)
$records = 0
try {
    while ($null -ne ($line = $reader.ReadLine())) {
        $records++
        if ($utf8.GetByteCount($line) -gt 1048576) { throw "Line $records exceeds the configured 1 MiB limit." }
        if ($extension -in @('.jsonl', '.ndjson')) {
            if ([string]::IsNullOrWhiteSpace($line)) { throw "Empty JSON line at $records." }
            if (-not $line.TrimStart().StartsWith('{')) { throw "JSON line $records must be an object." }
            # ConvertFrom-Json without -AsHashtable uses case-insensitive property
            # names and rejects valid case-distinct keys such as "Name" and "name".
            if ((Get-Command ConvertFrom-Json).Parameters.ContainsKey('AsHashtable')) {
                $parsedJson = ConvertFrom-Json -InputObject $line -AsHashtable -ErrorAction Stop
            } else {
                # Windows PowerShell 5.1 compatibility; JavaScriptSerializer uses
                # case-sensitive JSON member names, unlike ConvertFrom-Json's PSCustomObject.
                Add-Type -AssemblyName System.Web.Extensions
                $jsonSerializer = New-Object System.Web.Script.Serialization.JavaScriptSerializer
                $jsonSerializer.MaxJsonLength = 2097152
                $parsedJson = $jsonSerializer.DeserializeObject($line)
            }
            if ($parsedJson -isnot [System.Collections.IDictionary]) {
                throw "JSON line $records must decode to an object with named properties."
            }
        }
    }
} finally { $reader.Dispose() }
if ($records -eq 0) { throw 'Empty input file.' }
$hash = (Get-FileHash -LiteralPath $sourcePath -Algorithm SHA256).Hash.ToLowerInvariant()
$stem = [IO.Path]::GetFileNameWithoutExtension($sourcePath) -replace '[^a-zA-Z0-9_-]', '_'
$unique = [Guid]::NewGuid().ToString('N')
$filename = "${stem}_${unique}${extension}"
$partialPath = Join-Path $spoolRoot ".$filename.partial"
$targetPath = Join-Path $spoolRoot $filename
Copy-Item -LiteralPath $sourcePath -Destination $partialPath
Move-Item -LiteralPath $partialPath -Destination $targetPath
[pscustomobject]@{ File = $filename; Records = $records; SHA256 = $hash; Status = 'Queued for Flume, not yet confirmed in HDFS' }
