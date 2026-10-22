function Get-ExperimentScope {
    param([string]$Path = (Join-Path $PSScriptRoot '../manifests/experiment-scope.json'))
    $scope = Get-Content -LiteralPath $Path -Raw -Encoding UTF8 | ConvertFrom-Json
    $names = @($scope.categories)
    if ($scope.dataset -cne 'Amazon Reviews 2023' -or
        $scope.categories -isnot [array] -or $names.Count -eq 0 -or
        @($names | Where-Object { $_ -isnot [string] -or $_ -cnotmatch '^[A-Za-z0-9_]+$' }).Count -gt 0 -or
        @($names | Select-Object -Unique).Count -ne $names.Count -or
        $scope.kinds -isnot [array] -or ($scope.kinds -join ',') -cne 'reviews,metadata' -or
        $scope.version -isnot [string] -or $scope.version -cnotmatch '^[A-Za-z0-9_-]+$' -or
        $scope.hdfs_base -cne '/project/raw') {
        throw 'Invalid experiment scope; inspect ingestion/manifests/experiment-scope.json'
    }
    return $scope
}
