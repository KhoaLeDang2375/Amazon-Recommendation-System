#!/usr/bin/env bash
# One-time, idempotent migration; refuses to merge or overwrite destinations.
set -euo pipefail
hdfs dfsadmin -safemode wait
for layer in raw processed analytics; do
  source="/project/$layer/dataset"
  destination="/project/$layer/video_games_metadata"
  if hdfs dfs -test -e "$source"; then
    if hdfs dfs -test -e "$destination"; then
      echo "Both $source and $destination exist; refusing to merge." >&2
      exit 1
    fi
    hdfs dfs -mv "$source" "$destination"
  fi
  hdfs dfs -mkdir -p "/project/testing/$layer"
  if hdfs dfs -test -e "/project/$layer/smoke"; then
    if hdfs dfs -test -e "/project/testing/$layer/smoke"; then
      echo 'Testing destination already exists; refusing to merge.' >&2
      exit 1
    fi
    hdfs dfs -mv "/project/$layer/smoke" "/project/testing/$layer/smoke"
  fi
done
if hdfs dfs -test -e /data/test.txt; then
  hdfs dfs -mkdir -p /project/testing/legacy
  if hdfs dfs -test -e /project/testing/legacy/test.txt; then
    echo 'Legacy destination already exists; refusing to overwrite.' >&2
    exit 1
  fi
  hdfs dfs -mv /data/test.txt /project/testing/legacy/test.txt
fi
hdfs dfs -ls /project/raw
