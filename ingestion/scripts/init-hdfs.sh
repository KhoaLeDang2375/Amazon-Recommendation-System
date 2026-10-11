#!/usr/bin/env bash
set -euo pipefail
dataset="${DATASET_NAME:-video_games_metadata}"
version="${DATASET_VERSION:-v1}"
for component in "$dataset" "$version"; do
  if [[ ! "$component" =~ ^[a-zA-Z0-9][a-zA-Z0-9_-]*$ ]]; then
    echo 'Dataset and version must contain only letters, digits, underscore or hyphen.' >&2
    exit 1
  fi
done
hdfs dfsadmin -safemode wait
for layer in raw processed analytics; do
  hdfs dfs -mkdir -p "/project/$layer/$dataset/$version"
done
hdfs dfs -ls /project
