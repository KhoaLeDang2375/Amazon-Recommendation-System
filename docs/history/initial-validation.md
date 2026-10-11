# Implementation validation

The previous demo persisted NameNode metadata but did not explicitly set `dfs.datanode.data.dir`. The Hadoop image defaults to `/tmp/hadoop-hadoop/dfs/data`, while the old volume was mounted at `/tmp/hadoop-root/dfs/data`. The new configuration explicitly aligns the DataNode directory with its persistent volume.

During migration, existing named volumes and NameNode metadata were reused. HDFS reported that the pre-existing 19-byte `/data/test.txt` has one missing block. The old DataNode container is no longer present, so its original container filesystem is unavailable. The local sample `hadoop-demo/data/test.txt` remains untouched; the corrupt HDFS entry has not been overwritten or deleted. This is separate from validation of newly ingested data under `/project`.

Verified on 2026-10-11:

- Compose configuration validates, and both Flume and Spark images build successfully.
- Existing NameNode/DataNode named volumes are reused; the new explicit DataNode path matches its mount.
- The publishing script validates the three-object UTF-8 JSON Lines sample and queues it with a unique name and SHA256.
- Flume ingests into the isolated `/project/raw/smoke/v1` dataset and closes its output file.
- Spark reads exactly the expected three records (IDs 1, 2, 3 and values 10, 20, 30).
- Recreating Flume replays an empty channel and does not ingest the completed sample again; the Spark check still returns exactly three records.
- Restarting DataNode retains the new sample block; Spark reads the same three records afterwards.
- `hdfs fsck /project/raw/smoke/v1 -files -blocks` reports HEALTHY, with zero missing/corrupt blocks.
- Notebook schema and code syntax validate; Jupyter login endpoint returns HTTP 200.
- `pip check` confirms the installed Python dependencies are consistent.

These checks use a tiny sample, not the user's downloading dataset. They do not establish throughput, large-file memory usage, exactly-once delivery or Colab connectivity.
