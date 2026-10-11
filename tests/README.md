# Kiểm thử nhỏ

`fixtures/ingestion/smoke.jsonl` gồm ba record; `fixtures/legacy/test.txt` lưu ví dụ cũ. Smoke HDFS nằm riêng dưới `/project/testing/raw/smoke/v1`.

Với Spark đang chạy và smoke đã có trong HDFS:

```powershell
docker compose exec -T spark-notebook python /workspace/tests/integration/verify-smoke.py
```

Script kiểm tra notebook format/cú pháp và ba record smoke. Trên clone mới, copy fixture lên vùng testing bằng `-put`, không publish vào routing dataset thật:

```powershell
docker compose cp .\tests\fixtures\ingestion\smoke.jsonl namenode:/tmp/smoke.jsonl
docker compose exec namenode hdfs dfs -mkdir -p /project/testing/raw/smoke/v1
docker compose exec namenode hdfs dfs -put /tmp/smoke.jsonl /project/testing/raw/smoke/v1/
```

Chỉ nhập fixture một lần; script mong đúng ba record.
