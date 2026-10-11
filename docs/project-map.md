# Bản đồ đọc nhanh

Luồng hiện tại: staging → publish → spool → Flume → HDFS raw → Spark notebook. Web và artifacts đang giữ chỗ.

| Cần tìm | Đọc |
|---|---|
| Service, port, mount, RAM | `compose.yaml`, `.env.example` |
| HDFS và replication | `infrastructure/hadoop/` |
| Source / Channel / Sink | `infrastructure/flume/` |
| Kiểm tra và publish file | `ingestion/scripts/Publish-File.ps1` |
| Thư mục HDFS | `ingestion/scripts/init-hdfs.sh` |
| Dataset, checksum, số dòng | `ingestion/manifests/video_games_metadata.v1.json` |
| Môi trường EDA | `infrastructure/spark/`, `notebooks/01_eda.ipynb` |
| Smoke test | `tests/` |
| Ý nghĩa lệnh / lỗi | `docs/commands.md`, `docs/troubleshooting.md` |
| Web về sau | `docs/web-demo.md` |
| Graph repository | `tools/graphify/`, `graphify-out/` |

Đường dẫn thật: `/project/{raw,processed,analytics}/video_games_metadata/v1`. Hiện chỉ raw có dữ liệu. Smoke: `/project/testing/{raw,processed,analytics}/smoke/v1`. Legacy: `/project/testing/legacy`.

Giữ volume `hadoop-demo_namenode_data`, `hadoop-demo_datanode_data`, `hadoop-demo_flume_state`. Flume có thể giao lại event khi phục hồi; pipeline không bảo đảm exactly-once. File `.COMPLETED` không đồng nghĩa Sink đã ghi xong.

Graph không chứa dữ liệu tải về, model hoặc `.env`. Dùng graph để tìm nguồn cần đọc; không thay việc kiểm tra nguồn.
