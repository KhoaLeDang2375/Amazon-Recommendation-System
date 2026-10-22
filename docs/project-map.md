# Bản đồ đọc nhanh

Đọc source để xác minh; graph chỉ giúp điều hướng và có thể cũ. Scope chung: [experiment-scope.json](../ingestion/manifests/experiment-scope.json), ba ngành hàng và reviews/metadata. Batch: staging → direct put → HDFS raw → EDA một category; Flume là nhánh ingestion event/batch có routing riêng.

| Cần tìm | Đọc |
| --- | --- |
| Bài toán, phạm vi, trạng thái và quickstart | [README](../README.md) |
| Mục tiêu học thuật/backlog nhóm | [kế hoạch tổng quát](../../doc/KE_HOACH_TONG_QUAT.md), ngoài Git root |
| Service, port, mount, tài nguyên | [compose.yaml](../compose.yaml), [DataNodes overlay](../compose.datanodes.yaml), [EDA overlay](../compose.eda-light.yaml), [.env.example](../.env.example) |
| Kiến trúc hiện tại và phần chưa triển khai | [architecture.md](architecture.md) |
| Download/put/Flume/manifests/retention | [data-management.md](data-management.md), [ingestion](../ingestion/README.md) |
| Scope loaders | [Python](../ingestion/scripts/dataset_scope.py), [PowerShell](../ingestion/scripts/Get-ExperimentScope.ps1) |
| Raw Video metadata finalized | [manifest](../ingestion/manifests/video_games_metadata.v1.json) |
| EDA spec/run/report | [eda.md](eda.md), [implementation](../notebooks/eda_recommendation.py), [notebook 02](../notebooks/02_eda_recommendation.ipynb) |
| EDA bổ sung trên Silver trước Gold | [kế hoạch và vận hành](eda.md#8-kế-hoạch-eda-bổ-sung-trên-silver-trước-gold), [implementation](../notebooks/eda_model_readiness.py), [notebook 03](../notebooks/03_eda_model_readiness.ipynb), [runner](../tools/eda/Run-ModelReadiness.ps1) |
| Pilot implicit ALS trên mẫu Silver | [hướng dẫn](als-baseline.md), [notebook 04](../notebooks/04_als_baseline.ipynb), [implementation](../notebooks/als_baseline.py), [kiểm tra](../tests/test_als_baseline.py) |
| Kết quả ba full EDA, rules và kế hoạch processing/snapshot | [processing.md](processing.md) |
| Silver: code/schema/lưu trữ, vận hành và kết quả | [silver.md](silver.md), [silver.py](../processing/silver.py), [Run-Silver.ps1](../tools/silver/Run-Silver.ps1) |
| Batch/host disk monitor | [Run-Eda.ps1](../tools/eda/Run-Eda.ps1) |
| Khởi động/dừng/nodes/lỗi/Git/compact | [operations.md](operations.md), [Reclaim-DockerDisk.ps1](../tools/maintenance/Reclaim-DockerDisk.ps1) |
| Fixture và regression | [tests/README.md](../tests/README.md) |
| Bằng chứng có ngày, migration/checksum | [history.md](history.md) |
| Hồ sơ đợt cleanup đã hoàn thành pha 0–6 | [KE_HOACH_DON_DEP_REPO.md](KE_HOACH_DON_DEP_REPO.md) |
| Graph/code navigation | [graphify.md](graphify.md), [refresh](../tools/graphify/Refresh-Graph.ps1), [GRAPH_REPORT](../graphify-out/GRAPH_REPORT.md) |

Raw thật: /project/raw/<category_lower>_{reviews,metadata}/v1; chỉ sáu dataset thuộc thực nghiệm. Pha 5 ngày 14/10/2026 đã dọn Sports/Toys/Health khỏi HDFS và staging; inventory/bằng chứng ở outputs/validation/phase5-20261014/, không commit. Testing: /project/testing/{raw,processed,analytics}/smoke/v1, legacy riêng. Silver ba category đã complete ngày 16/10/2026; pilot ALS trên mẫu Silver chạy ngày 22/10/2026. Pipeline Gold tổng quát và web chưa triển khai.

Giữ identity hadoop-demo và tất cả named volumes, gồm ba DataNode hiện có và Flume state. Block replication 1 có thể trên cả ba node; không format/xóa volume/giảm node như bước dọn. Flume drain trước routing changes; .COMPLETED khác Sink finalized, không bảo đảm exactly-once.

Graph loại data/artifacts/outputs/.env/caches; không dùng nó thay source hoặc đọc payload để hiểu repo. Các hướng dẫn cũ đã gộp vào data-management/eda/operations/history, không tạo bản doc song song.
