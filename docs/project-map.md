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
| So sánh và khóa cutoff bằng thống kê Silver | [EDA mục 8.8](eda.md#88-so-sánh-cutoff-và-cửa-sổ-trước-gold), [implementation](../notebooks/eda_cutoff.py), [fixture](../tests/test_eda_cutoff.py); runner trên với `-CutoffStudy` |
| Pilot implicit ALS trên mẫu Silver | [hướng dẫn](als-baseline.md), [notebook 04](../notebooks/04_als_baseline.ipynb), [implementation](../notebooks/als_baseline.py), [kiểm tra](../tests/test_als_baseline.py) |
| Kết quả ba full EDA, rules và kế hoạch processing/snapshot | [processing.md](processing.md) |
| Kế hoạch Gold, Kaggle embedding và ba bản DeepFM | [processing mục 6.2–6.3](processing.md#63-kế-hoạch-triển-khai-tầng-gold); trạng thái từng phần ở [Gold](gold.md) |
| Triển khai G0–G2 và notebook Kaggle T4 × 2 | [vận hành Gold](gold.md), [producer](../processing/gold.py), [contract](../processing/gold_contract.json), [runner](../tools/gold/Run-Gold.ps1), [notebook 05](../notebooks/05_kaggle_text_embeddings.ipynb) |
| Tải và đọc embedding Kaggle về local | [notebook 06](../notebooks/06_download_read_embeddings.ipynb), [kiểm tra offline](../tests/test_download_read_embeddings.py); đọc theo batch, chưa import HDFS |
| Xuất Gold thành một Kaggle Dataset training | [runner PowerShell](../tools/gold/Publish-GoldDataset.ps1), [export/upload Python](../processing/gold_kaggle.py), [hướng dẫn](gold.md#xuất-gold-training-thành-một-kaggle-dataset); ba ngành riêng thư mục, chưa train model |
| ALS full và content-based trên Kaggle | [notebook 07](../notebooks/07_kaggle_als_full.ipynb), [notebook 08](../notebooks/08_kaggle_content_based.ipynb), [source chung](../notebooks/kaggle_recommendation.py), [metric/kế hoạch/vận hành](kaggle-recommendation-plan.md); đã kiểm tra fixture, chưa benchmark T4 full |
| NCF/NeuMF và DeepFM D0/D1/D2 trên Kaggle | [notebook 09](../notebooks/09_kaggle_ncf.ipynb), [notebook 10](../notebooks/10_kaggle_deepfm.ipynb), [neural source](../notebooks/kaggle_neural_recommendation.py), [builder](../tools/gold/Build-NeuralNotebooks.py), [vận hành](kaggle-recommendation-plan.md#10-vận-hành-notebook-ncf-và-deepfm-đã-triển-khai); compact dim8, early stopping, sampler vector hóa/prefetch, device Top-K và projection cache (mục 12); CPU fixtures đạt, chưa full GPU benchmark |
| Hybrid Two-Tower trên Gold và embedding Kaggle | [notebook 11](../notebooks/11_kaggle_two_tower.ipynb), [neural source](../notebooks/kaggle_neural_recommendation.py), [builder](../tools/gold/Build-NeuralNotebooks.py), [vận hành mục 14](kaggle-recommendation-plan.md#14-vận-hành-notebook-two-tower-08112026): TT0/TT1/TT2, independent encoders + cached dot-product, CPU fixtures; chưa benchmark full Kaggle |
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
