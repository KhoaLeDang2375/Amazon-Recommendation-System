# Đồ án Big Data — hệ thống gợi ý sản phẩm

## Bài toán và dữ liệu

Khi danh mục sản phẩm và lượng đánh giá tăng, người dùng khó tìm được sản phẩm phù hợp; hệ thống gợi ý hỗ trợ cá nhân hóa việc lựa chọn và khám phá sản phẩm. Đề tài xây dựng pipeline Big Data để khai thác dữ liệu đánh giá và thông tin sản phẩm thuộc ba ngành hàng All Beauty, Video Games và Industrial and Scientific của Amazon Reviews 2023. Thực nghiệm giới hạn trên dữ liệu của ba ngành hàng này, với dung lượng được xác nhận sau khi giải nén; kiến trúc hướng đến khả năng mở rộng khi dữ liệu và tương tác người dùng tăng.

Phạm vi chuẩn nằm trong [experiment-scope.json](ingestion/manifests/experiment-scope.json): ba category, reviews và metadata, tổng **7.136.947.646 bytes (~7,14 GB / 6,65 GiB JSONL không nén)**. Quy mô thực nghiệm này chưa phải bằng chứng khả năng mở rộng trên nhiều máy.

## Trạng thái hiện tại

- Đã có ingestion, HDFS và code EDA; máy hiện tại dùng một NameNode và ba DataNode cùng host, replication 1.
- EDA chạy một category mỗi process; batch mặc định Spark local[2], container 2 GiB/CPU 2, driver 1 GiB. Fixture chất lượng dữ liệu và vòng đời đã đạt ngày 14/10/2026; cấu hình hai CPU chưa có số đo tăng tốc raw.
- Ba job EDA full-category đã complete ngày 14/10/2026: All Beauty 701.528, Video Games 4.624.615 và Industrial and Scientific 5.183.005 reviews; cùng scope/code/cutoff. Phân tích và kế hoạch chuyển sang ETL nằm trong [processing.md](docs/processing.md). All Beauty gặp lỗi lưu log wrapper lịch sử, đã sửa; run/summary/resources vẫn có đủ.
- Pha 5 ngày 14/10/2026 đã dọn dữ liệu Sports/Toys/Health ngoài phạm vi; sáu dataset selected HEALTHY trước/sau, vẫn giữ sáu bản nguồn local và toàn bộ HDFS/Flume volumes. Kết quả dung lượng và biên bản nằm trong [kế hoạch dọn repo](docs/KE_HOACH_DON_DEP_REPO.md).
- Đợt điều chỉnh scope và cleanup đã hoàn thành pha 0–6 ngày 14/10/2026. Tài liệu/graph đã cập nhật; notebook cũ đã sao lưu và bỏ output, giữ nguyên source. Checkpoint code/EDA/Silver đã commit tại `2eaa8ba` ngày 22/10/2026; chưa push.
- Silver đã hoàn tất cả ba category ngày 16/10/2026: 10.395.239 reviews, 677.423 catalog rows, loại 113.909 bản sao chính xác; quarantine=0. Parquet đã đọc lại và đối soát. Xem [Silver](docs/silver.md) để lấy run/path và kết quả kiểm tra. [Notebook 04](notebooks/04_als_baseline.ipynb) thử implicit ALS và Popularity trên mẫu Silver; [hướng dẫn](docs/als-baseline.md). G0–G2 Gold ở trạng thái bên dưới; API và web chưa triển khai.

Batch dữ liệu hiện tại đi theo **staging → direct HDFS put → raw → EDA → Silver**. Flume là nhánh ingestion event/batch theo dòng đã có; metadata Video Games được giữ theo layout Flume, không publish lại.

Gold G0–G2 đã có [producer/runner và hợp đồng dữ liệu](docs/gold.md): temporal
pairs/queries/labels/mappings/cohorts, text refs và ZIP text-only cho Kaggle.
[Notebook 05 T4 × 2](notebooks/05_kaggle_text_embeddings.ipynb) dùng MiniLM đã pin
revision, chunk-mean cho review dài, pilot và resume từng shard. Trạng thái từng
full run ở Gold; chưa chạy GPU local, import embeddings hoặc train model mới.

Ngày 30/10/2026 đã bổ sung EDA cutoff trên toàn Silver và khóa validation nửa đầu
2021, refit trước 2022, test nửa đầu 2022; hai cửa sổ cùng 6 tháng. Xem
[cơ sở thống kê và biểu đồ](docs/eda.md#88-so-sánh-cutoff-và-cửa-sổ-trước-gold),
[protocol có version](processing/gold_protocol.json). Chưa tạo Gold; kết quả pilot
ALS cũ giữ nguyên với protocol lịch sử, chưa phải metric của cutoff mới.

## Bắt đầu trên máy đã có dữ liệu — PowerShell host

Chạy tại Git root **project/**; không chạy script tương đối từ C:/Windows/system32.

~~~powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
docker compose -f compose.yaml -f compose.datanodes.yaml config --quiet
docker compose -f compose.yaml -f compose.datanodes.yaml ps -a
~~~

Nếu các HDFS container hiện có đang dừng:

~~~powershell
docker compose -f compose.yaml -f compose.datanodes.yaml start namenode datanode1 datanode2 datanode3
docker compose exec -T namenode hdfs dfsadmin -report
~~~

Không thay volume hoặc giảm số DataNode để tiết kiệm RAM. Hướng dẫn clone mới/build nằm trong [vận hành](docs/operations.md); tải/nhập trong [quản lý dữ liệu](docs/data-management.md).

Khi cần chạy lại EDA dữ liệu thật, chọn một category:

~~~powershell
& ./tools/eda/Run-Eda.ps1 -Category All_Beauty
~~~

Wrapper không tự khởi động HDFS, build image hoặc compact đĩa. Nó kiểm tra tài nguyên, lưu log và dọn container sở hữu. Xem [EDA](docs/eda.md) trước khi chạy; không bật notebook và batch đồng thời. Xem báo cáo cũ bằng cell REPORT_DIRECTORY trong notebook, không tạo Spark.

## Tài liệu và cấu trúc

Notebook train trên Kaggle: [07 ALS](notebooks/07_kaggle_als_full.ipynb),
[08 Content-based](notebooks/08_kaggle_content_based.ipynb),
[09 NCF/NeuMF](notebooks/09_kaggle_ncf.ipynb),
[10 DeepFM D0/D1/D2](notebooks/10_kaggle_deepfm.ipynb).
Các notebook neural có train/validation/refit-test, checkpoint/resume và upload
Dataset private; hướng dẫn và giới hạn ở [kế hoạch/vận hành](docs/kaggle-recommendation-plan.md#10-vận-hành-notebook-ncf-và-deepfm-đã-triển-khai).
Mặc định compact: một trial, ID dim 8, MLP [32,16], projection 16; trần 20
epochs và early stopping theo warm_full NDCG@10. Giữ đủ user/item/tương tác.
Đã có kiểm tra CPU trên dữ liệu nhỏ; chưa benchmark neural full trên Kaggle.

| Nội dung | Điểm đọc chính |
| --- | --- |
| Kiến trúc đang chạy và hướng mở rộng | [architecture.md](docs/architecture.md) |
| Scope, tải, ingest, manifests và retention | [data-management.md](docs/data-management.md) |
| Đặc tả insight, chạy EDA và đọc report | [eda.md](docs/eda.md) |
| Phân tích ba full EDA và kế hoạch processing/snapshot | [processing.md](docs/processing.md) |
| Code, vận hành và kết quả Silver | [silver.md](docs/silver.md) |
| Khởi động/dừng, DataNodes, xử lý lỗi và bảo trì | [operations.md](docs/operations.md) |
| Bằng chứng kiểm chứng theo ngày | [history.md](docs/history.md) |
| Điều hướng source và Graphify | [project-map.md](docs/project-map.md), [graphify.md](docs/graphify.md) |
| Fixture và kiểm thử | [tests/README.md](tests/README.md) |
| Hồ sơ đợt dọn repo đã hoàn thành | [KE_HOACH_DON_DEP_REPO.md](docs/KE_HOACH_DON_DEP_REPO.md) |
| Kế hoạch học thuật của nhóm | [KE_HOACH_TONG_QUAT.md](../doc/KE_HOACH_TONG_QUAT.md) |

Kế hoạch học thuật nằm ở **../doc/** ngoài Git root, không tự đi cùng khi clone repo project; chia sẻ thư mục đó riêng nếu cần. Checklist nền tảng của kế hoạch tuần 1 đã gộp vào kế hoạch tổng quát.

| Thư mục | Vai trò |
| --- | --- |
| infrastructure/ | Image, cấu hình Hadoop/Flume/Spark/Graphify |
| ingestion/ | Scope, manifests và script tải/nhập |
| notebooks/ | Implementation EDA và notebook xem/chạy |
| tests/ | Fixture nhỏ, kiểm thử offline/integration |
| tools/eda, tools/maintenance | Batch EDA, bảo trì VHDX thủ công |
| tools/graphify, graphify-out/ | Công cụ và bản đồ source được sinh |
| docs/ | Các hướng dẫn chuẩn và lịch sử |
| data/, outputs/, artifacts/ | Dữ liệu local, kết quả, model; nội dung lớn không commit |
| apps/web/ | Giữ chỗ; chưa có code web |

## Làm việc cùng nhóm

Commit code, cấu hình, manifest quản lý, tài liệu, fixture nhỏ và graph. Dữ liệu, Docker volumes, .env, token, output và model không đi cùng Git/image. Backup notebook trước khi bỏ output; kiểm tra cả source cells để tránh đưa token vào Git. Dùng git status/diff và chọn file có chủ đích; không stage toàn bộ dữ liệu.

Các hướng dẫn an toàn về Git, notebook, replication và khôi phục nằm trong [operations.md](docs/operations.md). Image/dependency trực tiếp được ghim phiên bản, chưa bảo đảm build byte-identical.
