# Kiến trúc và phạm vi triển khai

## Hệ thống hiện tại

Phạm vi lấy từ [experiment-scope.json](../ingestion/manifests/experiment-scope.json): All Beauty, Video Games, Industrial and Scientific; hai bảng reviews/metadata. Tổng JSONL không nén đã đối chiếu là 7.136.947.646 bytes. Các category ngoài scope có thể còn tồn tại trên máy cho đến pha dọn dữ liệu; không thuộc thực nghiệm mới.

~~~mermaid
flowchart LR
    HF[Amazon Reviews 2023] --> DL[Python download + checksum]
    DL --> ST[Host staging]
    ST --> PUT[Put-AmazonRaw: preview / apply]
    PUT --> RAW[HDFS raw]
    ST --> PUB[Publish batch mới có chủ đích]
    PUB --> SP[Host spool]
    SP --> FL[Flume Source / File Channel / HDFS Sink]
    FL --> RAW
    RAW --> EDA[Spark local: một category mỗi process]
    EDA --> OUT[Host outputs: aggregate / report]
    RAW --> SILVER[Spark Silver: chuẩn hóa và exact dedup]
    SILVER --> PQ[HDFS Parquet: reviews / catalog / audit / quarantine]
~~~

Metadata Video Games đã qua Flume; direct put giữ nguyên dataset finalized đó để tránh nhập đôi. Flume không phải bước bắt buộc của batch tải sẵn. Nhánh publish chỉ dùng khi đích và batch mới đã được chốt.

| Service | Vai trò và trạng thái |
| --- | --- |
| namenode | Namespace, vị trí block; volume NameNode hiện có |
| datanode1/2/3 | Block trên ba volume riêng; máy hiện tại đang dùng cả ba |
| hdfs-init, các service init | Chuẩn bị đường dẫn/quyền; one-shot, Exited (0) có thể bình thường |
| flume | Nhập event theo dòng; bật khi cần, state/checkpoint bền vững |
| spark-notebook | Cùng image cho Jupyter hoặc batch; profile eda |
| graphify | Công cụ điều hướng, profile tools; CPU 1/RAM 512 MiB |

Một NameNode, **ba DataNode trên cùng host**, replication 1. Base Compose khai báo datanode1; overlay bổ sung datanode2/3. Block hiện có có thể phân bố trên cả ba node; base một node không phải căn cứ để tắt/xóa các node còn lại. Thêm container DataNode không thêm ổ đĩa vật lý; replica cùng host không bảo vệ trước mất cả host.

Batch PySpark hiện mặc định local[2], driver 1 GiB, container RAM 2 GiB/CPU 2; direct CLI/notebook mặc định local[1]. Hai job EDA All Beauty/Video Games hoàn tất với local[1], Industrial với local[2]. Không có Spark Master/Worker riêng, YARN, HA hoặc GPU. HDFS có nhiều dịch vụ lưu trữ nhưng compute Spark hiện tại vẫn local. API xử lý bằng Spark có thể được đưa lên cluster sau này; chưa có benchmark multi-host.

## Mạng, lưu trữ và ingestion

Trong mạng Compose, Spark/Flume dùng hdfs://namenode:8020 và hostname DataNode. Trình duyệt host dùng localhost với port publish: mặc định NameNode 9870, Jupyter 8888, Spark UI 4040. Token Jupyter chỉ dùng cho server đang chạy, không chia sẻ qua Git.

Named volumes giữ metadata, blocks và File Channel. Bind mounts giữ staging/spool, source notebook, outputs và artifacts trên host. Project identity hadoop-demo và tên volume là phần của dữ liệu cần bảo toàn.

Flume Spooldir đọc UTF-8 theo dòng, tối đa 1 MiB/dòng; File Channel giữ event/checkpoint. Sink DataStream/TEXT gom file khoảng 128 MiB, idle timeout 15 giây, file mở có đuôi .tmp. Luồng này không giữ nguyên bytes nguồn hoặc bảo đảm exactly-once/thứ tự toàn cục. .COMPLETED chỉ chứng minh Source đã đọc file vào channel; hoàn tất ingestion còn cần channel drain và HDFS đóng file.

Direct HDFS put dùng cho batch JSONL hiện tại và định dạng cần giữ bytes (Parquet, ZIP...). Raw được giữ nguyên; quy tắc làm sạch thuộc processed. Chi tiết manifests, routing và retry nằm trong [quản lý dữ liệu](data-management.md).

## Các tầng dữ liệu

| Tầng | Vai trò |
| --- | --- |
| /project/raw/<category>_<kind>/v1 | Sáu dataset gốc trong scope; Video metadata giữ batch layout cũ |
| /project/processed/silver/<category>/v1/<run_id> | Đã có reviews_clean/catalog/quarantine/duplicate audit, Parquet Snappy và manifest complete cho ba category |
| /project/processed/gold/... | Dự kiến: split, lịch sử, features và labels |
| /project/analytics/... | Dự kiến: bảng tổng hợp phục vụ phân tích |
| /project/testing/... | Smoke/fixture; tách khỏi raw thật |
| outputs/eda/ | Report EDA local, config và log; khác HDFS analytics |
| artifacts/analytics, artifacts/models | Dự kiến: kết quả nhỏ/model cho serving; không commit payload |

Snapshot thực nghiệm có thể tham chiếu dữ liệu bất biến bằng manifest, không phải copy toàn raw mỗi run. Không partition theo từng user/item. Mọi tầng mới cần schema, source lineage, data/code version, checksum khi phù hợp và quy tắc unknown ID.

## Hướng mở rộng, chưa triển khai

Pipeline mục tiêu: raw → Silver → Gold train/validation/test → baseline/cải tiến → evaluate → model/top-K → phục vụ và batch cập nhật. Ba full EDA và Silver đã complete; [silver.md](silver.md) giữ schema/run/path và kết quả đọc lại. Cohort, split/candidates và features Gold trong [processing.md](processing.md) vẫn là đề xuất. Baseline đề xuất train riêng theo source_category để giới hạn tài nguyên; train chung cần đo overlap và đối chiếu catalog.

apps/web, artifacts/analytics và artifacts/models hiện là nơi giữ chỗ. Web sau này đọc catalog/top-K/artifact đã xuất hoặc gọi inference API; không chạy EDA/train hay quét raw cho mỗi request. Chốt schema artifact, mapping ID, preprocessing, version và fallback trước viết UI. API/UI có thể gộp cho demo nhỏ; tách khi có nhu cầu cụ thể.

Tương tác ứng dụng cần namespace user riêng, event ID, event time/ingestion time và checkpoint. Batch retraining không tự là incremental learning. Việc cập nhật pipeline thành công khác với mô hình chắc chắn tốt hơn; cần cùng giao thức đánh giá và khả năng rollback.

MLflow, PostgreSQL, Kafka, Airflow, Warehouse riêng, chatbot và GPU cloud đều là lựa chọn sau khi có nhu cầu/yêu cầu cụ thể. Không thêm service chỉ để đủ sơ đồ. Kaggle/Colab chưa kết nối HDFS local; nếu dùng, xuất snapshot nhỏ qua storage được chọn, kiểm tra mapping/version/checksum khi nhập artifact về và công bố bước thủ công. Không mở HDFS ra Internet cho demo.

Kế hoạch học thuật và nghiệm thu nằm trong [kế hoạch tổng quát](../../doc/KE_HOACH_TONG_QUAT.md), ngoài Git root. Cách vận hành thực tế nằm trong [operations.md](operations.md).
