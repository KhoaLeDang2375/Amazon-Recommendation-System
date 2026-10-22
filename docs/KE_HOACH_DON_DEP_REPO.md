# Kế hoạch điều chỉnh phạm vi và dọn code, tài liệu, dữ liệu

Ngày khảo sát và kết thúc: **14/10/2026**. Trạng thái: **đã hoàn thành pha 0–6; đợt điều chỉnh scope và cleanup đã đóng**. Đây là hồ sơ kế hoạch/kết quả lịch sử, không phải checklist công việc đang chờ. Đường dẫn trong tài liệu tính từ Git root `project/`; `../doc/` là tài liệu đồ án bên ngoài Git root. Kết quả từng pha ghi ở mục 10. Hướng dẫn hiện hành nằm trong README/project-map; các bảng rà soát dưới đây mô tả hiện trạng trước khi sửa.

## 1. Phạm vi thống nhất

Sử dụng nguyên văn mô tả bài toán đã chốt:

> **Bài toán và dữ liệu:** Khi danh mục sản phẩm và lượng đánh giá tăng, người dùng khó tìm được sản phẩm phù hợp; hệ thống gợi ý hỗ trợ cá nhân hóa việc lựa chọn và khám phá sản phẩm. Đề tài xây dựng pipeline Big Data để khai thác dữ liệu đánh giá và thông tin sản phẩm thuộc ba ngành hàng All Beauty, Video Games và Industrial and Scientific của Amazon Reviews 2023. Thực nghiệm giới hạn trên dữ liệu của ba ngành hàng này, với dung lượng được xác nhận sau khi giải nén; kiến trúc hướng đến khả năng mở rộng khi dữ liệu và tương tác người dùng tăng.

Tên hiển thị và tên kỹ thuật phải ánh xạ nhất quán:

| Hiển thị | Category trong code/nguồn | Dataset HDFS cần giữ |
| --- | --- | --- |
| All Beauty | `All_Beauty` | `all_beauty_reviews/v1`, `all_beauty_metadata/v1` |
| Video Games | `Video_Games` | `video_games_reviews/v1`, `video_games_metadata/v1` |
| Industrial and Scientific | `Industrial_and_Scientific` | `industrial_and_scientific_reviews/v1`, `industrial_and_scientific_metadata/v1` |

Các đường dẫn HDFS ở trên nằm dưới `/project/raw/`. Giữ nguyên đường dẫn đang có; không đổi tên hoặc nhập lại raw chỉ để đồng nhất layout.

### Dung lượng đã đối chiếu

Đã đọc manifest tải và đối chiếu `Length` của 6 file JSONL local, không đọc lại toàn bộ payload hoặc tính lại SHA256 trong lượt khảo sát này. Manifest ghi revision nguồn `2b6d039ed471f2ba5fd2acb718bf33b0a7e5598e`, có SHA256 và trạng thái verified từ lần tải trước.

| Category | Reviews, bytes | Metadata, bytes | Tổng GB thập phân |
| --- | ---: | ---: | ---: |
| All_Beauty | 326.611.506 | 212.990.142 | 0,540 |
| Video_Games | 2.682.948.518 | 437.375.785 | 3,120 |
| Industrial_and_Scientific | 2.347.015.351 | 1.130.006.344 | 3,477 |
| **Tổng** | **5.356.575.375** | **1.780.372.271** | **7,137** |

Tổng chính xác **7.136.947.646 bytes**, khoảng **7,14 GB / 6,65 GiB** dữ liệu JSONL không nén. Không giữ các tuyên bố 15 GB, 18,70 GB, 20 GB hoặc 30,8 GB làm quy mô thực nghiệm mới. Số dòng của từng dataset chỉ ghi khi có bằng chứng đếm tương ứng; không ước lượng từ byte.

Dung lượng dữ liệu đầu vào khác dung lượng đĩa cần vận hành: giữ cả local và HDFS sẽ cần khoảng 14,27 GB dữ liệu logic, chưa tính spool, processed, shuffle, cache, image và hệ thống tệp. Replication, hardlink và VHDX khiến số bytes logic khác dung lượng vật lý.

## 2. Kết quả rà soát repo

| Vị trí | Vấn đề cụ thể | Điều chỉnh |
| --- | --- | --- |
| `../doc/KE_HOACH_TONG_QUAT.md`, `KE_HOACH_TUAN_1.md` | Vẫn chọn Sports, Musical, Video và gần 20 GB; thử Musical trước | Thay bằng phạm vi mới; All Beauty là lượt thử nhỏ; sửa lịch theo trạng thái thực tế |
| `ingestion/scripts/download_amazon_hf.py` | Mặc định tải sáu category, ba worker | Mặc định ba category trong scope; tải tuần tự một worker trên máy hiện tại |
| `ingestion/scripts/Put-AmazonRaw.ps1` | Danh sách sáu category; kiểm tra toàn `/project/raw` | Dùng scope mới; kiểm tra đúng sáu dataset đích; giữ cơ chế chống nhập trùng |
| `notebooks/eda_recommendation.py`, `Run-Eda.ps1` | CLI mặc định một category nhưng danh sách hỗ trợ vẫn sáu | Chỉ cho chạy ba category trong scope thực nghiệm; một category mỗi process |
| `notebooks/02_eda_recommendation.ipynb` | Chọn `list(CATEGORIES)`, `local[2]`, shuffle 16; sẽ bị guard một category từ chối | Một category; chung cấu hình với CLI; phần xem báo cáo không khởi tạo Spark |
| `README.md`, `docs/eda.md` | Vẫn hướng dẫn chạy sáu category và 30,8 GB | Viết lại hướng dẫn theo batch từng category, cập nhật phạm vi và trạng thái |
| `AGENTS.md`, `docs/data-management.md` | Dataset mặc định vẫn chỉ Video Games metadata | Dẫn đến scope ba category; phân biệt dữ liệu thật, smoke và lịch sử |
| `notebooks/_cleanup_eda_runtime.py` | Helper chữa sự cố với hai run ID cố định | Loại khỏi code vận hành sau khi kiểm tra cleanup trong vòng đời job |
| `Reclaim-EdaDocker.ps1` | Đường dẫn VHDX cố định theo ASUS; tác động toàn Docker/WSL | Chuyển sang công cụ bảo trì có tham số và hướng dẫn riêng |
| Tài liệu vận hành | Download, ingestion, commands, troubleshooting, validation, history phân tán | Gộp các nội dung trùng, giữ bằng chứng kiểm chứng và cảnh báo volume |
| Notebook cũ, ảnh kiến trúc, thay đổi Git hiện có | Có nội dung người dùng chưa commit | Bảo toàn; không coi untracked hoặc modified là file rác |

File `outputs/eda-reclaim-result.json` ghi VHDX giảm từ 131.088.777.216 xuống 41.879.076.864 bytes, thu hồi 83,08 GiB; dung lượng trống C từ 116,15 lên 200,91 GiB, Docker/WSL được dừng. Đây là kết quả của lần bảo trì đã ghi, không phải số đo ổ C hiện tại. Không cần compact tiếp chỉ vì lượt dọn repo này.

Hai run EDA cũ đều có trạng thái toàn run `failed`; run sau ghi hoàn tất All Beauty và Video Games. Có thể giữ kết quả từng category để tham khảo nếu truy vết đúng nguồn và code version. Chưa được ghi “đã EDA đủ ba ngành hàng”; Industrial and Scientific cần kết quả riêng. Các sửa cấu hình nhẹ gần đây cũng cần kiểm thử lại trước khi xác nhận ổn định.

## 3. Nguyên tắc dọn repo

1. Một nơi định nghĩa phạm vi; các entrypoint đọc cùng nguồn, không sao chép danh sách category ở Python, PowerShell và notebook.
2. Giữ cấu trúc repo hiện tại nếu việc di chuyển không giải quyết vấn đề cụ thể. Chưa cần dựng framework ETL, package nhiều tầng hoặc đổi Git root.
3. Giữ raw nguyên nghĩa; EDA chỉ đọc raw và xuất báo cáo. Quy tắc lọc thuộc processing và phải được quyết định từ số liệu.
4. Giữ project identity `hadoop-demo`, tất cả named volumes và Flume state. Không format HDFS, `down -v`, prune volumes hoặc xóa block trên DataNode.
5. Các file bị bỏ phải có nội dung đã được gộp hoặc được xác định là ngoài phạm vi. Dọn dữ liệu có danh sách đường dẫn và báo cáo xem trước riêng.
6. Không chạy EDA dữ liệu thật trong các bước dọn code/docs/data. Lượt chạy thực nghiệm là bước sau khi kiểm thử nhỏ đạt và người dùng yêu cầu chạy lại.

## 4. Điều chỉnh code và cấu hình

### 4.1. Khai báo phạm vi dùng chung

Thêm **một file nhỏ** `ingestion/manifests/experiment-scope.json` với dataset `Amazon Reviews 2023`, ba category theo đúng thứ tự, hai loại `reviews/metadata`, version raw và base HDFS. File được commit, không chứa token, đường dẫn máy cá nhân hoặc trạng thái tải.

Manifest tải trong staging tiếp tục là bằng chứng nguồn: revision, filename, SHA256, bytes, thời điểm và trạng thái. Manifest ingest tiếp tục ghi trạng thái HDFS. Không sửa manifest tải cũ từ 12 thành 6 entry rồi mất lịch sử; lưu manifest lịch sử và tạo bản hoạt động cho sáu file khi triển khai. Bổ sung nguồn Video Games từ manifest đã xác minh khi checksum trùng; không suy đoán ngày tải ban đầu.

Python và PowerShell dùng JSON chuẩn có sẵn. Nếu cần hàm đọc dùng chung cho Python, chỉ thêm helper nhỏ và kiểm tra schema, không dựng lớp cấu hình mới. Khi file scope không hợp lệ hoặc category ngoài phạm vi, dừng trước đọc dữ liệu.

### 4.2. Ingestion

| File | Công việc | Điều kiện đạt |
| --- | --- | --- |
| `download_amazon_hf.py` | Đọc scope; mặc định 3 category/2 loại; worker mặc định 1; giữ revision pin, checksum, resume và không overwrite | Dry-run chỉ lập kế hoạch 6 file; category ngoài scope bị từ chối |
| `Put-AmazonRaw.ps1` | Thêm chọn category trong scope; mặc định toàn scope cho preview; dùng manifest hiện tại và kiểm tra từng dataset đích | Preview không ghi HDFS; `-Apply` chỉ put file thiếu; chạy lại không nhân đôi dữ liệu |
| `video_games_metadata.v1.json` | Giữ provenance và dấu vết Flume 4 output; đối chiếu nguồn đã có | Không put thêm nguyên file metadata vào dataset Flume đã hoàn tất |
| `Publish-Spool.ps1`, init/migrate scripts | Giữ cho ingestion Flume và migration có chủ đích; ghi rõ vai trò lịch sử/streaming | Không republish dữ liệu đã có; migration không được chạy như bước setup thường lệ |

Kiểm tra size trùng là điều kiện cần, không phải bằng chứng đầy đủ nội dung giống nhau. Ghi nhận ingest provenance/checksum từ lần xác minh; khi thiếu bằng chứng hoặc file thay đổi thì lên lịch xác minh trước khi skip. Không hash lại hàng chục GB mỗi lệnh preview.

Batch JSONL hiện tại ưu tiên direct HDFS put. Flume giữ vai trò demo tiếp nhận batch/log hoặc dữ liệu cập nhật; không đưa cả sáu file qua Flume để đáp ứng sơ đồ kiến trúc.

### 4.3. EDA và notebook

Giữ `notebooks/eda_recommendation.py` làm implementation dùng chung trong đợt này để giảm thay đổi import/mount. Chỉnh cấu trúc hàm nếu có lỗi cụ thể; không tách thành nhiều module chỉ vì file dài.

- CLI, notebook và wrapper nhận **một category**. Mặc định All Beauty. Nếu hỗ trợ chạy cả scope sau này, wrapper gọi ba process lần lượt, dừng ngay khi một lượt lỗi và không tự retry job nặng.
- Notebook chỉ có chọn category/cấu hình, gọi implementation và xem bảng. Tách cell đọc một `report_dir` có sẵn; xem báo cáo không cần JVM.
- Dùng chung cấu hình tạo Spark; loại khác biệt `local[2]`/16 partitions giữa notebook và CLI. `spark.stop()` và `unpersist(blocking=True)` chạy trong `finally` theo đúng quyền sở hữu session.
- Giữ parser raw, case-sensitive, UTC, kiểm tra JSON lỗi/duplicate keys, missing/type/domain, timestamp seconds/milliseconds, duplicates, metadata conflicts và join không nhân dòng.
- Thu hẹp cột trước cache/join; từng cache có mục đích, vòng đời và nơi giải phóng rõ. Không cache raw text hoặc nested metadata toàn bộ chỉ để đếm.
- Tách logic bổ sung report lịch sử `complete_id_overview` khỏi đường chạy chính; khi counts được tạo đầy đủ trong lượt mới thì bỏ helper và các test chỉ phục vụ bản vá cũ, sau khi bảo toàn report cần thiết.
- Loại helper `_cleanup_eda_runtime.py` chứa run ID cố định khi đã có cleanup vòng đời và công cụ bảo trì an toàn.
- Summary ghi bytes, file list/fingerprint nguồn, scope version, code version nếu có, tham số, thời gian, trạng thái và các bước hoàn tất. `failed/interrupted` không được trình bày như báo cáo đầy đủ.

Không bật mặc định cross-category overlap hoặc 5-core. So sánh ba category trước hết đọc **summary aggregate nhỏ** từ ba run; kiểm tra cùng nguồn/quy ước rồi ghép bảng. Tổng user/item distinct toàn scope không suy ra bằng cộng các category. Nếu chưa chạy phép đo overlap thì ghi **chưa đo**, không xuất bảng rỗng dễ bị hiểu là overlap bằng 0. Không gọi sparsity của một category là global toàn scope.

### 4.4. Tài nguyên và bảo trì

Giữ `compose.eda-light.yaml` với cấu hình khởi điểm CPU 1, container RAM 2 GiB, driver 1 GiB. Đây là cấu hình thử và cần kiểm chứng với workload; không hứa đủ cho mọi groupBy/join. Mỗi category dùng container batch `--rm --no-deps`; không mở Jupyter khi chỉ chạy batch.

Chuyển `Run-Eda.ps1` sang `tools/eda/Run-Eda.ps1`, cập nhật cách tìm repo root và toàn bộ liên kết/callers. Wrapper kiểm tra trước chạy: HDFS đang sẵn sàng, không có job EDA khác, ổ chứa outputs và ổ chứa Docker còn dung lượng dự phòng. Trên Windows không mặc định dữ liệu Docker luôn ở C.

Guard hiện tại chỉ kiểm tra free space khi bắt đầu trong container; `/tmp` phản ánh filesystem Docker, không đảm bảo host còn chỗ cho VHDX tăng. Bổ sung kiểm tra host ở wrapper và kiểm tra giữa các pha lớn; ghi RAM/đĩa tạm trước, sau, mức quan sát lớn nhất. Ngưỡng ban đầu 20 GiB chỉ là mức dự phòng khởi điểm, phải hiệu chỉnh từ đo thực tế; một Spark action vẫn có thể vượt ngưỡng giữa hai lần kiểm tra.

Kiểm tra `spark.local.dir` nằm trong filesystem tạm của container batch, không vô tình đặt vào mount host hoặc volume lâu dài. Container thoát sẽ giải phóng file trong lớp ghi; kernel notebook còn sống có thể giữ cache. Xóa file trong Linux không tự thu nhỏ Docker VHDX trên Windows.

Chuyển `Reclaim-EdaDocker.ps1` sang `tools/maintenance/Reclaim-DockerDisk.ps1` và cập nhật `repoPath`. Nhận tham số VHDX thay đường dẫn ASUS cố định; kiểm tra đường dẫn thật, reparse point, trạng thái detach và kết quả compact. Giữ quyền Administrator và chế độ bảo trì thủ công. Ghi rõ việc dừng Docker/WSL ảnh hưởng các phiên WSL khác; không gắn compaction vào cuối mọi run EDA. Không thêm global prune.

Compose giữ nguyên volume và mount dữ liệu hiện có. Chỉ chỉnh mount cần thiết cho scope/helper/script mới. Hướng dẫn khởi động batch chỉ bật dịch vụ HDFS cần thiết; Flume được bật khi làm ingestion tương ứng. Với cluster hiện tại, tiếp tục giữ đủ DataNode đang chứa block; mặc định “một DataNode” trong tài liệu không áp dụng để tự giảm cluster đang có.

## 5. EDA phải đủ căn cứ cho processing

Kế hoạch EDA hiện có bao phủ tốt chất lượng dữ liệu, duplicates, phân bố, thời gian, long tail và metadata join. Giảm xuống ba category **không cần bỏ các nhóm insight này**. Cần thay phạm vi, cơ chế chạy và cách chuyển insight thành quyết định rõ ràng.

| Câu hỏi processing | Bằng chứng cần có cho từng category | Quyết định được phép đưa ra |
| --- | --- | --- |
| Bản ghi nào không đọc/không dùng được? | Parse lỗi, thiếu khóa, loại dữ liệu, timestamp, rating; số lượng và mẫu truy vết | Tách quarantine; không âm thầm drop và không sửa raw |
| Copies hay tương tác lặp? | Exact duplicates, event duplicates, repeated user-parent; xung đột rating/time | Quy tắc dedup có khóa/thứ tự xác định; không loại mọi cặp lặp |
| Product key và metadata join có an toàn? | `asin`/`parent_asin`, metadata singleton/copies/conflicts, unmatched, row-count assert | Dùng parent-level catalog nếu được xác minh; metadata conflict chưa resolve không tự merge |
| Có cần text/metadata để giữ interaction? | Missing theo tổ hợp; CF-only và content-ready cohorts | Giữ interaction CF hợp lệ dù thiếu text/price; chỉ gate feature content tương ứng |
| Lọc lịch sử có làm mất phần lớn dữ liệu? | Distinct degree, user ≥2/3/5, retention user/item/pairs; 5-core nếu có lý do | Chọn ngưỡng theo tradeoff; giữ nhóm cold-start riêng, không mặc định 5-core |
| Chia tập thời gian thế nào? | Phân bố UTC, ties, train/validation/test support, warm/cold và repeated pairs qua cutoff | Chốt cutoff sau khảo sát; cutoff 2022 hiện tại chỉ là diagnostic |
| Feature nào dễ gây leakage? | Metadata snapshot, average_rating/rating_number/price; thời điểm text review | Không dùng thống kê tương lai hoặc review của tương tác test làm feature sẵn có lúc gợi ý |
| Category có đủ dữ liệu đánh giá Top-K? | User có positive test hợp lệ, catalog train/candidate, coverage và mức mất sau lọc | Chốt positive threshold và candidate policy; N/A khi không có mẫu, không gán metric 0 |

Mỗi report có bảng hành động: **vấn đề → số liệu và mẫu số → quy tắc đề xuất → ảnh hưởng dự kiến → điều chưa chốt**. Phân biệt quyết định đã có bằng chứng với giả thuyết cần ETL thử. Tỷ lệ empty categories không tự là JSON lỗi; số lần xuất hiện phần tử chia số dòng có thể lớn hơn 1 và phải đặt nhãn phù hợp.

All Beauty từng cho thấy lịch sử user rất ngắn và metadata content thiếu nhiều; ngưỡng lọc mạnh có nguy cơ loại đa số người dùng. Kết quả cũ là bằng chứng cần kiểm tra lại theo đúng fingerprint/phiên bản, không phải lý do loại cả category. Industrial phải được đo riêng, không suy diễn schema hay độ đầy đủ từ Video Games.

Đợt này chuẩn bị hợp đồng processed: reviews/interactions, product catalog, quarantine, schema/version và lineage; chưa viết ETL/model/web mới. Giao thức Top-K giữ đánh giá theo category và trung bình category; train chung hay riêng là quyết định riêng. Đề xuất kịch bản chính gợi ý trong category để kiểm soát candidate set; chưa mở rộng cross-domain nếu không có mục tiêu/giao thức riêng.

## 6. Hợp nhất tài liệu

Ưu tiên một tài liệu cho mỗi chủ đề. Giữ kế hoạch đồ án ở `../doc/` trong đợt này; README chỉ liên kết và ghi rõ tài liệu ngoài Git root không đi cùng clone repo. Không sao chép nguyên kế hoạch vào docs thêm một lần.

Bảng dưới là kế hoạch đối chiếu **file nguồn trước hợp nhất → tài liệu đích**. Pha 3 đã thực hiện; tên file bị bỏ được giữ trong bảng như lịch sử thay đổi, không phải liên kết/hướng dẫn đang dùng.

| File hiện tại | Xử lý dự kiến | Nơi chứa nội dung sau dọn |
| --- | --- | --- |
| `README.md` | Viết gọn: bài toán, scope/bytes, cấu trúc, quickstart, trạng thái đã làm/chưa làm | Giữ |
| `../doc/KE_HOACH_TONG_QUAT.md` | Thay phạm vi, dung lượng, mốc tiến độ; giữ protocol và mục tiêu mô hình | Giữ làm kế hoạch đồ án chính |
| `../doc/KE_HOACH_TUAN_1.md` | Gộp phần còn hữu ích thành checklist nền tảng/backlog trong kế hoạch chính | Bỏ bản trùng sau đối chiếu, không giữ lịch tuần 1 như việc chưa bắt đầu |
| `docs/architecture.md` | Phân biệt hệ thống chạy thật và hướng mở rộng; direct put + Flume, Spark local; ba category | Giữ |
| `docs/web-demo.md` | Gộp yêu cầu tương lai vào architecture; web/model vẫn placeholder | Bỏ file riêng sau gộp |
| `docs/data-management.md` | Scope, manifests, paths, lineage, retention và điều kiện dọn | Giữ |
| `docs/download-amazon-hf.md`, `ingestion/README.md` | Gộp hướng dẫn download/put/Flume vào data-management; chỉ giữ README thư mục rất ngắn nếu cần định vị | Bỏ nội dung vận hành trùng |
| `docs/eda.md`, `notebooks/KE_HOACH_EDA.md` | Gộp đặc tả insight, quy ước, cách chạy, output và giới hạn vào `docs/eda.md` | Một tài liệu EDA chuẩn; bỏ kế hoạch cũ sau đối chiếu đủ nội dung |
| `docs/commands.md`, `troubleshooting.md`, `datanodes.md` | Gộp lệnh và xử lý sự cố vào `docs/operations.md`; giữ cảnh báo block/volume và compaction | Một hướng dẫn vận hành |
| `docs/github.md` | Rút gọn phần làm việc nhóm, Git, secrets và môi trường vào README/operations | Bỏ file riêng sau gộp |
| `docs/validation.md`, `docs/history/*.md` | Gộp bằng chứng checksum, 4 Flume output, migration và kiểm chứng cũ có ngày vào `docs/history.md` | Một lịch sử kiểm chứng; không dùng như trạng thái chạy hiện tại |
| `docs/project-map.md`, `AGENTS.md` | Cập nhật entrypoint và quy tắc theo layout cuối | Giữ, ngắn và nhất quán |
| `docs/graphify.md` | Giữ hướng dẫn công cụ cho bảo trì, ngoài quickstart bắt buộc | Giữ vì AGENTS yêu cầu graph/navigation |
| `tests/README.md` | Fixture/offline/smoke, điều kiện môi trường và lệnh theo layout mới | Giữ |
| `docs/architecture-proposed-20261013.png` | Xác định có được dùng; chỉ bỏ nếu không còn giá trị và đã có bản thay thế | Không xóa chỉ vì untracked |
| Tài liệu kế hoạch này | Checklist thực hiện; đánh dấu kết quả qua từng pha | Kết thúc đợt dọn thì gộp kết quả vào history/backlog, tránh thêm một doc lỗi thời |

Các kế hoạch học thuật phải bỏ lời hứa thực nghiệm gần 20 GB, cập nhật nguồn ba category và lấy số byte từ manifest. MLflow, PostgreSQL, GPU cloud, chatbot, orchestration chỉ giữ là lựa chọn mở rộng khi có nhu cầu; không thêm dịch vụ rỗng trong lần cleanup. Giữ baseline/cải tiến/Top-K nếu đó là mục tiêu đồ án đã chốt.

Kiểm tra lại mọi relative link, notebook path, mount và ví dụ lệnh sau move/merge. Tách rõ PowerShell host, shell container và Python notebook. Không dùng các lệnh phá volume trong quickstart.

## 7. Dọn dữ liệu có kiểm kê

### 7.1. Dữ liệu giữ và dữ liệu ngoài phạm vi

**Giữ:** sáu raw dataset của ba category đã chọn, manifest nguồn/ingest, named volumes, Flume channel/checkpoint, fixture nhỏ, code người dùng và các report hoàn tất cần đối chiếu.

**Ngoài phạm vi:** Sports and Outdoors, Toys and Games, Health and Personal Care. Musical Instruments chỉ xuất hiện trong kế hoạch cũ; không giả định có file để xóa.

| Category ngoài phạm vi | Review + metadata local/HDFS, bytes logic theo manifest | GB |
| --- | ---: | ---: |
| Sports_and_Outdoors | 13.389.119.810 | 13,389 |
| Toys_and_Games | 9.954.004.210 | 9,954 |
| Health_and_Personal_Care | 344.996.167 | 0,345 |
| **Tổng** | **23.688.120.187** | **23,688** |

Đây là snapshot trước cleanup, dung lượng logic mỗi bộ bản sao, không phải cam kết số GB ổ C sẽ thu hồi. Lượt khảo sát ban đầu có sáu file local ngoài phạm vi, chưa khởi động Docker hoặc kiểm chứng HDFS. Pha 5 đã kiểm kê/xác minh và dọn các target này; kết quả thực đo ở mục 10.

### 7.2. Trình tự dọn khi triển khai

1. Lập inventory chính xác bằng metadata: local path, bytes, vai trò, link/reparse status; HDFS dataset path, bytes, file list và trạng thái finalized. Chỉ duyệt các thư mục biết rõ, không quét payload toàn đĩa.
2. Xác minh sáu dataset giữ lại healthy, không thiếu block, không file đang ghi. Đối chiếu source/ingest manifest; Video metadata giữ layout Flume 4 file. Có bản nguồn phục hồi ngoài ổ đang dọn hoặc khả năng tải lại revision/checksum.
3. Xuất danh sách cleanup xem trước theo đúng 6 file local ngoài scope và 6 namespace HDFS tương ứng. Lưu inventory/bằng chứng trước khi thực hiện. Mỗi target resolve phải nằm trong staging hoặc prefix HDFS ngoài scope đã chỉ định; từ chối wildcard rộng, symlink và target mơ hồ.
4. Dọn HDFS bằng API/lệnh filesystem HDFS ở namespace dataset, không xóa thư mục block trên DataNode. Kiểm tra chính sách Trash: chuyển vào Trash có thể chưa trả dung lượng. Xóa vĩnh viễn là bước có chủ đích với inventory đã duyệt, không mặc định dùng bypass Trash.
5. Dọn 6 file local ngoài scope. Kiểm tra `.hf-home`, `.hf-downloads`, spool completed và `.invalid-*` để xác định file dở/cache thực sự liên quan. Downloader hiện tạo hardlink rồi unlink nguồn tạm khi thành công; không giả định cache luôn chứa bản JSONL thứ hai. Không cộng dung lượng hai đường dẫn hardlink như hai bản vật lý.
6. Spool chỉ dọn file hoàn tất sau drain và có bằng chứng ingest; giữ `.flumespool`/channel state. Không republish để khôi phục đường dẫn.
7. Dọn `_work` của run đã dừng và output dang dở ngoài scope nếu không còn giá trị chẩn đoán. Giữ một bộ report tham chiếu All Beauty/Video Games, bỏ bản lặp sau đối chiếu. Run failed có category complete vẫn cần ghi rõ trạng thái từng phần.
8. Giữ local JSONL của ba category làm nguồn phục hồi trong lần đầu. Chỉ chuyển sang ổ khác/xóa bản local khi HDFS đã xác minh và có phương án phục hồi; không bắt buộc giữ cả spool, staging và archive cho cùng một nguồn.
9. Đo lại local/HDFS/host free/VHDX. Nếu Linux đã trả block nhưng Windows chưa thu hồi tương ứng, mới dùng bảo trì compact sau khi dừng sạch. Không hứa quay về một dung lượng ban đầu cố định.

### 7.3. Quy tắc lưu trữ về sau

- Raw HDFS giữ ba category; snapshot processed có version và không ghi đè nguồn.
- Một nguồn local phục hồi khi cần, ưu tiên ổ ngoài C nếu máy có; không tự di chuyển toàn bộ Docker hoặc HDFS trong cleanup này.
- Run hoàn tất giữ aggregate, summary, cấu hình và báo cáo; scratch mặc định bị xóa khi kết thúc. Log lỗi giữ có thời hạn, không tự xóa run đang chạy.
- Không đưa raw, processed lớn, hashed edges, model, cache, token hoặc notebook output vào Git. Rà `.gitignore`, kể cả `outputs/eda-reclaim-result.json` và file DiskPart đang untracked; không chặn các fixture nhỏ cần test.
- Notebook `01_eda.ipynb` có thay đổi người dùng: backup ngoài Git trước khi bỏ output hoặc chuyển sang archive local; so sánh source cells và chuyển phần hữu ích trước khi retire. Không reset/overwrite.
- Không giảm từ ba DataNode xuống một như biện pháp dọn: replication 1 có thể phân bố block trên cả ba. Nếu sau này cần giảm node phải có kế hoạch decommission/migration và fsck riêng.

## 8. Thứ tự triển khai và nghiệm thu

| Pha | Việc làm | Đầu ra/điều kiện chuyển pha |
| --- | --- | --- |
| 0 — Bảo toàn hiện trạng | Ghi Git diff/untracked; backup notebook; inventory manifests và report; xác định file do người dùng tạo | Không mất code, không xóa data; có danh sách giữ/dọn chính xác |
| 1 — Chốt scope | Thêm scope JSON; sửa downloader, put, EDA selectors và AGENTS | Các entrypoint dùng cùng ba category; dry-run không tải/put/EDA |
| 2 — Ổn định vận hành | Đồng nhất notebook/CLI; wrapper batch; tài nguyên/cleanup/status; chuyển công cụ bảo trì | Một category mỗi process; guard trước scan; không có cleanup run ID cố định |
| 3 — Dọn tài liệu | Sửa kế hoạch đồ án; merge docs theo bảng; cập nhật README/map/links | Không còn hướng dẫn thực nghiệm sáu category hoặc Sports/Musical; phân biệt hiện tại/tương lai |
| 4 — Kiểm thử nhỏ | Tests offline/fixture; kiểm tra Compose và PowerShell; smoke HDFS riêng nếu môi trường sẵn sàng | Không dùng raw lớn làm test; không ghi vào raw thật; báo rõ test đã chạy/chưa chạy |
| 5 — Dọn dữ liệu | Review danh sách xóa; xác minh selected raw; thực hiện cleanup theo mục 7 | Healthy trước/sau; selected raw giữ nguyên; báo bytes logic và free thực tế |
| 6 — Bàn giao | Refresh graph; rà Git staged; ghi thay đổi và việc còn thiếu | Repo dễ điều hướng, không commit output/private data; chưa tự chạy EDA toàn scope |

Pha 5 chỉ thực hiện khi inventory và bằng chứng bảo toàn đủ, trong phạm vi dọn dữ liệu được người dùng cho phép. Người dùng đã yêu cầu triển khai tuần tự, đánh giá từng phần; không gộp mọi pha vào một đợt thay đổi. Không chờ hoàn tất tài liệu mới chặn một hành vi mặc định nguy hiểm ở pha 1–2.

### Kiểm thử có ý nghĩa

- Scope: đúng ba category/sáu dataset; cùng kết quả ở downloader, put preview, CLI và notebook; từ chối category ngoài scope.
- Download: giữ checksum/revision/resume/idempotency; fixture không truy cập nguồn lớn.
- Ingest: thiếu entry, sai size/checksum, file đích khác, Flume finalized, file đang ghi và không nhân đôi dữ liệu khi chạy lại.
- EDA: fixture lỗi JSON/type/key, missing, duplicates/conflicts, UTC seconds/milliseconds, density trên distinct pairs, row-count join, temporal warm/cold và diagnostic gates. Giữ test đã có nếu còn mô tả hành vi cần thiết.
- Tài nguyên: fixture chạy trong batch container nhẹ; normal/error/interruption đều dừng Spark và không giữ scratch; thiếu disk dừng trước scan. Không viết test chỉ lặp lại danh sách cấu hình.
- Repo: Compose config hợp lệ, import/mount/path mới dùng được, liên kết Markdown tồn tại, không có raw/output/token trong staged diff.

Kiểm tra nhỏ không chứng minh Industrial toàn bộ sẽ vừa RAM/đĩa. Sau khi được yêu cầu chạy EDA lại: All Beauty trước, đo tài nguyên và xác nhận report; tiếp Video Games rồi Industrial, mỗi lượt một container. Chỉ nâng tài nguyên hoặc chia pha khi có số đo/lỗi cụ thể. Sau ba report hợp lệ mới làm bảng so sánh và chốt quy tắc processing.

## 9. Checklist hoàn thành đợt dọn

- [x] README, kế hoạch đồ án, scope và tất cả entrypoint cùng ba category.
- [x] Dung lượng mới được ghi bằng byte/GB/GiB có nguồn; không tuyên bố benchmark khả năng mở rộng từ một máy local.
- [x] Notebook không chọn tất cả category mặc định; chỉ xem report không khởi tạo Spark.
- [x] Default batch nhẹ và vòng đời process rõ; RAM limit không bị hiểu là giới hạn đĩa (đã kiểm chứng fixture ở pha 2; chưa đo raw lớn).
- [x] Đặc tả EDA đủ căn cứ cho dedup, joins, content/CF, lọc lịch sử và temporal evaluation (policy cuối cùng còn cần report raw).
- [x] Docs trùng đã gộp trước khi bỏ; không mất checksum/provenance/history quan trọng.
- [x] Code notebook và file chưa commit của người dùng được bảo toàn (backup pha 0 và trước pha 2).
- [ ] Sáu raw dataset đã chọn healthy và nguyên vẹn; named volumes/Flume state được giữ.
- [ ] Dữ liệu ngoài scope/cache/output được dọn theo inventory cụ thể; số thu hồi được đo thực tế.
- [x] Tests nhỏ có kết quả, graph/map/links được cập nhật; Industrial full EDA còn thiếu được ghi rõ.

Thứ tự ưu tiên: **scope và mặc định an toàn → vòng đời EDA → hợp nhất tài liệu → kiểm thử nhỏ → dọn dữ liệu có kiểm kê → chạy thực nghiệm ở lượt riêng**.

## 10. Tiến độ và đánh giá từng pha

### Đợt 1 — bảo toàn và scope, 14/10/2026

**Pha 0 đạt:** đã lưu Git diff trước sửa vào `data/archive/before-scope-20261014.patch`, danh sách Git status và bản sao notebook/code/report specification chưa commit trong `data/archive/before-scope-20261014/`. Đối chiếu SHA256 nguồn/bản sao đạt. Không reset notebook người dùng, không thay manifest tải, không xóa raw/spool/volume.

**Pha 1 đạt kiểm tra offline:** thêm scope JSON cùng loader Python/PowerShell có validation. Downloader mặc định ba category, sáu file và một worker. Put preview chỉ kiểm kê namespace được chọn, giữ skip metadata Flume đã finalized; nhận `-Categories` và từ chối ngoài scope. EDA CLI đọc scope trước tạo Spark; notebook mặc định một category. Wrapper từ chối category ngoài scope trước gọi Docker. Compose bổ sung mount read-only manifest để driver container đọc scope.

Kết quả kiểm tra:

- `test_experiment_scope.py`: **3 test đạt**, gồm scope/driver, scope không hợp lệ và notebook chọn một category; không khởi tạo Spark.
- `test_download_amazon_hf.py`: **6 test đạt**, gồm checksum/corruption, resume/idempotency, selector và worker mặc định. Lần đầu sandbox chặn tạo hardlink NTFS; chạy lại fixture với quyền phù hợp đạt, không tải mạng.
- `test_scope_scripts.ps1`: **đạt**, Docker và manifest tải được giả lập, preview sáu dataset, không put/mkdir, giữ Flume skip, wrapper một category và reject trước Docker.
- Compose base + overlay nhẹ: `config --quiet` **đạt**, không khởi động container.

**Giới hạn đánh giá:** chưa chạy Spark fixture, chưa kiểm chứng HDFS live, chưa xác nhận peak RAM/đĩa. Notebook vẫn dùng cấu hình Spark cũ và code còn tùy chọn cross-category; việc đồng nhất vòng đời/tài nguyên thuộc pha 2. Docs học thuật/EDA cũ chờ pha 3. Graph hiện ghi ngày 13/10 và chưa phản ánh loader/scope mới; refresh bằng container tools ở pha bàn giao, không khởi động Docker chỉ để chạy graph trong đợt này. Nguồn và project-map mới là căn cứ điều hướng hiện tại.

**Bước tiếp theo:** pha 2, tập trung vòng đời Spark, cấu hình notebook/CLI, batch wrapper và guard tài nguyên; đánh giá bằng fixture nhỏ trước dọn tài liệu/data. Không cần chạy lại EDA raw để xác nhận pha 1.

### Đợt 2 — vòng đời EDA và tài nguyên, 14/10/2026

**Pha 2 đạt kiểm tra nhỏ và Docker thật:**

- CLI/notebook dùng chung `run_eda` và `create_spark`: một category, local[1], driver 1 GiB, shuffle 64, local dir dưới `/tmp`. Không tái sử dụng Spark session đang tồn tại; `finally` dừng session sở hữu và khôi phục handler SIGTERM. Cell cấu hình/xem report không tạo Spark.
- Cache parser được đăng ký trước action; cả hai lần load nằm trong `try/finally`. Cache 5-core trung gian cũng được đăng ký trước count, unpersist blocking và bỏ tham chiếu khi không cần. Cleanup cố giải phóng mọi cache ngay cả khi một cache bị lỗi; giữ exception gốc khi phân tích đã thất bại.
- Bỏ luồng cross-category, scratch hashed edges và CLI vá report cũ `complete_id_overview`. Counts ID vẫn được tạo trong report mới. Bảng comparison dùng `category_sparsity`; global distinct/overlap ghi `not_measured`, không xuất số 0 hoặc gọi số liệu category là global.
- `resources.json` lưu snapshot đĩa/cgroup memory ở ranh giới pha; guard trước tạo JVM và trước các pha lớn. `run.json` ghi code/scope fingerprint, config, thời điểm hoàn tất và status complete/failed/interrupted.
- Wrapper chuyển sang `tools/eda/Run-Eda.ps1`: lock độc quyền, kiểm tra Spark đang chạy, image có sẵn, HDFS safemode và hai ổ host chứa output/VHDX. Theo dõi host định kỳ 5 giây; ghi nối tiếp JSONL để tránh viết lại lịch sử lớn. Dùng container detach, đọc exit/log rồi dừng/xóa đúng ID trong finally; không khởi động HDFS hoặc xóa volume.
- Công cụ compact chuyển sang `tools/maintenance/Reclaim-DockerDisk.ps1`, nhận VHDX path, không cố định ASUS/C; từ chối junction/symlink và container Spark/Flume khác đang chạy. Bảo trì vẫn yêu cầu Administrator và dừng toàn Docker/WSL; **chưa chạy công cụ này trong pha 2**.
- Xóa helper chữa sự cố có run ID cố định sau khi backup. Notebook `01_eda.ipynb` không sửa; raw/staging/spool/manifests tải/named volumes không dọn trong pha này.

**Bằng chứng đánh giá:**

| Kiểm tra | Kết quả |
| --- | --- |
| Scope host | 3 test đạt; selector notebook vẫn theo scope |
| Runtime trong image Spark, không JVM | 7 test đạt: thiếu disk/multi-category trước JVM, stop thành công/lỗi/SIGTERM, cache khi load lỗi, status failed/interrupted, drop disk giữa pha, từ chối session cũ |
| Spark thật với fixture adversarial | 1 integration test đạt trong 75,257 giây, container RAM 2 GiB/CPU 1; giữ các assert quality/duplicates/joins/temporal/5-core; không còn persistent RDD và Spark context sau stop |
| PowerShell offline | Preview 6 dataset, giữ Flume skip; batch success/failure cleanup, host guard, selector reject và JSONL samples đạt |
| Wrapper với Docker thật, chỉ job mock | Exit code/log/samples đạt; một lượt bình thường và một lượt monitor bị ngắt; SIGTERM gọi stop session giả, container bị dọn |
| Compose base + DataNodes + overlay | Config hợp lệ, không thay volume/name |
| Công cụ compact | Parse PowerShell đạt; không thực thi DiskPart/WSL shutdown/compact |

Tùy chọn `--no-build` không được Compose v5.1.4 trên máy hỗ trợ; đã thay bằng kiểm tra image có sẵn và `--pull never`. Kiểm thử Spark in cảnh báo dependency socket/pyparsing; các assert và exit code vẫn đạt. Không nâng dependency trong pha ổn định vòng đời này.

**Giới hạn:** fixture vài dòng chứng minh logic/vòng đời, chưa chứng minh raw Video Games/Industrial vừa RAM hoặc thời gian chạy. Guard host và phase snapshots không phải quota; action có thể vượt dự phòng giữa hai lần đo. Forced kill/OOM có thể để status `running`; cần đối chiếu exit code/OOM/log trước xem kết quả là hoàn tất. Chưa compact, chưa dọn dataset, chưa chạy EDA thật.

**Bước tiếp theo:** pha 3, hợp nhất tài liệu và cập nhật kế hoạch đồ án theo ba ngành hàng. Không thêm ETL/model/web hoặc chạy raw trong đợt này.

### Đợt 3 — hợp nhất docs và kế hoạch học thuật, 14/10/2026

**Pha 3 hoàn thành:** tài liệu Markdown trong Git root giảm **21 → 12**, kế hoạch học thuật ngoài Git root giảm **2 → 1**. Đã backup 23 tài liệu và notebook 02 tại `data/archive/before-phase3-20261014/`, đối chiếu SHA256 bản sao trước khi gộp/bỏ.

Các tài liệu chuẩn sau hợp nhất:

| Nội dung | Nơi đọc |
| --- | --- |
| Bài toán/scope/trạng thái/quickstart/index | README.md |
| Hiện tại và hướng mở rộng | docs/architecture.md |
| Scope/download/put/Flume/provenance/retention | docs/data-management.md |
| Đặc tả đầy đủ insight và vận hành EDA | docs/eda.md |
| Startup/stop/DataNodes/lỗi/Git/compact | docs/operations.md |
| Migration/smoke/checksum và kiểm chứng theo ngày | docs/history.md |
| Source navigation/công cụ | docs/project-map.md, docs/graphify.md |
| Kế hoạch nhóm và checklist nền tảng tuần 1 đã gộp | ../doc/KE_HOACH_TONG_QUAT.md |

Đã bỏ 11 tài liệu repo trùng và kế hoạch tuần 1 sau khi chuyển nội dung hữu ích. Ingestion README chỉ còn định vị và link hướng dẫn chuẩn; ảnh kiến trúc đề xuất ngày 13/10 được giữ như lịch sử. Kế hoạch đồ án dùng nguyên văn mô tả bài toán đã chốt, bytes/revision đúng manifest và backlog có trạng thái; giữ protocol Top-K/leakage/artifact/retraining, không coi MLflow/cloud/web hoặc compute cluster là đã triển khai.

Đặc tả EDA giữ các nhóm missing/types/domain/duplicates/conflicts/rating/price/time/sparsity/coverage/gates/temporal và mẫu số; phân biệt output đã có với global overlap/URL availability/phân tích bổ sung chưa đo. Notebook chỉ sửa Markdown dẫn đến docs/eda.md; code cells giữ nguyên, notebook 01 không đổi.

**Đánh giá:** 13 tài liệu hiện hành có 91 local links hợp lệ, 25 khối PowerShell parse được; CLI help download/EDA khớp scope/flags, không tạo JVM. Scope regression 3 test đạt. Tổng bytes sáu entry và checksum/137.269 records/4 Flume output trong history khớp manifest. Compose base/DataNodes/EDA/tools config hợp lệ. Graph được refresh sau hợp nhất.

**Giới hạn:** kiểm tra link là local, không xác nhận mọi nguồn online còn truy cập được; parse command không phải chạy mọi lệnh thao tác dữ liệu. Các lệnh tải/put/start/compact trong docs không được thực thi ở pha 3. Không chạy lại Spark fixture đã đạt pha 2 chỉ vì sửa prose. Raw/staging/spool/manifests tải/volumes không dọn, không train/deploy; số free space lịch sử không trình bày như hiện tại.

**Bước tiếp theo:** pha 4, rà kiểm thử bàn giao và các rủi ro còn thiếu theo code hiện tại. Chỉ bổ sung/chạy lại kiểm tra khi có lỗi hoặc khoảng trống cụ thể, tận dụng bằng chứng pha 1–2; chưa chuyển sang cleanup data trước khi selected raw được xác minh.

### Đợt 4 — kiểm thử bàn giao và xác minh selected HDFS, 14/10/2026

**Pha 4 đạt các kiểm tra phù hợp:** đã backup source/script/test/docs trước sửa ở `data/archive/before-phase4-20261014/`. Không lặp lại full adversarial EDA fixture đã đạt pha 2 vì implementation phân tích không đổi.

Khoảng trống tìm thấy và xử lý:

1. Flume skip trước đây chỉ dựa finalized manifest, prefix/count và SHA256 nguồn; bổ sung tổng byte HDFS khớp và suffix finalized .jsonl. Manifest size phải là integer dương trong Int64, checksum là chuỗi SHA256 đúng dạng. HDFS safemode OFF được kiểm tra trước inventory; missing/failed manifest, mismatch/extra input và source ngoài staging không được upload.
2. Wrapper cleanup trước đây có thể bỏ qua rm/nhả lock nếu lấy log bị lỗi. Tách stop/log/rm thành các bước cleanup độc lập và bảo đảm dispose lock/Pop-Location trong finally. Lỗi cleanup sau job thành công phải báo failed; lỗi job gốc không bị lỗi cleanup che mất. Chỉ xuất success object/thông báo complete sau cleanup đạt, không trả success sớm.
3. Smoke script trước đây cho truyền path tùy ý và collect không giới hạn. Giới hạn namespace hdfs://namenode:8020/project/testing/, từ chối traversal/encoded path trước JVM; collect tối đa 4 để phát hiện hơn 3 record. Kiểm tra notebook hiện hành 02, dùng Spark nhẹ và luôn stop.

| Kiểm tra | Kết quả và phạm vi |
| --- | --- |
| PowerShell offline fault cases | Đạt 11 tình huống ingest không hợp lệ trước write; preview, finalized Flume skip, batch success/failure/log error cleanup, lock release, concurrent Spark/host guard và out-of-scope rejection đạt |
| Runtime/mock trong Spark image | 8 test đạt, không tạo JVM; thêm smoke raw/traversal guard và excess record/limit(4) |
| Scope regression | 3 test đạt |
| HDFS live count/fsck | **6 selected datasets HEALTHY**, **9 file**, tổng **7.136.947.646 bytes** khớp manifest; không rehash/read payload raw |
| Put preview live | Chỉ skip sáu namespace selected, remaining 0; không -Apply, không upload/hash nguồn lớn |
| Spark smoke live | Đọc đúng **3 record/111 bytes** tại testing; notebook 02 hợp lệ, session dừng; container 2 GiB/CPU 1 |
| Wrapper với Docker thật sau sửa | Thay EDA bằng 8 test mock; exit/log/host samples và cleanup đạt, không JVM/raw |
| Legacy fsck | Block thiếu thuộc **/project/testing/legacy/test.txt (19 bytes)**, đã biết từ demo cũ; không sửa/xóa |
| Persistent mounts | NameNode và ba DataNode dùng đúng named volume/path hiện có; không recreate/format/restart để kiểm tra |

Báo cáo HDFS và summary tại `outputs/validation/phase4-20261014/`, được ignore Git. fsck/size xác minh metadata/block availability, không chứng minh SHA256 payload mới. Cluster report có 3 Live DataNode và 1 missing block legacy; **không gọi toàn cluster healthy**, chỉ xác nhận sáu selected datasets. Configured Capacity cộng qua các DataNode cùng host không là dung lượng ổ C vật lý.

Bằng chứng pha 1–3 được giữ cho downloader checksum/resume, full EDA metric/joins/temporal/5-core và guard vòng đời không đổi. Không chạy lại tải/hash 7 GB hoặc full EDA chỉ để tăng số lần test. Cú pháp/config/links và graph được kiểm tra/cập nhật sau các sửa trên.

**Giới hạn và chuyển pha:** chưa tạo processed/model, chưa đo raw EDA peak, chưa compact hoặc xóa datasets/spool/state/volumes. Pha 5 còn cần inventory từng target local/HDFS, hardlink/cache/spool trạng thái, nguồn phục hồi và kiểm chứng lại selected ngay trước cleanup. Legacy corrupt được tách riêng, không nằm trong danh sách tự động dọn ở pha này.

### Pha 5 — hoàn thành 14/10/2026

Đã thực hiện tuần tự inventory → fsck trước dọn → kiểm tra target/path/reparse/hardlink → xóa HDFS → xóa local → cập nhật manifest → dọn spool/report đã đối chiếu → fsck và đo dung lượng sau dọn. Không chạy raw EDA để kiểm tra cleanup.

| Hạng mục | Kết quả |
| --- | --- |
| Selected raw trước/sau | Sáu dataset HEALTHY, 9 file, 7.136.947.646 bytes; file list, bytes không đổi; không có file đang ghi. Video metadata giữ nguyên bốn Flume output |
| Ngoài scope | Dọn Sports/Toys/Health: sáu file local và sáu namespace HDFS, mỗi bộ 23.688.120.187 bytes logic. Sau xóa xác minh không còn target và dọn các parent rỗng bằng rmdir |
| Trash | fs.trash.interval = 0 đã kiểm tra trước xóa; dùng rm -r từng đường dẫn xác định, không wildcard/skipTrash/expunge. Đây là xóa vĩnh viễn đã có inventory và revision/checksum phục hồi |
| Source manifest | Bảo toàn nguyên bản 12 entry; bản hiện hành sáu entry, cập nhật đúng path hiện có; revision/checksum/status của nguồn giữ nguyên |
| Hardlink/cache | Mỗi JSONL staging và spool chỉ có một link. Cache chỉ ~1,7 MB metadata/log; không có payload/invalid/partial cần dọn, giữ lại |
| Spool | Xóa đúng bản COMPLETED 437.375.785 bytes sau SHA256 khớp manifest, channel queueSize 0 và Sink finalized; giữ .flumespool/channel/checkpoint |
| Reports | Giữ All Beauty/Video Games ở run_20261013T030858107175Z; dọn run cũ và phần Sports dang dở. Lưu run/failure và sáu file khác biệt của All Beauty cũ để giải thích sửa schema ảnh/coverage. Run tham chiếu vẫn failed, không đổi thành complete |
| Tổng spool/report dọn | 439.610.755 bytes; log kiểm thử nhỏ và code/notebook backups được giữ |
| Ổ C đo trước/sau | Free 196.325.085.184 → 220.442.812.416 bytes (182,84 → 205,30 GiB); tăng 24.117.727.232 bytes (~22,46 GiB), có thể gồm hoạt động nền ngoài cleanup |
| Linux dùng trước/sau | df Used 40.743.641.088 → 16.869.916.672 bytes; giảm 23.873.724.416 bytes (~22,23 GiB). Không cộng số này thành dung lượng ổ C đã thu hồi |
| Docker VHDX | Giữ 41.995.468.800 bytes (~39,11 GiB), chưa compact; không hứa tự thu nhỏ khi xóa file hoặc dừng container |
| Bảo toàn | NameNode, ba DataNode và tất cả named volumes/state giữ nguyên. Smoke và legacy test.txt giữ nguyên; missing block legacy vẫn 1, không gọi toàn cluster HEALTHY |

Biên bản tại `outputs/validation/phase5-20261014/`: summary.json, inventory.before.json, actions.jsonl, fsck trước/sau, số đo đĩa, manifest gốc và diagnostics. Tất cả được ignore Git. Fsck/size không thay cho SHA256 payload mới trên selected raw; SHA256 mới chỉ chạy trên bản spool cần xác nhận để dọn. Các báo cáo còn lại là dữ liệu lịch sử, chưa xác minh theo code EDA hiện hành.

**Bước tiếp theo:** pha 6 bàn giao/rà Git và giới hạn còn thiếu. Không tự chạy lại EDA ba ngành hàng, train hoặc compact trong pha 5.

### Pha 6 — bàn giao hoàn thành 14/10/2026

Đợt điều chỉnh phạm vi và dọn repo đã hoàn thành các pha 0–6. README/project-map dẫn đến các tài liệu chuẩn; history giữ kết quả theo ngày. Hồ sơ này được giữ để truy vết quyết định, không mở thêm một tài liệu bàn giao song song.

- Rà toàn bộ file tracked hiện có và untracked không bị ignore: không có payload data/output/cache hoặc credential theo các mẫu kiểm tra; fixture nhỏ và .gitkeep vẫn được giữ. Đây là kiểm tra mẫu credential, không cam kết phát hiện mọi loại secret. Git index rỗng; chưa stage/commit/push.
- Rà các Markdown hiện hành và kế hoạch học thuật ngoài Git root: local links hợp lệ; PowerShell scripts parse đạt. Không chạy các ví dụ download/put/compact trong tài liệu chỉ để kiểm thử prose.
- Notebook 01 đã sao lưu ở data/archive/before-phase6-20261014/notebooks/01_eda.ipynb, xóa output và execution_count của hai cell. Source, cell metadata và thứ tự cell giữ nguyên, đã so sánh trước/sau. Notebook 02 không có output. Không reset thay đổi code người dùng.
- Graph được refresh sau cập nhật hồ sơ bàn giao; chỉ số và kiểm tra source path nằm trong GRAPH_REPORT.md/biên bản. Bằng chứng test pha 1–4 và selected HDFS pha 5 được tái sử dụng vì không thay đổi logic phân tích/runtime trong pha 6.
- Biên bản rà soát local ở outputs/validation/phase6-20261014/, ignored cùng backups, dataset và kết quả EDA.

**Công việc phát triển sau đợt cleanup:** chạy EDA thật từng category để đo peak RAM/disk và lấy report theo code hiện hành, rồi mới chốt quy tắc processing/temporal split/threshold từ diagnostic gates. Report All Beauty/Video Games hiện là lịch sử; Industrial chưa có report hoàn tất. ETL processed, train/evaluate và API/web vẫn chưa triển khai, không thuộc các pha cleanup đã đóng. Máy một host/ba DataNode và fixture nhỏ chưa chứng minh khả năng mở rộng nhiều máy. Legacy testing còn một missing block riêng, selected raw healthy; compact VHDX là bảo trì thủ công khi cần.
