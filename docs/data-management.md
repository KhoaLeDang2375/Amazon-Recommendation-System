# Phạm vi, ingestion và quản lý dữ liệu

## Scope và nguồn bằng chứng

[experiment-scope.json](../ingestion/manifests/experiment-scope.json) là cấu hình phạm vi được commit. Python/PowerShell/CLI/notebook đọc cùng file; chỉ hỗ trợ **All_Beauty, Video_Games, Industrial_and_Scientific**, hai loại reviews/metadata. Scope không chứa trạng thái tải hoặc đường dẫn máy cá nhân.

Manifest tải ở data/staging/amazon-reviews-2023/download-manifest.json ghi revision, SHA256, bytes, local path và status. Sau pha 5 ngày 14/10/2026, bản hiện hành có sáu entry của ba category, đường dẫn local đã cập nhật. Bản 12 entry trước cleanup được giữ nguyên tại outputs/validation/phase5-20261014/download-manifest.before.json để bảo toàn lịch sử và khả năng tải lại đúng revision/checksum.

| Category | Reviews, bytes | Metadata, bytes | Tổng GB |
| --- | ---: | ---: | ---: |
| All_Beauty | 326.611.506 | 212.990.142 | 0,540 |
| Video_Games | 2.682.948.518 | 437.375.785 | 3,120 |
| Industrial_and_Scientific | 2.347.015.351 | 1.130.006.344 | 3,477 |
| **Tổng** | **5.356.575.375** | **1.780.372.271** | **7,137** |

Tổng **7.136.947.646 bytes (~7,14 GB / 6,65 GiB)** là JSONL không nén: đã đối chiếu Length local với manifest ngày 14/10/2026, không hash lại payload ở pha dọn docs. Manifest tải ghi revision **2b6d039ed471f2ba5fd2acb718bf33b0a7e5598e**. Khi chạy lại nguồn, dùng revision đã xác minh; mặc định downloader main sẽ resolve commit ở lần chạy mới.

[video_games_metadata.v1.json](../ingestion/manifests/video_games_metadata.v1.json) ghi nguồn metadata Video Games 437.375.785 bytes, 137.269 records, SHA256 và 4 output Flume finalized. URL/ngày tải ban đầu trong manifest này còn null; không suy ra ngày tải từ ngày kiểm chứng. Checksum trùng nguồn trong download manifest là bằng chứng đối chiếu bản file, không tự thay đổi provenance ingest.

## Vị trí và hợp đồng dữ liệu

| Vị trí | Nội dung và quy tắc |
| --- | --- |
| data/staging/ | Nguồn tải hoàn chỉnh, manifest tải, cache tải dở; không commit |
| data/spool/ | Batch Flume và Source tracking; không sửa khi agent đang đọc |
| data/archive/ | Backup local có chủ đích; không phải nguồn ingest tự động |
| tests/fixtures/ | Fixture nhỏ được commit, có nhãn testing |
| /project/raw/<category_lower>_{reviews,metadata}/v1 | Sáu dataset thật trong scope |
| /project/processed/silver/<category>/v1/<run_id> | Đã có Parquet reviews_clean/catalog/quarantine/duplicate audit cho ba category; chỉ dùng run có manifest complete, xem [Silver](silver.md) |
| /project/processed/gold/... | Split/features/labels dự kiến, chưa triển khai |
| /project/analytics/... | Tổng hợp HDFS về sau |
| /project/testing/... | Smoke và legacy, không lẫn raw thật |
| outputs/eda/, artifacts/ | Aggregate/report/model local có version; không commit payload |

Giữ nguyên raw và case-sensitive JSON. Đọc JSON bằng Spark phải bật spark.sql.caseSensitive=true. EDA kiểm tra native type/field presence trước chuẩn hóa; missing field khác null. Chuẩn hóa ID/timestamp, xử lý duplicates/conflicts và impute là quyết định processing có báo cáo tác động.

Manifest ingest/snapshot về sau cần source revision/URL, category/kind, input fingerprint, bytes/records, batch ID, ingested_at, status, target path và schema/policy version. Đối soát input/accepted/quarantine/duplicates với nhóm loại trừ rõ ràng; không cộng các nhóm lỗi chồng lấn như partition của toàn bộ dữ liệu.

## Tải trên PowerShell host

Từ Git root project/, dùng Python 3.10+ và môi trường riêng, không cài dependency downloader vào Spark:

~~~powershell
py -3 -m venv .tools/hf-download
./.tools/hf-download/Scripts/python.exe -m pip install -r ingestion/requirements-download.txt
./.tools/hf-download/Scripts/python.exe ingestion/scripts/download_amazon_hf.py --dry-run
~~~

Dry-run gọi metadata nguồn và **hash các file local có sẵn**; có thể tốn I/O nhiều GB dù không tải bytes qua mạng. Nó không đọc HDFS. Khi thực sự cần tải:

~~~powershell
./.tools/hf-download/Scripts/python.exe ingestion/scripts/download_amazon_hf.py --ask-token --revision 2b6d039ed471f2ba5fd2acb718bf33b0a7e5598e
# Hoặc chỉ một category và một loại dữ liệu:
./.tools/hf-download/Scripts/python.exe ingestion/scripts/download_amazon_hf.py --categories All_Beauty --kind metadata --revision 2b6d039ed471f2ba5fd2acb718bf33b0a7e5598e
~~~

Mặc định ba category/sáu file, một worker. Không cần tải lại file đã đủ bằng chứng. Token nhập ẩn bằng --ask-token hoặc lấy từ HF_TOKEN; Enter thử public không token. Script không tự đọc .env. Không đưa token vào code/history/Git; token không bảo đảm tốc độ.

- Chỉ tìm đúng filename trong thư mục đích và staging, không quét archive/spool/HDFS.
- Skip khi size và SHA256 khớp metadata nguồn; file sai giữ nguyên và dừng, không overwrite.
- Ghi revision pin và status downloaded_verified/skipped_verified/failed; retry mạng có giới hạn, mặc định ba attempts.
- .hf-downloads giữ phần tải dở. Khi thành công, tạo hardlink cùng filesystem tới đích rồi unlink file tạm; không tạo một bản copy hoàn chỉnh thứ hai. NTFS/ext4 cần cho phép hardlink.
- .download.lock chặn lượt tải đồng thời; chỉ xử lý lock cũ sau khi xác nhận process đã dừng.
- Dự phòng tối thiểu bytes còn thiếu + 1 GiB là kiểm tra tải, chưa bao gồm toàn bộ ngân sách EDA/HDFS.
- --workers/--high-performance chỉ tăng khi tài nguyên cho phép, không là mặc định của máy hiện tại.

Nguồn tải: [Amazon Reviews 2023 trên Hugging Face](https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023). Tải raw JSONL, không chạy dataset loading script hay tự chuyển Parquet.

## Nhập HDFS batch — PowerShell host

HDFS phải đang sẵn sàng; xem [vận hành](operations.md). Preview chỉ lập kế hoạch, không ghi HDFS; nó vẫn hash nguồn local khi cần nhập file còn thiếu:

~~~powershell
& ./ingestion/scripts/Put-AmazonRaw.ps1
# Giới hạn preview một category:
& ./ingestion/scripts/Put-AmazonRaw.ps1 -Categories All_Beauty
# Chỉ apply sau khi đã xem đúng source/target:
& ./ingestion/scripts/Put-AmazonRaw.ps1 -Categories All_Beauty -Apply
~~~

Script dùng download manifest, kiểm kê đúng namespace được chọn, chỉ put file thiếu với replication 1; không overwrite hoặc xóa file dở. PLAN là dự kiến; OK sau Apply xác nhận write và đối chiếu size.

File HDFS cùng tên/size được skip, **không có SHA256 HDFS mới**. Giữ bằng chứng ingest/checksum sẵn có; nếu thiếu provenance hoặc nguồn thay đổi phải xác minh trước khi xem skip là xác nhận nội dung. Dataset chứa file khác bị chặn; riêng metadata Video Games giữ 4 output Flume theo manifest finalized, checksum nguồn, tổng byte khớp và không có file chưa đóng, không put thêm nguyên file. Manifest size phải là số nguyên dương trong Int64 và checksum chuỗi SHA256 hợp lệ; HDFS phải safemode OFF trước kiểm kê.

Source local phải trong staging, size/SHA256 khớp. Nếu manifest path cũ mất, tìm đúng filename tại hai vị trí staging đã định; không quét thư mục khác. Không tự thay replication của dữ liệu đã có, tạo processed, hay cập nhật trạng thái manifest Flume.

Kiểm chứng HDFS sau ingest bằng fsck trên **đúng dataset** và đối soát manifest; -count cho bytes/files/directories, không phải số records. Lệnh mẫu ở [operations.md](operations.md). Smoke chỉ ở /project/testing.

## Flume — batch mới có chủ đích

Publish giữ file nguồn, kiểm tra UTF-8/JSON Lines, SHA256, copy qua tên tạm rồi rename vào spool; mỗi lượt có batch ID mới. Log/text cũng được nhận nhưng không nhất thiết là JSON. ZIP/Parquet/media hoặc định dạng cần giữ bytes dùng direct put.

Routing Flume lấy DATASET_NAME/DATASET_VERSION từ .env, **không tự lấy từ scope JSON**. Chỉ publish khi batch mới và target đã được chọn; không dùng file metadata Video Games đã ingest làm ví dụ chạy lại.

~~~powershell
# new-batch.jsonl là input mới do người vận hành chuẩn bị trong staging.
# Chỉ chạy khi routing .env trỏ đúng dataset dành cho batch này.
& ./ingestion/scripts/Publish-File.ps1 -File ./data/staging/new-batch.jsonl
docker compose logs --tail 100 flume
~~~

.COMPLETED chỉ là Source đã đọc. Chờ channel rỗng, Sink đóng .tmp và kiểm chứng HDFS trước xác nhận finalized. Không xóa .flumespool/checkpoint/File Channel hoặc republish để sửa đường dẫn.

Đổi routing: chờ batch cũ finalize/drain → dừng Flume → archive file completed đã xác nhận ngoài spool, giữ tracking → sửa .env → recreate hdfs-init/flume có chủ đích. Không chạy migration lịch sử trên clone mới. File Channel có thể replay; pipeline không cam kết exactly-once. Flume config maxLineLength 1 MiB áp dụng nhánh Flume, không phải giới hạn chung cho direct put.

## Retention và dọn dữ liệu

Giữ sáu raw dataset trong scope, sáu JSONL local phục hồi, manifests, named volumes và Flume state. Pha 5 ngày 14/10/2026 đã dọn sáu file staging và sáu namespace HDFS của Sports/Toys/Health sau kiểm kê; selected raw HEALTHY trước/sau, bytes và file list không đổi. Bản spool Video metadata COMPLETED được dọn sau kiểm tra checksum, channel rỗng và Sink finalized; .flumespool/channel/checkpoint vẫn giữ. Cache chỉ có metadata/log (~1,7 MB), không có JSONL dở hoặc bản payload thứ hai cần dọn.

Bộ report tham chiếu giữ tại outputs/eda/run_20261013T030858107175Z/{All_Beauty,Video_Games}. Run vẫn mang status failed vì lỗi ở category sau đó; không coi là EDA hoàn tất toàn scope hoặc theo code hiện tại. Đã dọn run cũ trùng và phần Sports dang dở; metadata/lỗi và sáu file khác biệt của report All Beauty cũ nằm trong biên bản pha 5. Không xóa các log kiểm thử nhỏ để thu hồi dung lượng không đáng kể.

Trước cleanup, lập danh sách đường dẫn thật/bytes/link status, kiểm chứng selected raw healthy/finalized và nguồn phục hồi. Dọn namespace qua HDFS, không xóa block trong volume DataNode. Trash có thể chưa trả dung lượng; bypass Trash là xóa vĩnh viễn có chủ đích, không phải mặc định.

Một bản local phục hồi cộng HDFS làm tăng dung lượng logic; spool/cache/processed/shuffle/VHDX cần ngân sách riêng. Cache có hardlink phải tính theo file identity/link count, không cộng mỗi path như một bản vật lý. Không giảm từ ba DataNode xuống một khi chưa decommission/migrate block.

Run hoàn tất giữ aggregate/config/report; scratch mới đã bỏ khỏi đường chạy. Log và output lỗi cần thời hạn lưu; không xóa run đang chạy. Hash trace/ID là pseudonymous, không phải bảo đảm ẩn danh để chia sẻ. Backup notebook trước khi clear output và không đưa raw/model/token vào Git.

Xóa file Linux không tự thu nhỏ VHDX Windows; [công cụ bảo trì](operations.md) chỉ chạy thủ công khi cần. Checklist cleanup cụ thể nằm trong [kế hoạch dọn repo](KE_HOACH_DON_DEP_REPO.md).
