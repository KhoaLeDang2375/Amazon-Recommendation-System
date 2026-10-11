# Tải Amazon Reviews 2023 qua Hugging Face

Script `ingestion/scripts/download_amazon_hf.py` mặc định tải cả reviews và metadata của 6 category: `All_Beauty`, `Video_Games`, `Sports_and_Outdoors`, `Toys_and_Games`, `Industrial_and_Scientific`, `Health_and_Personal_Care` (12 file JSONL).

Nguồn: [McAuley-Lab/Amazon-Reviews-2023](https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/tree/main/raw). Tải trực tiếp raw JSONL, không chạy dataset loading script hoặc chuyển đổi sang Parquet. Token dùng để xác thực, không bảo đảm nhanh hơn; tốc độ tùy mạng, nguồn và ổ đĩa. [Hub hỗ trợ tải](https://huggingface.co/docs/huggingface_hub/guides/download); [Xet high performance](https://huggingface.co/docs/huggingface_hub/package_reference/environment_variables#hfxethighperformance) tăng sử dụng CPU/mạng, phù hợp SSD.

## Chạy trên host PowerShell

Cần Python 3.10 trở lên. Từ thư mục gốc repository, tạo môi trường riêng cho downloader; không cài vào Python của Spark:

```powershell
py -3 -m venv .tools/hf-download
.\.tools\hf-download\Scripts\python.exe -m pip install -r ingestion/requirements-download.txt
```

Kiểm tra trước khi tải (chỉ gọi metadata nguồn và hash các file đích có sẵn):

```powershell
.\.tools\hf-download\Scripts\python.exe ingestion/scripts/download_amazon_hf.py --dry-run
```

Tải tự động:

```powershell
.\.tools\hf-download\Scripts\python.exe ingestion/scripts/download_amazon_hf.py --ask-token
```

Nhập token đọc mới tại prompt ẩn; Enter để thử tải public không token. Không ghi token vào code, notebook, command history hoặc Git. Script cũng nhận `HF_TOKEN` đã cấu hình trong môi trường, không tự đọc `.env`. Nếu token từng được gửi trong chat, hãy thu hồi và tạo token mới trước khi dùng.

Tùy chọn (host PowerShell):

```powershell
# SSD và mạng tốt; có thể dùng nhiều CPU hơn
.\.tools\hf-download\Scripts\python.exe ingestion/scripts/download_amazon_hf.py --workers 3 --high-performance --ask-token
# Chỉ một category và metadata
.\.tools\hf-download\Scripts\python.exe ingestion/scripts/download_amazon_hf.py --categories Video_Games --kind metadata
```

## Kiểm tra đã có dữ liệu và tải tiếp

- Đối chiếu đúng tên tại `data/staging/amazon-reviews-2023` và `data/staging`, không quét archive, spool hoặc HDFS. File `.gz`, tên khác hoặc dữ liệu đã chia batch trên HDFS không được xem là bản JSONL nguồn hoàn chỉnh.
- Bỏ qua khi kích thước và SHA-256 khớp metadata LFS nguồn, kể cả file tải thủ công chưa có manifest. Hash file lớn có thể mất thời gian nhưng không tải lại qua mạng.
- File đích tồn tại nhưng sai kích thước/checksum: dừng trước khi tải; giữ nguyên file. Kiểm tra thủ công, chuyển file đó ra khỏi vị trí đích nếu muốn tải bản mới.
- Pin commit nguồn trong mỗi lượt tải, ghi revision và checksum vào `download-manifest.json` dưới thư mục đích. Có thể dùng `--revision <commit>` để chạy lại đúng bản.
- File đang tải và metadata tiếp tục tải nằm ở `.hf-downloads`; giữ thư mục này để Hub có thể dùng lại phần tải dở. Không cam kết mọi lỗi Xet đều tiếp tục từ đúng byte cuối.
- Chỉ đưa file đã kiểm tra SHA-256 ra tên đích bằng hard link cùng ổ đĩa, không ghi đè và không tạo bản sao lớn thứ hai. Cần filesystem hỗ trợ hard link (NTFS/ext4); lỗi tạo link giữ file trong `.hf-downloads`.
- Script thử lại tối đa 3 lần mỗi thao tác mạng, mặc định 3 file song song. File lỗi được ghi trong manifest; chạy lại bỏ qua file đã xác minh. Mã thoát `0` là thành công; `1` có lỗi.
- Lock ngăn hai lượt chạy cùng thư mục đích. Khi tiến trình bị kill, chỉ xóa `.download.lock` sau khi chắc chắn tiến trình cũ đã dừng.
- Kiểm tra dung lượng trống cho toàn bộ file còn thiếu cộng 1 GiB dự phòng. Cache/file lỗi có thể cần thêm dung lượng; `--dry-run` hiển thị tổng byte nguồn cần tải.

File hoàn chỉnh chỉ nằm trong staging. Không tự publish Flume hoặc cập nhật manifest HDFS hiện có. Việc đổi category/routing cần drain Flume và chuẩn bị đường dẫn dataset phù hợp theo [quản lý dữ liệu](data-management.md).

## Nhập phần còn thiếu vào HDFS raw

Chạy trên host PowerShell tại gốc repository; HDFS phải đang hoạt động:

```powershell
# Xem kế hoạch; kiểm tra SHA-256 nguồn trước khi put
powershell -NoProfile -ExecutionPolicy Bypass -File ingestion/scripts/Put-AmazonRaw.ps1
# Nhập các file còn thiếu, replication 1
powershell -NoProfile -ExecutionPolicy Bypass -File ingestion/scripts/Put-AmazonRaw.ps1 -Apply
docker compose exec -T namenode hdfs fsck /project/raw
docker compose exec -T namenode hdfs dfs -ls -R /project/raw
```

Script đọc download manifest, tách `/project/raw/<category>_reviews/v1` và `<category>_metadata/v1`, không put cache/manifest. File đích cùng tên và kích thước được bỏ qua (không kiểm tra SHA-256 HDFS); dataset có file khác sẽ bị chặn để tránh nhập trùng. Riêng metadata Video_Games đã có manifest finalized và 4 file Flume đúng dạng sẽ được giữ nguyên. Kiểm tra `fsck` để xác nhận block; không dùng `-f`, không tự xóa file dở hoặc thay routing Flume. Không đổi replication của dữ liệu đã tồn tại và không tạo processed/analytics khi chưa có kết quả.

Nếu đường dẫn nguồn trong manifest không còn tồn tại, script tìm đúng tên file tại `data/staging/amazon-reviews-2023` rồi `data/staging`, vẫn kiểm tra kích thước và SHA-256 trước khi nhập. Không quét archive/spool. Các dòng `PLAN` chỉ là kế hoạch; chỉ dòng `OK` sau `-Apply` xác nhận đã put và kiểm tra kích thước thành công.

## Kiểm thử offline (host PowerShell)

```powershell
py -3 -m unittest discover -s tests -p test_download_amazon_hf.py
```

Fixture tạm nằm trong `tests`, kiểm tra bỏ qua file hợp lệ, phát hiện checksum sai cùng kích thước, phân biệt file tải dở và chọn đúng 12 file.
