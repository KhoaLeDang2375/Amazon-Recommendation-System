# Quản lý dữ liệu

Dataset thật được mô tả trong `ingestion/manifests/video_games_metadata.v1.json`: checksum file nguồn, byte và số dòng. Chưa biết chính xác URL nguồn/ngày tải nên để `null`; bổ sung từ nguồn bạn đã tải, không suy đoán.

| Vị trí | Nội dung |
|---|---|
| `data/staging` | File tải về hoàn chỉnh |
| `data/spool` | Batch Flume và checkpoint; không sửa khi agent đang đọc |
| `data/archive` | Smoke cũ và backup notebook, chỉ local |
| `tests/fixtures` | Ví dụ nhỏ được commit |
| `/project/raw/video_games_metadata/v1` | Dataset thật đã ingest |
| `/project/processed/video_games_metadata/v1` | Dữ liệu làm sạch về sau |
| `/project/analytics/video_games_metadata/v1` | Tổng hợp về sau |
| `/project/testing` | Smoke và legacy, tách khỏi dữ liệu thật |

Chỉ xác nhận batch xong khi Source hoàn tất, channel rỗng và file HDFS đã đóng. Không xóa `.flumespool`, state volume hoặc publish lại để sửa đường dẫn; dùng di chuyển HDFS sau khi channel drain.

Khi đổi dataset: chờ batch cũ xong → stop Flume → archive file đã hoàn tất ra ngoài spool, giữ checkpoint → sửa `.env` → `docker compose up -d --force-recreate hdfs-init flume`. Tên dataset/version dùng chữ, số, `_`, `-`, bắt đầu bằng chữ/số. Recreate notebook nếu environment đổi và chạy lại kernel/setup.

Flume nhận UTF-8 JSON Lines/log, một record mỗi dòng, tối đa 1 MiB. Parquet, ZIP, ảnh hoặc dữ liệu cần giữ byte nguyên gốc dùng `hdfs dfs -put`. Log được sink ghi đuôi `.jsonl` nhưng vẫn phải đọc bằng `spark.read.text` nếu nội dung không phải JSON.

JSON thật có khóa khác nhau chỉ ở chữ hoa/thường. Giữ raw nguyên nghĩa, bật `spark.sql.caseSensitive=true` khi đọc. Chuẩn hóa tên và xử lý trùng nên làm ở bước processed, với quy tắc ghi rõ trong code.

Artifacts hiện trống. Khi có kết quả, ghi version dataset/model, schema, checksum và cách tiền xử lý cùng output. File lớn/model không commit; dùng storage hoặc release phù hợp. Notebook giữ code, loại output trước khi chia sẻ và kiểm tra không có token trong code.
