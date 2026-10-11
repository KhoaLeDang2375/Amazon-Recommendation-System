# Các lệnh và ý nghĩa

Chạy tại thư mục gốc bằng **PowerShell trên host**, trừ phần Python notebook. `docker compose exec` thực thi phần lệnh phía sau trong container đang chạy.

| Lệnh | Ý nghĩa |
|---|---|
| `docker compose config --quiet` | Kiểm tra Compose và biến môi trường; không khởi chạy |
| `docker compose up -d --build` | Build khi cần, tạo/khởi động stack mặc định ở nền |
| `docker compose --profile eda up -d --build spark-notebook` | Khởi chạy notebook tùy chọn và dependencies |
| `docker compose ps -a` | Xem cả service đang chạy và init đã kết thúc |
| `docker compose logs --tail 100 flume` | 100 dòng log cuối |
| `docker compose logs -f flume` | Theo dõi log; Ctrl+C dừng theo dõi |
| `docker compose exec spark-notebook jupyter server list` | Lấy URL/token của server hiện tại |
| `docker compose --profile eda stop spark-notebook` | Dừng notebook, giữ container và dữ liệu |
| `docker compose restart flume` | Restart cấu hình container hiện có |
| `docker compose --profile eda down` | Gỡ container/network, giữ named volumes |
| `docker volume ls` | Xem volume hiện có |

`restart` không cập nhật environment từ `.env`; dùng `up -d --force-recreate <service>` khi đổi environment. `down -v` xóa named volumes; không dùng cho dừng thường ngày. Init có `Exited (0)` là đã hoàn tất.

## Publish

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\ingestion\scripts\Publish-File.ps1 -File .\data\staging\meta_Video_Games.jsonl
```

`-NoProfile` bỏ profile cá nhân. `-ExecutionPolicy Bypass` áp dụng cho process PowerShell này. `-File` đầu chọn script; `-File` sau là tham số file dữ liệu của script. Script kiểm tra đầu vào, tính SHA256, copy qua tên tạm và đổi tên để Flume chỉ thấy file hoàn chỉnh. File gốc vẫn còn; chạy lại có thể tạo dữ liệu trùng.

## HDFS

| Lệnh | Ý nghĩa |
|---|---|
| `docker compose exec namenode hdfs dfs -ls -R /project/raw` | Liệt kê đệ quy dữ liệu raw |
| `docker compose exec namenode hdfs dfs -count /project/raw/video_games_metadata/v1` | Số thư mục, số file, tổng byte; không phải số bản ghi |
| `docker compose exec namenode hdfs dfsadmin -report` | Trạng thái DataNode và dung lượng |
| `docker compose exec namenode hdfs fsck /project/raw/video_games_metadata/v1` | Kiểm tra block; không sửa/xóa dữ liệu |
| `docker compose exec namenode hdfs dfs -mkdir -p /project/raw/video_games_metadata/v1/originals` | Tạo thư mục nếu chưa có |
| `docker compose exec namenode hdfs dfs -put /ingestion/staging/file.parquet /project/raw/video_games_metadata/v1/originals/` | Copy nguyên file từ staging mount vào HDFS |

`-put` dùng đường dẫn nhìn từ container. `hdfs dfs -cat <file-nhỏ>` xem nội dung; tránh in toàn bộ dataset ra terminal. `migrate-hdfs-layout.sh` là script chuyển dữ liệu lịch sử trên máy hiện tại, không thuộc quy trình clone mới.

## Python trong notebook

```python
spark.conf.set("spark.sql.caseSensitive", "true")
path = "hdfs://namenode:8020/project/raw/video_games_metadata/v1"
df = spark.read.option("recursiveFileLookup", "true").json(path)
df.printSchema()
df.show(5, truncate=80)
```

`recursiveFileLookup` đọc các thư mục batch bên dưới. JSON Lines dùng mặc định mỗi dòng một record; không bật `multiLine`. Schema inference có thể đọc dữ liệu để suy luận. `show` kích hoạt job; `count()` quét để đếm record. `collect()`/`toPandas()` đưa dữ liệu về RAM driver, chỉ dùng sau khi giới hạn/tổng hợp phù hợp.

## Graphify

```powershell
docker compose --profile tools build graphify
powershell -NoProfile -ExecutionPolicy Bypass -File .\tools\graphify\Refresh-Graph.ps1
docker compose --profile tools run --rm --no-deps graphify query "Flume ingestion" --budget 1200
```

`run --rm` chạy container công cụ rồi gỡ container đó; graph vẫn nằm trong bind mount `graphify-out`. `--budget` đặt mục tiêu dung lượng ngữ cảnh query, không phải trần cứng: phiên bản hiện tại có thể trả nhiều hơn để giữ đủ cạnh liên kết. Không phải giới hạn chi phí hội thoại. Dùng câu hỏi cụ thể hoặc `graphify explain <tên-node>` để thu hẹp. Refresh dùng AST, cấu hình và heading tài liệu, không gọi LLM.
