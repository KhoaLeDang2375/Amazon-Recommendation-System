# Chuyển cấu trúc ngày 2026-10-11

Giữ project và named volumes hiện tại. Sau khi Flume channel rỗng và các file đóng, dùng `hdfs dfs -mv` chuyển `/project/{raw,processed,analytics}/dataset` sang `video_games_metadata`; chuyển smoke sang `/project/testing/{raw,processed,analytics}/smoke`.

Dataset nguồn: `meta_Video_Games.jsonl`, 137269 dòng, 437375785 byte. Bốn file HDFS raw đã finalize được giữ nguyên; không publish lại. Checksum nguồn nằm trong manifest. URL/ngày tải cần người tải bổ sung.

Fixture smoke chuyển vào `tests/fixtures/ingestion`; các bản staging/spool smoke cũ vào `data/archive/smoke`. Script kiểm thử chuyển vào `tests/integration`. README demo cũ và validation cũ được giữ ở thư mục history; đường dẫn trong chúng phản ánh trạng thái lịch sử.

Notebook được backup tại `data/archive/notebooks/01_eda.before-layout.ipynb`, giữ code, bỏ output/execution count, đổi default dataset và bật caseSensitive khi tạo Spark.

`/data/test.txt` cũ chuyển vào `/project/testing/legacy/test.txt`; block thiếu từ demo cũ không được khôi phục bằng việc di chuyển. Fixture local được giữ ở `tests/fixtures/legacy/test.txt`. Không format hoặc xóa volume.
