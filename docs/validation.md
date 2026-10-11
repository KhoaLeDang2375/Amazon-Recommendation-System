# Xác nhận sau khi sắp xếp

Kiểm tra ngày 2026-10-11:

- `docker compose config --quiet`: đạt.
- HDFS dataset thật: 4 file, 437375785 byte, 7 block; `fsck` HEALTHY, không thiếu/hỏng block.
- SHA256 của nội dung nối các file HDFS bằng checksum nguồn: `3ad94dfaa8dfed23c5f420aa9ddec415d00f2b8ec9f2d796f487b4b2c4d256de`.
- Nguồn JSON Lines: 137269 dòng, ghi trong manifest.
- Smoke Spark tại `/project/testing/raw/smoke/v1`: đúng 3 record; notebook format và cú pháp đạt.
- Graphify refresh và query: đạt; generated graph/report/HTML đã tạo. Query budget là mục tiêu, có thể vượt để giữ liên kết.
- `.env`, dataset nguồn, backup archive và nội dung model được Git ignore.
- NameNode, DataNode, Flume và Spark notebook chạy; init kết thúc mã 0. Graphify chạy one-shot khi cần.

File legacy thiếu block vẫn được giữ riêng; kết quả kiểm tra dataset thật không bao gồm việc phục hồi file legacy. Validation demo ban đầu nằm ở `history/initial-validation.md`.
