# Ingestion

File tải xong trong `data/staging` → `Publish-File.ps1` → `data/spool` → Flume → HDFS raw. Script publish giữ nguyên file nguồn, kiểm tra JSON Lines, tính checksum và đưa file hoàn chỉnh vào spool bằng tên duy nhất.

Dataset thật: [video_games_metadata.v1.json](manifests/video_games_metadata.v1.json). Manifest là thông tin quản lý, không tự cấu hình Flume; routing được lấy từ `.env`. Template manifest dùng khi thêm dataset mới.

Các script: `init-hdfs.sh` tạo thư mục; `Publish-File.ps1` publish; `migrate-hdfs-layout.sh` chuyển đường dẫn lịch sử một lần, không cần chạy khi clone mới.

Fixture và kiểm thử nằm ở `tests/`. Đọc [quản lý dữ liệu](../docs/data-management.md) và [lệnh](../docs/commands.md) trước khi thay dataset/version.
