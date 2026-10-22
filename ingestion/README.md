# Ingestion

Scope chuẩn: [experiment-scope.json](manifests/experiment-scope.json), ba ngành hàng và reviews/metadata. Hướng dẫn tải, direct put, Flume, manifests và retention dùng chung ở [quản lý dữ liệu](../docs/data-management.md); cách khởi động HDFS ở [vận hành](../docs/operations.md).

download_amazon_hf.py chỉ tải staging; Put-AmazonRaw.ps1 mặc định preview, cần -Apply để ghi HDFS. Publish-File.ps1 dành cho batch Flume mới với routing đã chốt. init-hdfs.sh chuẩn bị thư mục; migrate-hdfs-layout.sh chỉ là migration lịch sử, không chạy trên clone mới. Manifest [Video Games metadata](manifests/video_games_metadata.v1.json) giữ bằng chứng ingest Flume đã finalized.

Fixture và kiểm thử ở [tests](../tests/README.md); không dùng dataset thật để smoke.
