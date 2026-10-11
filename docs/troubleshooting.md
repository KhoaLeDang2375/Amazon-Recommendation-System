# Các lỗi thường gặp

## Không có pyspark

Kernel đang dùng có thể là Python trên host thay vì container. Kiểm tra `docker compose exec spark-notebook python -m pip show pyspark`, lấy server bằng `jupyter server list`, rồi kết nối VS Code tới server và chọn kernel Python của container.

## Jupyter hỏi token/password

Chạy `docker compose exec spark-notebook jupyter server list`. Mở URL với hostname `localhost` và port publish trong `.env`; dùng token của server đang chạy. Recreate có thể đổi token. Không commit token hoặc tắt authentication.

## PowerShell báo duplicated keys

JSON có thể chứa `Number Of Pieces` và `Number of Pieces`. Bộ chuyển đổi PowerShell mặc định coi chúng trùng. Script publish dùng validator tương thích trường hợp này; không dùng lệnh `ConvertFrom-Json` mặc định riêng để kết luận dữ liệu hỏng.

## Spark COLUMN_ALREADY_EXISTS

Bật `spark.conf.set("spark.sql.caseSensitive", "true")` trước `.json(...)`, sau đó đọc lại DataFrame. Khóa JSON lồng nhau cũng có thể khác nhau chỉ ở hoa/thường. Khi chọn cột có dấu chấm/ký tự đặc biệt, dùng tên được escape bằng backtick phù hợp.

## Không thấy đường dẫn cũ

Dataset thật đã chuyển từ `/project/raw/dataset/v1` sang `/project/raw/video_games_metadata/v1`. Smoke đã chuyển sang `/project/testing/raw/smoke/v1`. Đọc lại DataFrame sau khi sửa URI; object cũ vẫn tham chiếu đường dẫn cũ.

## Có .COMPLETED nhưng chưa đủ dữ liệu HDFS

Source và Sink hoạt động bất đồng bộ qua channel. Theo dõi log Flume, chờ channel drain và file `.tmp` được đóng; không publish lại ngay.

## File legacy thiếu block

File test cũ được giữ tại `/project/testing/legacy/test.txt`. Lỗi block của demo cũ là lịch sử; di chuyển namespace không khôi phục block bị thiếu. Không format HDFS hoặc xóa volume để xử lý file đó. Kiểm tra dataset thật bằng `fsck` trên đúng đường dẫn.
