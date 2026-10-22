# Vận hành, xử lý lỗi và bảo trì

## Quy ước lệnh và bảo toàn

Các khối PowerShell chạy trên **host tại Git root project/**. Phần sau docker compose exec chạy trong container; URI hdfs://namenode:8020 dùng trong mạng Compose. Python notebook được ghi nhãn riêng. Script -File tương đối cần đúng thư mục; có thể truyền đường dẫn tuyệt đối nếu đang ở nơi khác.

Giữ project hadoop-demo và các volume NameNode, datanode1/2/3, flume_state. Máy hiện có dùng ba DataNode; block replication 1 có thể nằm trên cả ba. Không format, đổi volume, xóa block, prune volumes hoặc dùng down -v để sửa lỗi. Không dùng remove-orphans cho các DataNode chỉ vì chạy lệnh với thiếu overlay.

## Kiểm tra và khởi động HDFS

PowerShell host:

~~~powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
docker compose -f compose.yaml -f compose.datanodes.yaml config --quiet
docker compose -f compose.yaml -f compose.datanodes.yaml ps -a
docker volume ls
~~~

Máy đã có container/volume: dùng start để khởi động lại, không recreate:

~~~powershell
docker compose -f compose.yaml -f compose.datanodes.yaml start namenode datanode1 datanode2 datanode3
docker compose exec -T namenode hdfs dfsadmin -safemode get
docker compose exec -T namenode hdfs dfsadmin -report
~~~

Chờ safemode OFF và đủ DataNode đang chứa dữ liệu. Init Exited (0) là hoàn tất, không phải lỗi service. Nếu một node thiếu, kiểm tra mount/log/node trước job; không tự bỏ node khỏi cluster.

Trên **máy clone mới chưa có dữ liệu**, tải/build phần mềm và tạo các service cần thiết:

~~~powershell
docker compose --profile eda build spark-notebook
docker compose -f compose.yaml -f compose.datanodes.yaml up -d --no-recreate namenode datanode1 datanode2 datanode3
~~~

NameNode bootstrap trong code chỉ format khi không có VERSION. Với volume đã từng chứa dữ liệu mà thiếu VERSION, dừng và kiểm tra volume/metadata trước khi khởi động; không coi đó là clone mới. Git clone không mang theo dataset hoặc volume. Không dựng bản clone thứ hai trên cùng host dùng chung các volume đang hoạt động.

Sau đó chạy fixture theo [tests/README.md](../tests/README.md), tải/put theo [data-management.md](data-management.md). Không chạy migrate-hdfs-layout.sh hoặc publish lại dữ liệu lịch sử trên clone mới.

## Kiểm chứng filesystem — PowerShell host

Chọn đúng namespace cần kiểm tra; không in payload GB ra terminal:

~~~powershell
docker compose exec -T namenode hdfs dfs -ls -R /project/raw/video_games_metadata/v1
docker compose exec -T namenode hdfs dfs -count /project/raw/video_games_metadata/v1
docker compose exec -T namenode hdfs fsck /project/raw/video_games_metadata/v1
~~~

- -ls liệt kê file; -count trả directories/files/bytes, **không phải records**.
- fsck ở đây kiểm tra block, không kèm tùy chọn xóa/sửa. HEALTHY không tự xác nhận schema, số dòng hoặc checksum nguồn.
- -put đọc path nhìn từ container; staging host được mount tại /ingestion/staging. Dùng script put có manifest thay lệnh overwrite thủ công.
- .tmp hoặc ._COPYING_ là dấu vết input chưa hoàn tất; không xóa/bỏ qua rồi gọi dataset complete.

## Spark batch và notebook

Batch dùng [tools/eda/Run-Eda.ps1](../tools/eda/Run-Eda.ps1), một category/process, khóa độc quyền, image có sẵn, HDFS sẵn sàng và dự phòng đĩa host. Không bật Jupyter cùng batch. Wrapper thu exit code/log rồi dọn đúng container; Ctrl+C đi qua finally. Không có daemon EDA cần chạy liên tục.

Notebook tùy chọn, với HDFS đã chạy:

~~~powershell
docker compose -f compose.yaml -f compose.datanodes.yaml -f compose.eda-light.yaml --profile eda up -d --no-deps spark-notebook
docker compose exec -T spark-notebook jupyter server list
~~~

Mở URL localhost với JUPYTER_PORT và token vừa in; không đưa token lên Git hoặc tắt authentication. Notebook 02 dùng run_eda và dừng session sở hữu khi hoàn tất. REPORT_DIRECTORY cho xem report cũ mà không tạo Spark. Runtime, flags và insight được mô tả duy nhất ở [eda.md](eda.md).

Dừng notebook khi không dùng; dừng HDFS sau khi job và ingestion đã kết thúc:

~~~powershell
docker compose --profile eda stop spark-notebook
docker compose -f compose.yaml -f compose.datanodes.yaml stop namenode datanode1 datanode2 datanode3
~~~

Dùng stop cho dừng thường ngày; container/volume vẫn còn. down không xóa named volumes nếu không có -v, nhưng không cần gỡ cả stack để thu RAM. Restart không cập nhật environment .env; recreate chỉ dùng khi có thay đổi cấu hình cần áp dụng và đã bảo toàn state.

## Flume và thay routing

Flume bật khi có batch mới đã chốt đích; build image riêng nếu chưa có:

~~~powershell
docker compose build flume
docker compose -f compose.yaml -f compose.datanodes.yaml up -d flume
docker compose logs --tail 100 flume
~~~

Publish/manifest/finalize theo [data-management.md](data-management.md). Khi đổi DATASET_NAME/DATASET_VERSION, chờ source hoàn tất, channel drain và Sink đóng file trước stop; giữ checkpoint/tracking. Sau khi sửa .env, recreate hdfs-init/flume có chủ đích để cập nhật environment. Không dùng restart rồi tưởng routing đã đổi, không republish để chữa path.

## DataNode bổ sung

compose.datanodes.yaml khai báo datanode2/3 với volume riêng. Node đầu vẫn dùng volume hadoop-demo_datanode_data; không đổi thành tên mới chỉ vì service là datanode1.

~~~powershell
docker compose -f compose.yaml -f compose.datanodes.yaml config --services
docker compose -f compose.yaml -f compose.datanodes.yaml up -d --no-recreate datanode2 datanode3
docker compose exec -T namenode hdfs dfsadmin -report
~~~

Lệnh trên dùng cho node đã khai báo, giữ service/volume cũ; không phải lệnh scale compute Spark. Nếu thêm node khác, cần hostname/volume mới và init quyền riêng. Không gắn volume DataNode đang hoạt động vào init sửa quyền hàng loạt. --no-recreate không áp dụng thay đổi init cũ; init cho volume mới phải được xem xét riêng.

## Xử lý lỗi

| Triệu chứng | Kiểm tra và xử lý |
| --- | --- |
| -File không tồn tại từ system32 | cd về project trước lệnh tương đối hoặc dùng absolute path; không chạy cd sau lệnh đã lỗi |
| Không có pyspark | Kernel có thể là Python host; chọn Jupyter container, kiểm tra pip show pyspark trong container |
| Jupyter hỏi token/password | Lấy server list hiện tại, đúng localhost/port; recreate có thể đổi token |
| COLUMN_ALREADY_EXISTS | Bật caseSensitive trước đọc; khóa lồng nhau có thể khác hoa/thường; escape tên cột đặc biệt |
| ConvertFrom-Json báo duplicated keys | PowerShell mặc định có thể gộp key khác hoa/thường; Publish validator xử lý tương thích, không kết luận raw hỏng bằng parser đó |
| no such service datanode2 | Thiếu overlay; kiểm tra config --services, không đổi tên service/volume |
| Không thấy đường dẫn cũ | Đọc history và scope; DataFrame cũ vẫn giữ URI cũ, cần đọc lại |
| .COMPLETED nhưng HDFS chưa đủ | Source/Sink bất đồng bộ; chờ channel rỗng, file đóng và kiểm chứng; không publish lại |
| OOM, Spark/JVM mất kết nối | Xem exit code/OOM/log, dừng job, giảm concurrency/pha/cache từ số đo; report running không chứng minh hoàn tất |
| Thiếu dự phòng đĩa | Guard dừng job; kiểm kê runtime/output/cache theo đúng chủ sở hữu, không xóa volume |
| Guard báo session cũ | Dừng Spark session hoặc restart kernel; không tái sử dụng session/context có sẵn cho run mới |
| Legacy test thiếu block | Lỗi lịch sử tách khỏi raw thật; di chuyển namespace không phục hồi block; không format để sửa |

Trong Python notebook, schema inference/show/count đều có thể kích hoạt job. collect/toPandas chỉ sau aggregate/giới hạn phù hợp; không đưa toàn raw về RAM. Snapshot memory và free space không phải quota cứng hoặc phép đo peak shuffle liên tục.

## Thu hồi dung lượng Docker

Disk read/write trong Docker là I/O tích lũy, không phải lượng đĩa đang chiếm. Dừng container thu hồi RAM/tác vụ nhưng không tự trả toàn dung lượng VHDX cho Windows. Xóa file Linux có thể giải phóng block bên trong mà VHDX vẫn giữ kích thước.

[Reclaim-DockerDisk.ps1](../tools/maintenance/Reclaim-DockerDisk.ps1) là **bảo trì thủ công** ở PowerShell Administrator. Dừng job/notebook và drain Flume trước; nó từ chối Spark/Flume/container khác đang chạy, dừng Hadoop/Docker và **tất cả phiên WSL**, trim, kiểm tra detach và compact đúng VHDX đã chọn. Nó không xóa named volumes và không tự khởi động lại stack.

~~~powershell
# Chỉ dùng khi cần compact, không chạy sau mọi lượt EDA.
& ./tools/maintenance/Reclaim-DockerDisk.ps1
# Nếu Docker disk đã chuyển ổ, truyền đúng path thực tế:
& ./tools/maintenance/Reclaim-DockerDisk.ps1 -DockerDiskPath 'D:/Docker/wsl/disk/docker_data.vhdx'
~~~

Path mặc định dưới LOCALAPPDATA/Docker/wsl/disk; không cố định tài khoản ASUS hoặc ổ C. Kết quả đo trước/sau ở outputs/eda-reclaim-result.json; không hứa trả về đúng dung lượng ban đầu. Pha dọn docs không thực thi công cụ này.

## Chia sẻ code và tái lập

Git/image không chứa volume hoặc dataset. Commit code, config, manifests quản lý, docs, fixture nhỏ và graph; loại .env, token, output, cache và artifact lớn. Backup rồi clear notebook output khi được thực hiện; kiểm tra source cells vẫn không có token.

~~~powershell
git status --short
git diff --check
git diff
# Stage các file đã xem, sau đó rà nội dung:
git diff --cached --stat
git diff --cached
~~~

Không stage rộng toàn workspace để tiện. Remote/push dùng repository của nhóm, không tự đặt URL hoặc push trong quy trình kiểm tra. Clone mới đọc README rồi fixture → download/manifest → put missing → verify → EDA một category. Không chạy migration hay publish dữ liệu cũ như setup mặc định.

Dependency trực tiếp và phiên bản image được ghim; chưa khóa digest/base image và toàn bộ dependency bắc cầu. Registry chứa phần mềm, không chứa dữ liệu host. Notebook GPU cloud không tự truy cập hostname/volume/HDFS local; artifact trao đổi cần mapping/version/checksum riêng.

Graphify là công cụ tùy chọn; workflow và giới hạn nằm trong [graphify.md](graphify.md).
