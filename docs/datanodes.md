# Thêm DataNode

`compose.datanodes.yaml` bổ sung `datanode2`, `datanode3` với volume riêng. Giữ service/volume cũ và replication mặc định `1`; thêm node không tự tăng số bản sao hay dung lượng ổ đĩa vật lý của máy host.

Node đầu tiên có service/hostname `datanode1`; volume vẫn mang tên `hadoop-demo_datanode_data` để giữ các block hiện có. Container Compose tương ứng là `hadoop-demo-datanode1-1`.

Chạy tại thư mục project bằng PowerShell trên host:

```powershell
docker compose -f compose.yaml -f compose.datanodes.yaml config --quiet
docker compose -f compose.yaml -f compose.datanodes.yaml up -d --no-recreate datanode2 datanode3
docker compose exec -T namenode hdfs dfsadmin -report
```

Chờ HDFS báo `Live datanodes (3)`. Init có `Exited (0)` là bình thường. Dùng cả hai file Compose khi quản lý các node bổ sung; không chạy `down -v`, format HDFS hoặc dùng chung volume cho nhiều DataNode.

`no such service: datanode2` nghĩa là cấu hình ghép không chứa service đó, chưa chạy đến init. Kiểm tra file đã lưu và danh sách:

```powershell
docker compose -f compose.yaml -f compose.datanodes.yaml config --services
```

Khi thêm node tiếp theo, khai báo service/hostname/volume riêng và cấp quyền cho volume mới. Nếu sửa init đã tồn tại, `--no-recreate` sẽ không cập nhật init cũ: hãy chạy init riêng cho volume mới trước khi khởi chạy node đó. Không gắn các volume DataNode đang hoạt động vào init để sửa quyền hàng loạt.
