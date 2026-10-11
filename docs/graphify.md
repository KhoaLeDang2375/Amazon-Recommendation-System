# Graphify cho repository

Dùng [Graphify](https://github.com/safishamsi/graphify), package `graphifyy==0.9.85`, trong container profile `tools`. Không cài thêm vào Python của Spark hay máy host. Đây là tích hợp CLI; không tự cài skill toàn cục cho coding agent.

`tools/graphify/Refresh-Graph.ps1` thực hiện ba bước: AST extraction `--code-only --no-cluster`; bổ sung service/mount/dependency Compose và heading tài liệu bằng script Python; cluster và export bằng `--no-label`. Workflow không cần API key hoặc semantic extraction qua LLM.

Graphify không hiểu đầy đủ PowerShell/YAML/notebook chỉ bằng AST. Script bổ sung tạo liên kết cấu hình xác định; nó không suy diễn ý nghĩa nghiệp vụ từ dataset. Heading tiếng Việt và source location giúp tra cứu file liên quan, không thay thế nguồn.

Đọc `docs/project-map.md` trước, truy vấn với budget nhỏ, rồi mở đúng file. Ví dụ:

```powershell
docker compose --profile tools run --rm --no-deps graphify query "spark-notebook" --budget 1200
```

Graph được lưu ở `graphify-out/graph.json`, báo cáo và HTML do CLI export. Refresh sau thay đổi cấu trúc/code/tài liệu. `.graphifyignore` loại data, artifacts, `.env`, graph output và cache khỏi đầu vào. Generated graph được commit để bạn cùng nhóm đọc, cache/machine state không commit. Không khẳng định mức tiết kiệm token cố định: hiệu quả phụ thuộc truy vấn và việc chỉ đọc file liên quan.
