# Graphify cho điều hướng repository

Dùng Graphify (package graphifyy==0.9.85) trong image tools, không cài thêm vào Python Spark/host. Container giới hạn CPU 1/RAM 512 MiB. Đây là CLI, không tự cài skill toàn cục.

[Refresh-Graph.ps1](../tools/graphify/Refresh-Graph.ps1) dùng AST --code-only --no-cluster, bổ sung service/mount/dependencies/heading tài liệu, rồi cluster/export --no-label; không cần API key hoặc semantic LLM pass. Dùng overlay ba DataNode trong cấu hình, --no-deps không khởi động HDFS và --pull never không tải image.

PowerShell host tại project/; build image riêng nếu chưa có:

~~~powershell
docker compose --profile tools build graphify
& ./tools/graphify/Refresh-Graph.ps1
docker compose -f compose.yaml -f compose.datanodes.yaml --profile tools run --rm --pull never --no-deps graphify query run_eda --budget 700
~~~

Đọc [project-map](project-map.md) trước, query nhỏ rồi mở source. --budget là mục tiêu dung lượng ngữ cảnh query, có thể truncate hoặc vượt để giữ liên kết; không là giới hạn chi phí hội thoại. Thu hẹp tên symbol/câu hỏi trước tăng budget.

Graphify không hiểu đầy đủ PowerShell/YAML/notebook chỉ bằng AST. [enrich_graph.py](../tools/graphify/enrich_graph.py) bổ sung liên kết cấu hình/heading xác định, không suy diễn dữ liệu/nghiệp vụ. Cảnh báo script không có symbol không có nghĩa script không hợp lệ.

graphify-out/graph.json, GRAPH_REPORT.md và graph.html là artifacts điều hướng được commit; cache/manifest machine state không commit. .graphifyignore loại data, artifacts, outputs, .env, Git/caches. Refresh sau đổi code/layout/docs; file deleted trong Git diff là thay đổi source cần giữ, không restore từ graph cũ.

Nguồn công cụ để tra cứu: [Graphify repository](https://github.com/safishamsi/graphify). Không hứa mức tiết kiệm token cố định hoặc dùng graph thay code review.
