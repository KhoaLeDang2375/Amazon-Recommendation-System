# Graph Report - workspace  (2026-10-11)

## Corpus Check
- cluster-only mode — file stats not available

## Summary
- 144 nodes · 168 edges · 16 communities (8 shown, 8 thin omitted)
- Extraction: 98% EXTRACTED · 2% INFERRED · 0% AMBIGUOUS · INFERRED: 4 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- Community 0
- Community 1
- Community 2
- Community 3
- Community 5
- Community 6
- Community 7
- Community 8
- Community 9
- Community 10
- Community 11
- Community 12

## God Nodes (most connected - your core abstractions)
1. `Compose services, ports, mounts and resource limits` - 25 edges
2. `spark-notebook Compose service` - 11 edges
3. `main()` - 9 edges
4. `flume Compose service` - 9 edges
5. `namenode Compose service` - 8 edges
6. `DownloadTests` - 6 edges
7. `hdfs-init Compose service` - 5 edges
8. `sha256()` - 4 edges
9. `verify()` - 4 edges
10. `retry()` - 4 edges

## Surprising Connections (you probably didn't know these)
- `Compose services, ports, mounts and resource limits` --extended_by--> `Additional DataNode services and volumes`  [EXTRACTED]
  compose.yaml → compose.datanodes.yaml
- `datanode2 Compose service` --depends_on--> `namenode Compose service`  [EXTRACTED]
  compose.datanodes.yaml → compose.yaml
- `datanode3 Compose service` --depends_on--> `namenode Compose service`  [EXTRACTED]
  compose.datanodes.yaml → compose.yaml

## Import Cycles
- None detected.

## Communities (16 total, 8 thin omitted)

### Community 0 - "Community 0"
Cohesion: 0.08
Nodes (21): Repository navigation, Additional DataNode services and volumes, Compose services, ports, mounts and resource limits, Kiến trúc hiện tại, Quản lý dữ liệu, Thêm DataNode, Graphify cho repository, Bản đồ đọc nhanh (+13 more)

### Community 1 - "Community 1"
Cohesion: 0.18
Nodes (10): existing_file(), filenames(), main(), download(), transfer(), parse_args(), retry(), save_manifest() (+2 more)

### Community 2 - "Community 2"
Cohesion: 0.20
Nodes (8): artifacts mounted path, infrastructure/spark/conf mounted path, ingestion/scripts mounted path, notebooks mounted path, outputs mounted path, tests mounted path, hdfs-init Compose service, spark-notebook Compose service

### Community 3 - "Community 3"
Cohesion: 0.20
Nodes (9): Tải Amazon Reviews 2023 qua Hugging Face, SSD và mạng tốt; có thể dùng nhiều CPU hơn, Chỉ một category và metadata, Kiểm tra đã có dữ liệu và tải tiếp, Nhập phần còn thiếu vào HDFS raw, Xem kế hoạch; kiểm tra SHA-256 nguồn trước khi put, Nhập các file còn thiếu, replication 1, Chạy trên host PowerShell (+1 more)

### Community 5 - "Community 5"
Cohesion: 0.20
Nodes (6): JAVA_OPTS, flume-env.sh script, data/spool mounted path, infrastructure/flume/flume.conf mounted path, flume Compose service, flume-init Compose service

### Community 6 - "Community 6"
Cohesion: 0.22
Nodes (8): Các lỗi thường gặp, PowerShell báo duplicated keys, Spark COLUMN_ALREADY_EXISTS, Không thấy đường dẫn cũ, Có .COMPLETED nhưng chưa đủ dữ liệu HDFS, File legacy thiếu block, Không có pyspark, Jupyter hỏi token/password

### Community 7 - "Community 7"
Cohesion: 0.25
Nodes (7): Đồ án Big Data — ingestion và Spark EDA, Khởi chạy, Nhập dữ liệu, Spark notebook, Thư mục, Bản đồ Graphify, Vận hành và chia sẻ

### Community 9 - "Community 9"
Cohesion: 0.33
Nodes (5): Các lệnh và ý nghĩa, Publish, HDFS, Python trong notebook, Graphify

## Knowledge Gaps
- **50 isolated node(s):** `JAVA_OPTS`, `data/staging mounted path`, `data/spool mounted path`, `infrastructure/flume/flume.conf mounted path`, `notebooks mounted path` (+45 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 89 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **8 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `Compose services, ports, mounts and resource limits` connect `Community 0` to `Community 2`, `Community 3`, `Community 5`, `Community 6`, `Community 7`, `Community 9`, `Community 10`?**
  _High betweenness centrality (0.348) - this node is a cross-community bridge._
- **Are the 2 inferred relationships involving `main()` (e.g. with `download()` and `sha256()`) actually correct?**
  _`main()` has 2 INFERRED edges - model-reasoned connections that need verification._
- **What connects `JAVA_OPTS`, `data/staging mounted path`, `data/spool mounted path` to the rest of the system?**
  _50 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Community 0` be split into smaller, more focused modules?**
  _Cohesion score 0.07575757575757576 - nodes in this community are weakly interconnected._
- **Why does `spark-notebook Compose service` connect `Community 2` to `Community 0`?**
  _High betweenness centrality (0.070) - this node is a cross-community bridge._
- **Should `Community 4` be split into smaller, more focused modules?**
  _Cohesion score 0.10526315789473684 - nodes in this community are weakly interconnected._
- **Why does `flume Compose service` connect `Community 5` to `Community 0`, `Community 2`?**
  _High betweenness centrality (0.066) - this node is a cross-community bridge._