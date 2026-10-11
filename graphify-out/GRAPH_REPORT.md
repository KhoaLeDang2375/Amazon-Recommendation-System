# Graph Report - workspace  (2026-10-11)

## Corpus Check
- cluster-only mode — file stats not available

## Summary
- 92 nodes · 94 edges · 15 communities (6 shown, 9 thin omitted)
- Extraction: 100% EXTRACTED · 0% INFERRED · 0% AMBIGUOUS
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- Community 0
- Community 1
- Community 2
- Community 3
- Community 5
- Community 6
- Community 7
- Community 9
- Community 10
- Community 11
- Community 12

## God Nodes (most connected - your core abstractions)
1. `Compose services, ports, mounts and resource limits` - 22 edges
2. `spark-notebook Compose service` - 11 edges
3. `flume Compose service` - 9 edges
4. `namenode Compose service` - 6 edges
5. `hdfs-init Compose service` - 5 edges
6. `datanode Compose service` - 4 edges
7. `namenode-init Compose service` - 2 edges
8. `datanode-init Compose service` - 2 edges
9. `ingestion/scripts mounted path` - 2 edges
10. `flume-init Compose service` - 2 edges

## Surprising Connections (you probably didn't know these)
- `Compose services, ports, mounts and resource limits` --defines--> `flume Compose service`  [EXTRACTED]
  compose.yaml → compose.yaml  _Bridges community 0 → community 2_
- `Compose services, ports, mounts and resource limits` --defines--> `hdfs-init Compose service`  [EXTRACTED]
  compose.yaml → compose.yaml  _Bridges community 0 → community 1_
- `flume Compose service` --depends_on--> `hdfs-init Compose service`  [EXTRACTED]
  compose.yaml → compose.yaml  _Bridges community 1 → community 2_

## Import Cycles
- None detected.

## Communities (15 total, 9 thin omitted)

### Community 0 - "Community 0"
Cohesion: 0.10
Nodes (14): Repository navigation, Compose services, ports, mounts and resource limits, Quản lý dữ liệu, Graphify cho repository, Bản đồ đọc nhanh, Xác nhận sau khi sắp xếp, Chuẩn bị web demo, Kiểm thử nhỏ (+6 more)

### Community 1 - "Community 1"
Cohesion: 0.20
Nodes (8): artifacts mounted path, infrastructure/spark/conf mounted path, ingestion/scripts mounted path, notebooks mounted path, outputs mounted path, tests mounted path, hdfs-init Compose service, spark-notebook Compose service

### Community 2 - "Community 2"
Cohesion: 0.20
Nodes (6): JAVA_OPTS, flume-env.sh script, data/spool mounted path, infrastructure/flume/flume.conf mounted path, flume Compose service, flume-init Compose service

### Community 3 - "Community 3"
Cohesion: 0.22
Nodes (8): Các lỗi thường gặp, PowerShell báo duplicated keys, Spark COLUMN_ALREADY_EXISTS, Không thấy đường dẫn cũ, Có .COMPLETED nhưng chưa đủ dữ liệu HDFS, File legacy thiếu block, Không có pyspark, Jupyter hỏi token/password

### Community 5 - "Community 5"
Cohesion: 0.25
Nodes (7): Đồ án Big Data — ingestion và Spark EDA, Khởi chạy, Nhập dữ liệu, Spark notebook, Thư mục, Bản đồ Graphify, Vận hành và chia sẻ

### Community 6 - "Community 6"
Cohesion: 0.33
Nodes (5): Các lệnh và ý nghĩa, Publish, HDFS, Python trong notebook, Graphify

## Knowledge Gaps
- **40 isolated node(s):** `JAVA_OPTS`, `data/staging mounted path`, `data/spool mounted path`, `infrastructure/flume/flume.conf mounted path`, `notebooks mounted path` (+35 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 64 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **9 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `Compose services, ports, mounts and resource limits` connect `Community 0` to `Community 1`, `Community 2`, `Community 3`, `Community 5`, `Community 6`, `Community 7`, `Community 9`, `Community 10`?**
  _High betweenness centrality (0.570) - this node is a cross-community bridge._
- **What connects `JAVA_OPTS`, `data/staging mounted path`, `data/spool mounted path` to the rest of the system?**
  _40 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Community 0` be split into smaller, more focused modules?**
  _Cohesion score 0.10276679841897234 - nodes in this community are weakly interconnected._
- **Why does `spark-notebook Compose service` connect `Community 1` to `Community 0`?**
  _High betweenness centrality (0.140) - this node is a cross-community bridge._
- **Why does `flume Compose service` connect `Community 2` to `Community 0`, `Community 1`?**
  _High betweenness centrality (0.133) - this node is a cross-community bridge._