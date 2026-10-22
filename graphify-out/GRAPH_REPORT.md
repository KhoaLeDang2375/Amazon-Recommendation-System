# Graph Report - workspace  (2026-10-22)

## Corpus Check
- cluster-only mode — file stats not available

## Summary
- 479 nodes · 794 edges · 31 communities (20 shown, 11 thin omitted)
- Extraction: 94% EXTRACTED · 6% INFERRED · 0% AMBIGUOUS · INFERRED: 44 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- Community 0
- Community 1
- Community 2
- Community 3
- Community 4
- Community 5
- Community 6
- Community 7
- Community 8
- Community 9
- Community 10
- Community 11
- Community 12
- Community 13
- Community 14
- Community 15
- Community 16
- Community 17
- Community 18
- Community 19
- Community 20
- Community 21
- Community 22
- Community 23
- Community 24
- Community 25
- Community 26

## God Nodes (most connected - your core abstractions)
1. `Compose services, ports, mounts and resource limits` - 25 edges
2. `analyze_category()` - 18 edges
3. `run_readiness()` - 17 edges
4. `run_silver()` - 17 edges
5. `analyze_all()` - 16 edges
6. `parse_line()` - 16 edges
7. `parse_partition()` - 15 edges
8. `_run_baseline()` - 12 edges
9. `spark-notebook Compose service` - 12 edges
10. `run_baseline()` - 11 edges

## Surprising Connections (you probably didn't know these)
- `transfer()` --indirect_call--> `content()`  [INFERRED]
  tests/test_download_amazon_hf.py → notebooks/eda_model_readiness.py
- `get_scope()` --calls--> `load_scope()`  [INFERRED]
  notebooks/eda_recommendation.py → ingestion/scripts/dataset_scope.py
- `Get-ExperimentScope()` --calls--> `Get-Content()`  [INFERRED]
  ingestion/scripts/Get-ExperimentScope.ps1 → tests/test_scope_scripts.ps1
- `scope()` --calls--> `load_scope()`  [EXTRACTED]
  processing/silver.py → ingestion/scripts/dataset_scope.py
- `run_silver()` --calls--> `digest()`  [EXTRACTED]
  processing/silver.py → notebooks/eda_recommendation.py

## Import Cycles
- None detected.

## Communities (31 total, 11 thin omitted)

### Community 0 - "Community 0"
Cohesion: 0.15
Nodes (12): load_scope(), existing_file(), filenames(), main(), download(), transfer(), parse_args(), retry() (+4 more)

### Community 1 - "Community 1"
Cohesion: 0.08
Nodes (30): analyze_all(), analyze_category(), canonical(), degree_bin(), digest(), duplicates(), get_categories(), identity_overview() (+22 more)

### Community 2 - "Community 2"
Cohesion: 0.10
Nodes (27): evaluate(), fit_fold(), keep(), recommend(), query_population(), query_scores(), run_baseline(), _run_baseline() (+19 more)

### Community 3 - "Community 3"
Cohesion: 0.06
Nodes (32): Processing: từ EDA đến Silver, Gold và dữ liệu đánh giá, 1.1. Đầu vào khác với đáp án cần dự đoán, 3.3. Một sản phẩm khác một review duy nhất, 3.4. Các nhóm đánh giá nằm trong cùng tập test, 4. Từ phát hiện EDA đến hành động, 5. Các bước processing cụ thể sẽ thực hiện, Bước 1. Khóa input, schema và cấu hình run, Bước 2. Parse và chuẩn hóa Silver (+24 more)

### Community 4 - "Community 4"
Cohesion: 0.06
Nodes (10): main(), validate_path(), verify(), BaselineCheck, EdaChecks, read_table(), ReadinessChecks, rate() (+2 more)

### Community 5 - "Community 5"
Cohesion: 0.10
Nodes (11): create_spark(), Hdfs, local_json(), main(), Resources, run_silver(), schema(), scope() (+3 more)

### Community 6 - "Community 6"
Cohesion: 0.07
Nodes (26): Kế hoạch điều chỉnh phạm vi và dọn code, tài liệu, dữ liệu, 4.4. Tài nguyên và bảo trì, 5. EDA phải đủ căn cứ cho processing, 6. Hợp nhất tài liệu, 7. Dọn dữ liệu có kiểm kê, 7.1. Dữ liệu giữ và dữ liệu ngoài phạm vi, 7.2. Trình tự dọn khi triển khai, 7.3. Quy tắc lưu trữ về sau (+18 more)

### Community 7 - "Community 7"
Cohesion: 0.08
Nodes (25): Đặc tả và vận hành EDA Recommendation, 2. Chạy an toàn — PowerShell host, 5.3. Duplicates và metadata conflicts, 5.4. Rating, text và price, 5.5. Thời gian, 5.6. Sparsity, long tail và tác động lọc, 5.7. Metadata coverage và join, 5.8. Gates, temporal split và chuyển sang processing (+17 more)

### Community 8 - "Community 8"
Cohesion: 0.09
Nodes (8): Get-ExperimentScope(), Invoke-Case(), Get-Content(), Get-Item(), Invoke-Case(), Save-HostResources(), Save-ReadinessResources(), Save-SilverHostResources()

### Community 9 - "Community 9"
Cohesion: 0.10
Nodes (20): Silver: code, lưu trữ và vận hành, 5.2. Không đồng nhất ba DataNode với ba ổ vật lý, 6. Dự toán dung lượng và điều kiện dừng, 6.1. Phần đã biết và phần chưa đo, 6.2. Ngân sách thử, không phải dự đoán kích thước nén, 6.3. Cách kiểm chứng và điều chỉnh, 7. Hive ở đâu trong kế hoạch?, 8. Mốc kiểm tra và lệnh vận hành (+12 more)

### Community 10 - "Community 10"
Cohesion: 0.21
Nodes (12): Repository navigation, Additional DataNode services and volumes, Compose services, ports, mounts and resource limits, Bản đồ đọc nhanh, data/staging mounted path, datanode-init Compose service, datanode1 Compose service, datanode2 Compose service (+4 more)

### Community 11 - "Community 11"
Cohesion: 0.14
Nodes (13): Lịch sử triển khai và bằng chứng kiểm chứng, 14/10/2026 — ba full EDA hoàn tất và kế hoạch processing, 16/10/2026 — Silver triển khai và hoàn tất ba category, 18/10/2026 — EDA bổ sung trên Silver hoàn tất, 11/10/2026 — migration và Video Games metadata, 13/10/2026 — EDA cũ và áp lực tài nguyên, 11/10/2026 — volume và smoke ban đầu, 14/10/2026 — phạm vi ba ngành hàng và vòng đời EDA (+5 more)

### Community 12 - "Community 12"
Cohesion: 0.14
Nodes (13): Vận hành, xử lý lỗi và bảo trì, Xử lý lỗi, Thu hồi dung lượng Docker, Chỉ dùng khi cần compact, không chạy sau mọi lượt EDA., Nếu Docker disk đã chuyển ổ, truyền đúng path thực tế:, Chia sẻ code và tái lập, Stage các file đã xem, sau đó rà nội dung:, Quy ước lệnh và bảo toàn (+5 more)

### Community 13 - "Community 13"
Cohesion: 0.15
Nodes (12): Phạm vi, ingestion và quản lý dữ liệu, Retention và dọn dữ liệu, Vị trí và hợp đồng dữ liệu, Scope và nguồn bằng chứng, Tải trên PowerShell host, Hoặc chỉ một category và một loại dữ liệu:, Nhập HDFS batch — PowerShell host, Giới hạn preview một category: (+4 more)

### Community 14 - "Community 14"
Cohesion: 0.18
Nodes (9): artifacts mounted path, infrastructure/spark/conf mounted path, ingestion/manifests mounted path, ingestion/scripts mounted path, notebooks mounted path, outputs mounted path, tests mounted path, hdfs-init Compose service (+1 more)

### Community 16 - "Community 16"
Cohesion: 0.20
Nodes (6): JAVA_OPTS, flume-env.sh script, data/spool mounted path, infrastructure/flume/flume.conf mounted path, flume Compose service, flume-init Compose service

### Community 17 - "Community 17"
Cohesion: 0.22
Nodes (8): Kiểm thử nhỏ, Smoke HDFS chỉ đọc, EDA Recommendation, Kiểm thử bổ sung pha 4, Scope và ingestion offline — PowerShell host, Silver, EDA bổ sung trên Silver, Kiểm tra baseline ALS

### Community 19 - "Community 19"
Cohesion: 0.29
Nodes (6): Đồ án Big Data — hệ thống gợi ý sản phẩm, Bắt đầu trên máy đã có dữ liệu — PowerShell host, Bài toán và dữ liệu, Tài liệu và cấu trúc, Làm việc cùng nhóm, Trạng thái hiện tại

### Community 20 - "Community 20"
Cohesion: 0.33
Nodes (5): Kiến trúc và phạm vi triển khai, Hệ thống hiện tại, Mạng, lưu trữ và ingestion, Các tầng dữ liệu, Hướng mở rộng, chưa triển khai

### Community 21 - "Community 21"
Cohesion: 0.50
Nodes (3): Thử baseline implicit ALS trên Silver, Chạy notebook, Output và cách đọc

## Knowledge Gaps
- **177 isolated node(s):** `JAVA_OPTS`, `data/staging mounted path`, `data/spool mounted path`, `infrastructure/flume/flume.conf mounted path`, `notebooks mounted path` (+172 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 276 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **11 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `Compose services, ports, mounts and resource limits` connect `Community 10` to `Community 3`, `Community 6`, `Community 7`, `Community 9`, `Community 11`, `Community 12`, `Community 13`, `Community 14`, `Community 16`, `Community 17`, `Community 19`, `Community 20`, `Community 21`, `Community 22`, `Community 23`, `Community 24`?**
  _High betweenness centrality (0.186) - this node is a cross-community bridge._
- **Are the 6 inferred relationships involving `analyze_category()` (e.g. with `metadata_group()` and `.chart()`) actually correct?**
  _`analyze_category()` has 6 INFERRED edges - model-reasoned connections that need verification._
- **What connects `JAVA_OPTS`, `data/staging mounted path`, `data/spool mounted path` to the rest of the system?**
  _177 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Community 1` be split into smaller, more focused modules?**
  _Cohesion score 0.07759562841530054 - nodes in this community are weakly interconnected._
- **Why does `RuntimeChecks` connect `Community 15` to `Community 4`?**
  _High betweenness centrality (0.020) - this node is a cross-community bridge._
- **Are the 4 inferred relationships involving `run_readiness()` (e.g. with `interrupt()` and `.rows()`) actually correct?**
  _`run_readiness()` has 4 INFERRED edges - model-reasoned connections that need verification._
- **Should `Community 2` be split into smaller, more focused modules?**
  _Cohesion score 0.09758454106280193 - nodes in this community are weakly interconnected._