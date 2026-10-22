# Graph Report - workspace  (2026-10-18)

## Corpus Check
- cluster-only mode — file stats not available

## Summary
- 448 nodes · 716 edges · 26 communities (16 shown, 10 thin omitted)
- Extraction: 94% EXTRACTED · 6% INFERRED · 0% AMBIGUOUS · INFERRED: 42 edges (avg confidence: 0.85)
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

## God Nodes (most connected - your core abstractions)
1. `Compose services, ports, mounts and resource limits` - 24 edges
2. `analyze_category()` - 18 edges
3. `run_readiness()` - 17 edges
4. `run_silver()` - 17 edges
5. `analyze_all()` - 16 edges
6. `parse_line()` - 16 edges
7. `parse_partition()` - 15 edges
8. `spark-notebook Compose service` - 12 edges
9. `Report` - 11 edges
10. `Hdfs` - 10 edges

## Surprising Connections (you probably didn't know these)
- `transfer()` --indirect_call--> `content()`  [INFERRED]
  tests/test_download_amazon_hf.py → notebooks/eda_model_readiness.py
- `get_scope()` --calls--> `load_scope()`  [INFERRED]
  notebooks/eda_recommendation.py → ingestion/scripts/dataset_scope.py
- `Get-ExperimentScope()` --calls--> `Get-Content()`  [INFERRED]
  ingestion/scripts/Get-ExperimentScope.ps1 → tests/test_scope_scripts.ps1
- `scope()` --calls--> `load_scope()`  [EXTRACTED]
  processing/silver.py → ingestion/scripts/dataset_scope.py
- `nested()` --calls--> `canonical()`  [EXTRACTED]
  processing/silver.py → notebooks/eda_recommendation.py

## Import Cycles
- None detected.

## Communities (26 total, 10 thin omitted)

### Community 0 - "Community 0"
Cohesion: 0.14
Nodes (12): load_scope(), existing_file(), filenames(), main(), download(), transfer(), parse_args(), retry() (+4 more)

### Community 1 - "Community 1"
Cohesion: 0.07
Nodes (31): analyze_all(), canonical(), digest(), get_categories(), inventory(), key_value(), load(), metadata_group() (+23 more)

### Community 2 - "Community 2"
Cohesion: 0.14
Nodes (18): analyze_fold(), bins(), content(), count_if(), diagnostic_fold(), profile_text(), read_manifest(), run_readiness() (+10 more)

### Community 3 - "Community 3"
Cohesion: 0.05
Nodes (35): Repository navigation, Additional DataNode services and volumes, Compose services, ports, mounts and resource limits, Kiến trúc và phạm vi triển khai, Hệ thống hiện tại, Mạng, lưu trữ và ingestion, Các tầng dữ liệu, Hướng mở rộng, chưa triển khai (+27 more)

### Community 4 - "Community 4"
Cohesion: 0.08
Nodes (8): main(), validate_path(), verify(), EdaChecks, read_table(), rate(), read_csv(), summarize()

### Community 5 - "Community 5"
Cohesion: 0.06
Nodes (31): Processing: từ EDA đến Silver, Gold và dữ liệu đánh giá, 1.1. Đầu vào khác với đáp án cần dự đoán, 3.3. Một sản phẩm khác một review duy nhất, 3.4. Các nhóm đánh giá nằm trong cùng tập test, 4. Từ phát hiện EDA đến hành động, 5. Các bước processing cụ thể sẽ thực hiện, Bước 1. Khóa input, schema và cấu hình run, Bước 2. Parse và chuẩn hóa Silver (+23 more)

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
Cohesion: 0.14
Nodes (13): Lịch sử triển khai và bằng chứng kiểm chứng, 14/10/2026 — ba full EDA hoàn tất và kế hoạch processing, 16/10/2026 — Silver triển khai và hoàn tất ba category, 18/10/2026 — EDA bổ sung trên Silver hoàn tất, 11/10/2026 — migration và Video Games metadata, 13/10/2026 — EDA cũ và áp lực tài nguyên, 11/10/2026 — volume và smoke ban đầu, 14/10/2026 — phạm vi ba ngành hàng và vòng đời EDA (+5 more)

### Community 11 - "Community 11"
Cohesion: 0.14
Nodes (13): Vận hành, xử lý lỗi và bảo trì, Xử lý lỗi, Thu hồi dung lượng Docker, Chỉ dùng khi cần compact, không chạy sau mọi lượt EDA., Nếu Docker disk đã chuyển ổ, truyền đúng path thực tế:, Chia sẻ code và tái lập, Stage các file đã xem, sau đó rà nội dung:, Quy ước lệnh và bảo toàn (+5 more)

### Community 12 - "Community 12"
Cohesion: 0.15
Nodes (12): Phạm vi, ingestion và quản lý dữ liệu, Retention và dọn dữ liệu, Vị trí và hợp đồng dữ liệu, Scope và nguồn bằng chứng, Tải trên PowerShell host, Hoặc chỉ một category và một loại dữ liệu:, Nhập HDFS batch — PowerShell host, Giới hạn preview một category: (+4 more)

### Community 13 - "Community 13"
Cohesion: 0.27
Nodes (6): analyze_category(), degree_bin(), duplicates(), identity_overview(), Report, stats()

### Community 15 - "Community 15"
Cohesion: 0.25
Nodes (7): Kiểm thử nhỏ, Smoke HDFS chỉ đọc, EDA Recommendation, Kiểm thử bổ sung pha 4, Scope và ingestion offline — PowerShell host, Silver, EDA bổ sung trên Silver

### Community 18 - "Community 18"
Cohesion: 0.29
Nodes (6): Đồ án Big Data — hệ thống gợi ý sản phẩm, Bắt đầu trên máy đã có dữ liệu — PowerShell host, Bài toán và dữ liệu, Tài liệu và cấu trúc, Làm việc cùng nhóm, Trạng thái hiện tại

## Knowledge Gaps
- **172 isolated node(s):** `JAVA_OPTS`, `data/staging mounted path`, `data/spool mounted path`, `infrastructure/flume/flume.conf mounted path`, `notebooks mounted path` (+167 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 261 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **10 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `Compose services, ports, mounts and resource limits` connect `Community 3` to `Community 5`, `Community 6`, `Community 7`, `Community 9`, `Community 10`, `Community 11`, `Community 12`, `Community 15`, `Community 18`?**
  _High betweenness centrality (0.200) - this node is a cross-community bridge._
- **Are the 6 inferred relationships involving `analyze_category()` (e.g. with `metadata_group()` and `.chart()`) actually correct?**
  _`analyze_category()` has 6 INFERRED edges - model-reasoned connections that need verification._
- **What connects `JAVA_OPTS`, `data/staging mounted path`, `data/spool mounted path` to the rest of the system?**
  _172 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Community 0` be split into smaller, more focused modules?**
  _Cohesion score 0.14130434782608695 - nodes in this community are weakly interconnected._
- **Why does `RuntimeChecks` connect `Community 14` to `Community 4`?**
  _High betweenness centrality (0.020) - this node is a cross-community bridge._
- **Are the 4 inferred relationships involving `run_readiness()` (e.g. with `interrupt()` and `.rows()`) actually correct?**
  _`run_readiness()` has 4 INFERRED edges - model-reasoned connections that need verification._
- **Should `Community 1` be split into smaller, more focused modules?**
  _Cohesion score 0.0703962703962704 - nodes in this community are weakly interconnected._