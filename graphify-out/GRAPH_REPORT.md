# Graph Report - workspace  (2026-11-08)

## Corpus Check
- cluster-only mode — file stats not available

## Summary
- 921 nodes · 1845 edges · 56 communities (35 shown, 21 thin omitted)
- Extraction: 95% EXTRACTED · 5% INFERRED · 0% AMBIGUOUS · INFERRED: 95 edges (avg confidence: 0.85)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- Community 0
- Community 1
- Community 2
- Community 3
- Community 4
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
- Community 27
- Community 28
- Community 29
- Community 30
- Community 31
- Community 32
- Community 33
- Community 34
- Community 35
- Community 36
- Community 37
- Community 38
- Community 39
- Community 40
- Community 41
- Community 42
- Community 43
- Community 44
- Community 45
- Community 46
- Community 50
- Community 51

## God Nodes (most connected - your core abstractions)
1. `run()` - 31 edges
2. `run()` - 28 edges
3. `Compose services, ports, mounts and resource limits` - 27 edges
4. `read_json()` - 22 edges
5. `NeuralChecks` - 21 edges
6. `fit_trial()` - 20 edges
7. `sha256()` - 20 edges
8. `save_json()` - 20 edges
9. `analyze_category()` - 18 edges
10. `run_gold()` - 18 edges

## Surprising Connections (you probably didn't know these)
- `transfer()` --indirect_call--> `content()`  [INFERRED]
  tests/test_download_amazon_hf.py → notebooks/eda_model_readiness.py
- `Get-ExperimentScope()` --calls--> `Get-Content()`  [INFERRED]
  ingestion/scripts/Get-ExperimentScope.ps1 → tests/test_scope_scripts.ps1
- `get_scope()` --calls--> `load_scope()`  [INFERRED]
  notebooks/eda_recommendation.py → ingestion/scripts/dataset_scope.py
- `transfer()` --indirect_call--> `work()`  [INFERRED]
  ingestion/scripts/download_amazon_hf.py → notebooks/kaggle_recommendation.py
- `Save-HostResources()` --calls--> `Get-Item()`  [INFERRED]
  tools/eda/Run-Eda.ps1 → tests/test_scope_scripts.ps1

## Import Cycles
- None detected.

## Communities (56 total, 21 thin omitted)

### Community 0 - "Community 0"
Cohesion: 0.14
Nodes (13): existing_file(), filenames(), main(), download(), transfer(), parse_args(), retry(), save_manifest() (+5 more)

### Community 1 - "Community 1"
Cohesion: 0.16
Nodes (7): main(), parse_rows(), run_silver(), schema(), scope(), source_lineage(), validate_paths()

### Community 2 - "Community 2"
Cohesion: 0.16
Nodes (28): predictions(), als_predictions(), array_hash(), compare_results(), content_profiles(), evaluate(), fingerprint(), fit_als() (+20 more)

### Community 3 - "Community 3"
Cohesion: 0.27
Nodes (3): file_hash(), local_json(), verify()

### Community 4 - "Community 4"
Cohesion: 0.08
Nodes (8): configuration(), neural_fixture(), NeuralChecks, metrics(), scores(), scoring(), configuration(), TowerChecks

### Community 6 - "Community 6"
Cohesion: 0.08
Nodes (22): checkpoint_state(), early_stop_state(), epoch_batches(), fit_trial(), load_examples(), make_model(), mix64(), observed_pair_keys() (+14 more)

### Community 7 - "Community 7"
Cohesion: 0.05
Nodes (37): Kế hoạch notebook ALS, Content-based, NCF, DeepFM và Two-Tower trên Kaggle, 13.6. Scoring và đánh giá, 13.7. File cần triển khai và nghiệm thu, 14. Vận hành notebook Two-Tower (08/11/2026), 4. Notebook ALS, 5. Notebook content-based, 6. T4 × 2, tốc độ và checkpoint, 7. Thứ tự triển khai và nghiệm thu (+29 more)

### Community 8 - "Community 8"
Cohesion: 0.06
Nodes (33): Processing: từ EDA đến Silver, Gold và dữ liệu đánh giá, 1.1. Đầu vào khác với đáp án cần dự đoán, 3.3. Một sản phẩm khác một review duy nhất, 3.4. Các nhóm đánh giá nằm trong cùng tập test, 4. Từ phát hiện EDA đến hành động, 5. Các bước processing cụ thể sẽ thực hiện, Bước 1. Khóa input, schema và cấu hình run, Bước 2. Parse và chuẩn hóa Silver (+25 more)

### Community 9 - "Community 9"
Cohesion: 0.14
Nodes (22): atomic_checkpoint(), compact_d2(), load_review_vectors(), phase_valid(), read_checkpoint(), run(), run_suite(), validate_config() (+14 more)

### Community 10 - "Community 10"
Cohesion: 0.21
Nodes (12): child_path(), dataset_snapshot(), encode_batch(), encode_shard(), normalize_rows(), pending_tasks(), restore_shard(), run() (+4 more)

### Community 11 - "Community 11"
Cohesion: 0.07
Nodes (10): Get-ExperimentScope(), Invoke-Case(), Invoke-Case(), Get-Content(), Get-Item(), Invoke-Case(), Save-HostResources(), Save-ReadinessResources() (+2 more)

### Community 12 - "Community 12"
Cohesion: 0.07
Nodes (26): Kế hoạch điều chỉnh phạm vi và dọn code, tài liệu, dữ liệu, 4.4. Tài nguyên và bảo trì, 5. EDA phải đủ căn cứ cho processing, 6. Hợp nhất tài liệu, 7. Dọn dữ liệu có kiểm kê, 7.1. Dữ liệu giữ và dữ liệu ngoài phạm vi, 7.2. Trình tự dọn khi triển khai, 7.3. Quy tắc lưu trữ về sau (+18 more)

### Community 13 - "Community 13"
Cohesion: 0.07
Nodes (26): Đặc tả và vận hành EDA Recommendation, 2. Chạy an toàn — PowerShell host, 5.3. Duplicates và metadata conflicts, 5.4. Rating, text và price, 5.5. Thời gian, 5.6. Sparsity, long tail và tác động lọc, 5.7. Metadata coverage và join, 5.8. Gates, temporal split và chuyển sang processing (+18 more)

### Community 14 - "Community 14"
Cohesion: 0.09
Nodes (22): Lịch sử triển khai và bằng chứng kiểm chứng, 14/10/2026 — All Beauty hoàn tất, sửa thu log Windows PowerShell, 14/10/2026 — tăng CPU cho các batch tiếp theo, 14/10/2026 — ba full EDA hoàn tất và kế hoạch processing, 16/10/2026 — Silver triển khai và hoàn tất ba category, 18/10/2026 — EDA bổ sung trên Silver hoàn tất, 30/10/2026 — khóa cutoff bằng thống kê toàn Silver, 30/10/2026 — cập nhật kế hoạch Gold theo ba phiên bản DeepFM (+14 more)

### Community 15 - "Community 15"
Cohesion: 0.15
Nodes (16): canonical(), key_value(), nested_issues(), bad(), depth(), numeric(), parse_partition(), resolve_timestamp() (+8 more)

### Community 16 - "Community 16"
Cohesion: 0.23
Nodes (17): configure_auth(), export_category(), export_tables(), emit(), finalize(), inside(), json_hash(), main() (+9 more)

### Community 17 - "Community 17"
Cohesion: 0.10
Nodes (20): Silver: code, lưu trữ và vận hành, 5.2. Không đồng nhất ba DataNode với ba ổ vật lý, 6. Dự toán dung lượng và điều kiện dừng, 6.1. Phần đã biết và phần chưa đo, 6.2. Ngân sách thử, không phải dự đoán kích thước nén, 6.3. Cách kiểm chứng và điều chỉnh, 7. Hive ở đâu trong kế hoạch?, 8. Mốc kiểm tra và lệnh vận hành (+12 more)

### Community 18 - "Community 18"
Cohesion: 0.15
Nodes (11): build_fold(), clean_text(), description_profile(), export_text(), identify(), item_text(), mapping(), read_contract() (+3 more)

### Community 19 - "Community 19"
Cohesion: 0.17
Nodes (3): configuration(), fixture(), RecommendationChecks

### Community 20 - "Community 20"
Cohesion: 0.18
Nodes (5): run_gold(), create_spark(), Hdfs, GoldChecks, SilverChecks

### Community 21 - "Community 21"
Cohesion: 0.15
Nodes (11): Repository navigation, Compose services, ports, mounts and resource limits, Graphify cho điều hướng repository, Bản đồ đọc nhanh, Ingestion, data/staging mounted path, datanode-init Compose service, datanode1 Compose service (+3 more)

### Community 22 - "Community 22"
Cohesion: 0.12
Nodes (15): Gold G0–G2 và notebook Kaggle, Chạy trên máy local, G2 title + description ngày 03/11/2026, Kết quả G0–G2 ngày 30/10/2026, Chỉ chạy tiếp sau khi run trước complete và đã xem budget/resource report:, Tải và đọc embedding về máy local, Xuất Gold training thành một Kaggle Dataset, Mặc định chỉ in kế hoạch, không chạy Docker/export/upload: (+7 more)

### Community 23 - "Community 23"
Cohesion: 0.14
Nodes (13): Vận hành, xử lý lỗi và bảo trì, Xử lý lỗi, Thu hồi dung lượng Docker, Chỉ dùng khi cần compact, không chạy sau mọi lượt EDA., Nếu Docker disk đã chuyển ổ, truyền đúng path thực tế:, Chia sẻ code và tái lập, Stage các file đã xem, sau đó rà nội dung:, Quy ước lệnh và bảo toàn (+5 more)

### Community 24 - "Community 24"
Cohesion: 0.15
Nodes (12): Phạm vi, ingestion và quản lý dữ liệu, Retention và dọn dữ liệu, Vị trí và hợp đồng dữ liệu, Scope và nguồn bằng chứng, Tải trên PowerShell host, Hoặc chỉ một category và một loại dữ liệu:, Nhập HDFS batch — PowerShell host, Giới hạn preview một category: (+4 more)

### Community 25 - "Community 25"
Cohesion: 0.18
Nodes (9): artifacts mounted path, infrastructure/spark/conf mounted path, ingestion/manifests mounted path, ingestion/scripts mounted path, notebooks mounted path, outputs mounted path, tests mounted path, hdfs-init Compose service (+1 more)

### Community 26 - "Community 26"
Cohesion: 0.21
Nodes (6): device_topk(), worker(), rank_neural(), rank_tower(), ScoringFeatures, TowerScoringFeatures

### Community 27 - "Community 27"
Cohesion: 0.18
Nodes (10): Kiểm thử nhỏ, Kiểm tra Gold G0–G2 và encoder notebook, Kiểm tra EDA cutoff, Kiểm tra baseline ALS, Scope và ingestion offline — PowerShell host, Smoke HDFS chỉ đọc, EDA Recommendation, Kiểm thử bổ sung pha 4 (+2 more)

### Community 29 - "Community 29"
Cohesion: 0.05
Nodes (11): main(), validate_path(), verify(), BaselineCheck, LocalReaderChecks, SelectionChecks, SparkChecks, rate() (+3 more)

### Community 30 - "Community 30"
Cohesion: 0.20
Nodes (6): JAVA_OPTS, flume-env.sh script, data/spool mounted path, infrastructure/flume/flume.conf mounted path, flume Compose service, flume-init Compose service

### Community 33 - "Community 33"
Cohesion: 0.29
Nodes (6): Đồ án Big Data — hệ thống gợi ý sản phẩm, Bài toán và dữ liệu, Bắt đầu trên máy đã có dữ liệu — PowerShell host, Tài liệu và cấu trúc, Trạng thái hiện tại, Làm việc cùng nhóm

### Community 37 - "Community 37"
Cohesion: 0.06
Nodes (49): evaluate(), fit_fold(), keep(), recommend(), query_population(), query_scores(), run_baseline(), _run_baseline() (+41 more)

### Community 38 - "Community 38"
Cohesion: 0.33
Nodes (5): Kiến trúc và phạm vi triển khai, Hệ thống hiện tại, Mạng, lưu trữ và ingestion, Các tầng dữ liệu, Hướng mở rộng, chưa triển khai

### Community 42 - "Community 42"
Cohesion: 0.40
Nodes (4): build_d2(), feature_arrays(), prefix_features(), save_array()

### Community 44 - "Community 44"
Cohesion: 0.83
Nodes (4): Additional DataNode services and volumes, datanode2 Compose service, datanode3 Compose service, datanodes-extra-init Compose service

### Community 45 - "Community 45"
Cohesion: 0.50
Nodes (3): Thử baseline implicit ALS trên Silver, Chạy notebook, Output và cách đọc

## Knowledge Gaps
- **242 isolated node(s):** `JAVA_OPTS`, `data/staging mounted path`, `data/spool mounted path`, `infrastructure/flume/flume.conf mounted path`, `notebooks mounted path` (+237 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 477 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **21 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `Compose services, ports, mounts and resource limits` connect `Community 21` to `Community 33`, `Community 38`, `Community 7`, `Community 8`, `Community 44`, `Community 45`, `Community 12`, `Community 13`, `Community 14`, `Community 17`, `Community 22`, `Community 23`, `Community 24`, `Community 25`, `Community 27`, `Community 30`?**
  _High betweenness centrality (0.087) - this node is a cross-community bridge._
- **Are the 2 inferred relationships involving `run()` (e.g. with `selection_score()` and `.fold()`) actually correct?**
  _`run()` has 2 INFERRED edges - model-reasoned connections that need verification._
- **What connects `JAVA_OPTS`, `data/staging mounted path`, `data/spool mounted path` to the rest of the system?**
  _242 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Community 0` be split into smaller, more focused modules?**
  _Cohesion score 0.1383399209486166 - nodes in this community are weakly interconnected._
- **Why does `NeuralChecks` connect `Community 4` to `Community 29`?**
  _High betweenness centrality (0.023) - this node is a cross-community bridge._
- **Should `Community 4` be split into smaller, more focused modules?**
  _Cohesion score 0.08080808080808081 - nodes in this community are weakly interconnected._
- **Why does `neural_fixture()` connect `Community 4` to `Community 19`, `Community 29`?**
  _High betweenness centrality (0.017) - this node is a cross-community bridge._