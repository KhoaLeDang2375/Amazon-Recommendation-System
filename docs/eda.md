# Đặc tả và vận hành EDA Recommendation

## 1. Phạm vi và kết quả cần đạt

EDA phân tích reviews và metadata của **All_Beauty, Video_Games, Industrial_and_Scientific** theo [scope chung](../ingestion/manifests/experiment-scope.json), tổng khoảng 7,14 GB JSONL không nén. **Mỗi lượt chạy toàn bộ một category**, không sample các phép đếm/count/distinct/density/coverage. Fixture và notebook cũ 25.000 dòng đầu không đại diện toàn dataset.

EDA trả lời: chất lượng/missing, duplicates/conflicts, rating/price/text, thời gian, sparsity/long tail/history, metadata coverage và tác động các gate/split. Nó không sửa raw, tạo processed/features, tự drop/impute hoặc train model. Quy tắc xử lý chỉ chốt sau khi có bằng chứng.

Implementation dùng chung [eda_recommendation.py](../notebooks/eda_recommendation.py); [notebook 02](../notebooks/02_eda_recommendation.ipynb) là entrypoint cấu hình và xem report. [Notebook 01](../notebooks/01_eda.ipynb) giữ nguyên code người dùng để tham khảo. Kế hoạch EDA cũ đã gộp vào tài liệu này; kết quả chạy thực tế theo ngày nằm trong [history.md](history.md). Phân tích ba job complete ngày 14/10/2026 và kế hoạch chuyển sang ETL/snapshot nằm trong [processing.md](processing.md).

## 2. Chạy an toàn — PowerShell host

Tại Git root project/, Docker/image Spark/HDFS đã sẵn sàng. Không bật batch và notebook đồng thời. Cách chuẩn cho dữ liệu thật:

~~~powershell
& ./tools/eda/Run-Eda.ps1 -Category All_Beauty
~~~

Chỉ khi lượt trước đạt và cần chạy tiếp:

~~~powershell
& ./tools/eda/Run-Eda.ps1 -Category Video_Games
& ./tools/eda/Run-Eda.ps1 -Category Industrial_and_Scientific
~~~

Các dòng trên là **các lượt độc lập**, không yêu cầu chạy cả hai ngay. Wrapper không tự bật HDFS, build/pull image hoặc compact đĩa. Nó:

- Nhận đúng một category, dùng khóa độc quyền và từ chối Spark container đang chạy.
- Kiểm tra HDFS safemode, image và dung lượng ổ host chứa outputs cùng ổ chứa Docker VHDX.
- Dùng container detach với overlay RAM 2 GiB/CPU 2, driver 1 GiB, Spark local[2] mặc định; đọc exit code/log rồi dọn đúng ID trong finally. Có thể truyền -Cores 1 để chỉ dùng một luồng Spark; CPU quota của overlay vẫn tối đa 2.
- Theo dõi free space/VHDX mỗi khoảng 5 giây, append JSONL; khi lỗi/thiếu reserve/ngắt thì dừng và dọn container sở hữu.
- Giữ named volumes, log và report; không tự retry job nặng.

Mặc định -MinFreeGiB 20 là dự phòng khởi điểm. Nếu Docker disk chuyển ổ, truyền đúng path thực tế:

~~~powershell
& ./tools/eda/Run-Eda.ps1 -Category All_Beauty -DockerDiskPath 'D:/Docker/wsl/disk/docker_data.vhdx'
~~~

Path mặc định dưới LOCALAPPDATA/Docker/wsl/disk; không cố định ASUS/C. Guard host và guard giữa pha không phải quota; một action vẫn có thể vượt tài nguyên giữa hai lần đo. Limit RAM không giới hạn đĩa shuffle. Giảm dữ liệu không thay việc quản lý cache/vòng đời.

CLI nâng cao trong container nhận --base/--version/--output/--split-date/--min-free-gb/--cores/--shuffle-partitions; chỉ một --categories được phép. --five-core là tùy chọn nặng, không mặc định. Không còn --cross-category hoặc CLI vá report lịch sử. Direct CLI không có monitor host PowerShell; ưu tiên wrapper cho raw.

## 3. Notebook — Python trong kernel container

Mở Jupyter nhẹ theo [operations.md](operations.md). Cell cấu hình chỉ đọc scope/chọn một category; cell chạy gọi run_eda, sở hữu session mới và dừng Spark trong finally khi hoàn tất/lỗi/SIGTERM. Nếu có SparkSession hoặc SparkContext cũ, dừng/restart kernel trước, không ngầm reuse.

Batch PowerShell mặc định local[2]; direct CLI/notebook vẫn local[1] nếu chưa truyền cores=2/--cores 2. Driver 1 GiB, shuffle 64, input partitions tối đa 32 MiB, UTC/case-sensitive, Spark local dir dưới /tmp. Container Jupyter phải dùng overlay nhẹ (RAM 2 GiB/CPU 2). Cấu hình mới chỉ áp dụng container/job mới; không tăng luồng hoặc restart job đang chạy.

Để xem report có sẵn, điền REPORT_DIRECTORY trong cell đọc báo cáo và chạy riêng cell đó. Không cần chạy cell EDA hoặc tạo Spark. Failed run chỉ hiển thị completed_categories đã ghi; không coi partial report là complete.

## 4. Hợp đồng nguồn, output và truy vết

Input /project/raw/<category_lower>_{reviews,metadata}/v1. Inventory đọc recursively để hỗ trợ batch Flume Video Games; file .tmp/._COPYING_ làm dừng trước scan. Không tự bỏ qua file đang ghi hoặc sửa layout raw.

run_eda validate category/cutoff/reserve trước tạo JVM. Parser đọc text JSONL, kiểm tra object/duplicate JSON keys và native type/field presence trên executor; exact key lặp là lỗi, key khác hoa/thường được giữ. Parsed compact cache được đăng ký trước action, release blocking khi lỗi hoặc kết thúc; cache 5-core cũng có vòng đời riêng. Không collect raw/toPandas toàn dataset; chỉ aggregate tối đa 10.000 dòng, quá giới hạn báo lỗi.

Mỗi lượt tạo outputs/eda/run_<UTC>/, không overwrite:

| File/nhóm | Nội dung |
| --- | --- |
| run.json | status, category/full_category scope, scope/code SHA256, base/version/cutoff/UTC, Spark config, completed_categories, thời điểm kết thúc |
| resources.json | Phase/time, filesystem free, cgroup memory.current/peak/max khi hỗ trợ; min free quan sát được |
| <category>/summary.json, BAO_CAO_EDA.md | Tóm tắt category và link bảng; chỉ xuất sau phân tích category |
| <category>/*.csv, *.png | Aggregate UTF-8 BOM và biểu đồ; không export hàng loạt raw ID/text |
| comparison/categories.csv, category_sparsity.csv | Category đang chạy; không phải global toàn scope |
| failure.txt | Exception khi run failed; giữ bảng đã hoàn thành |
| batch_*.log, batch_*.resources.jsonl | Log/host samples của wrapper; không commit |

Status running/failed/interrupted/complete phải đối chiếu exit code/OOM/log. Forced kill/OOM có thể để running; có summary riêng chưa chắc cả run complete. resources.json là phase snapshots, không peak shuffle liên tục; free space phản ánh cả filesystem/process khác. Cgroup memory peak không tự là RAM của riêng DataFrame.

Trace lỗi: path file + SHA256 dòng UTF-8 **không gồm newline**, không giả là line number/byte offset. Hash/ID ví dụ là pseudonymous, không bảo đảm ẩn danh để công bố. Raw bytes/revision/SHA256 nguồn lấy từ manifest khi có; file-list bytes/modified_ms của inventory giúp đối chiếu scope nhưng không thay checksum nội dung.

Không tạo scratch _work/hashed edge ở đường chạy mới. Global distinct/overlap **not_measured**; không xuất bảng rỗng như overlap=0. So sánh ba report cần cùng source/config/quy ước, ghép aggregate nhỏ; không cộng unique users/items mỗi category để suy global.

## 5. Đặc tả insight và định nghĩa

### 5.1. Tổng quan và đơn vị quan sát

Reviews là một JSON object review; giữ user_id, asin, parent_asin và timestamp. Parent-level item dùng parent_asin; asin là biến thể, review cùng parent chưa là copies. Metadata dự kiến ở cấp parent nhưng phải đo uniqueness trước join. Title/images của review khác title/images sản phẩm, không trộn ý nghĩa.

Ghi file list/bytes/input lines/parsed rows/parse errors, user/asin/parent khác nhau có khóa hợp lệ, fields thực tế/thiếu/ngoài schema, time range và native types. Khóa trim trên bản phân tích; đo nhiều dạng gốc bị gộp qua id_normalization. Không suy records từ -count HDFS/bytes.

### 5.2. Missing, chất lượng và schema

| Dạng | Diễn giải |
| --- | --- |
| Absent / null | Không có field khác field có giá trị null |
| Empty string / whitespace | Không có nội dung text sử dụng được |
| Empty list/dict | Có thể hợp lệ, không tự là lỗi |
| Placeholder như N/A/unknown | Báo riêng, ý nghĩa cần kiểm tra; không tự impute |
| Wrong original type | Sai schema gốc, kể cả khi convertible |
| Convertible / invalid after conversion | Numeric string có thể parse nhưng vẫn ghi type gốc; miền giá trị kiểm tra riêng |

Từng cột có count/rate/mẫu số và role key/label/time/content. Theo nhóm: missing metadata theo category, review text theo rating/năm, tổ hợp key/time thiếu, item title/features/description thiếu đồng thời và co-unavailable. Heatmap co-unavailable còn gồm nội dung không hợp lệ, không đồng nghĩa mọi empty collection là lỗi. Occurrences chia số rows có thể >1; phân biệt số lần xuất hiện với tỷ lệ dòng.

| Trường | Kiểm tra |
| --- | --- |
| user_id/asin/parent_asin | String hợp lệ, empty/whitespace/trim collisions |
| title/text/main_category/store | Native string, usable content và placeholder |
| rating/average_rating | Số hữu hạn trong [1,5]; bool không phải rating |
| price | Parse/domain, missing, âm/0, số vô hạn; tiền tệ/text không tự parse |
| rating_number/helpful_vote | Integer không âm, bool/decimal được nhận diện |
| verified_purchase | Boolean gốc khác string/số convertible |
| timestamp | Integer, seconds/milliseconds, mixed units và bounds UTC |
| features/description/categories | List/element type, flat/nested và depth |
| bought_together | ID list, empty/duplicates/self-link |
| images/videos | Object/list shape, keys và URL format; không tải media |
| details | Dict/key/value types, khác biệt theo domain |

URL ở đây được kiểm tra cấu trúc/prefix theo implementation, không xác thực host hoặc truy cập; field count/presence không tự chứng minh content chất lượng. Review image small_image_url/medium_image_url/large_image_url khác metadata thumb/large/hi_res. Không đổi raw theo schema nguồn mà chưa đối chiếu file thực tế.

### 5.3. Duplicates và metadata conflicts

Phân biệt exact record copies, user–asin–timestamp event, user–parent–timestamp event, repeated user–parent khác thời điểm, metadata parent duplicates và text reuse giữa user/item. Báo groups/rows/excess rows nếu giữ một dòng; cùng pair hoặc cùng text chưa chứng minh copies/gian lận.

Exact duplicate dùng SHA256 JSON canonical: bỏ khác biệt thứ tự dict keys, giữ list order. Không normalize text/whitespace rồi gọi đó là exact raw duplicate. Event check chỉ khi keys/time đã xác minh.

Metadata trùng parent: identical, complementary, conflicting. Nested list/dict là giá trị atomic khi so facts; classification không tự merge thành hồ sơ mới. Singleton/exact copies dùng safe content/price; unresolved phải riêng.

### 5.4. Rating, text và price

Rating distribution count/rate, mean/median/std, bất thường và theo category/năm/verified; user/item averages kèm support. Category của review chỉ gán từ safe metadata, unmatched/missing/unresolved riêng.

Review text: missing/usable, độ dài ký tự và reuse theo user/item/rating/time. Không dùng text của tương tác test như thông tin sẵn có lúc dự đoán chính tương tác đó.

Price ở **cấp item safe**, không nhân theo review: count missing/valid/zero/âm, min/P1/P25/median/P75/P95/P99/max, histogram gốc/log cho giá dương và phân vị theo category. Quantile xấp xỉ accuracy=10000, count/distinct chính xác. Chưa quy đổi tiền tệ, clipping/impute; giá 0/lớn là cần kiểm tra, không tự lỗi. Tập unresolved price được báo riêng.

Price/average_rating/rating_number là metadata crawl snapshot, không có lịch sử hiệu lực trong schema. Review count không bắt buộc bằng rating_number; không suy rằng metadata đã tồn tại ở thời điểm review.

### 5.5. Thời gian

Thử timestamp seconds/milliseconds và mixed units trên bản phân tích, giữ trace gốc. Bounds kiểm tra hiện [1996-01-01, 2023-10-01) UTC; ngoài bounds/chưa xác định báo riêng, không tự xóa raw hoặc dùng split.

Theo tháng/năm: review count, active distinct users/items, first-seen trong scope, rating và missing text/metadata. Kỳ trống có activity 0, mean rating không tự 0; tháng biên có thể chưa đầy đủ. First-seen không phải ngày tạo user/ra mắt item; review time không phải purchase time, không suy timezone user.

History: events/distinct items, span, gaps và timestamp ties, user chỉ trong một ngày/tháng. Gaps=0 gồm ties/copies; không tự đặt thứ tự ties hoặc kết luận bot từ hoạt động dày. Điều tra theo ngày khi cần là phân tích bổ sung, chưa một output mặc định.

### 5.6. Sparsity, long tail và tác động lọc

Trên review có cả user_id/parent_asin hợp lệ: U distinct users, I distinct parents, E distinct pairs. **Density=E/(U×I); sparsity=1−density**. E khác tổng review events; nếu U/I=0 thì undefined/N/A, không gán 0.

Đo distinct neighbor degree/quantiles, user 1/2/3–4/5–9/≥10 items, item một user, repeated pair, tích lũy long tail và tỷ trọng top 1/5/10% items. Top dùng ceil(I×fraction), tie-break parent_asin để giữ đúng số items. Event-weighted long tail khác distinct-pair-weighted, cần nhãn riêng.

Mô phỏng user ≥2/3/5 distinct items một pass: users/items/reviews/pairs còn lại, retention và denominator. 5-core hai phía lặp đến ổn định chỉ khi yêu cầu, báo iteration/impact riêng. Đây là diagnostic, chưa chọn policy hoặc mặc định loại cold-start khỏi evaluation.

### 5.7. Metadata coverage và join

Membership từ **distinct metadata parent keys**, không join raw duplicate keys rồi đếm. Safe metadata singleton/exact copies khác membership. Assert left-join row count bằng review đầu vào; metadata conflicts không tự pick first.

| Chỉ số | Mẫu số |
| --- | --- |
| Review coverage | Review có parent khớp / reviews có parent hợp lệ |
| Item coverage | Distinct review parents khớp / distinct review parents hợp lệ |
| Metadata chưa có review | Distinct metadata parents không xuất hiện trong reviews / distinct metadata parents hợp lệ |

Reviews invalid parent riêng; unmatched, missing category và unresolved riêng. Matched content đo title/features/description/category/store/price/images/details trong hồ sơ safe, theo popularity/category/time khi có output. Một ảnh nhiều URL kích thước vẫn là một ảnh. Brand khai thác từ details cần schema/policy tương ứng, không giả là cột chung đã có.

Coverage lịch sử với snapshot hiện tại không chứng minh content tồn tại tại review time. Không yêu cầu review phải có text/price/metadata để mọi mô hình dùng được; CF-only/content-ready có cohort khác nhau.

### 5.8. Gates, temporal split và chuyển sang processing

processing_gate_impact đo từng gate độc lập và cộng dồn (keys/rating/time/membership/safe/content). Thứ tự gate là diagnostic, không phải pipeline lọc bắt buộc.

temporal_split_diagnostic/cold_start dùng cutoff UTC để đo train/test support, warm/cold user/item và test pairs đã thấy train; missing/invalid time riêng. Cutoff mặc định 2022-01-01 chưa là split model đã chốt. Không có test hợp lệ thì N/A, không coi cold-start rate 0.

Mỗi processing_actions có **phát hiện → count/rate và mẫu số → bằng chứng → bước đề xuất → measured/needs-check**. Quyết định dedup/latest/aggregate, conflict resolve, quarantine, minimum history, positive threshold và split cần xem ảnh hưởng. Đo số user có positive test/candidate hợp lệ và cohort sau rule khi chuẩn bị evaluation; phần này chưa đồng nghĩa ETL/model đã triển khai.

## 6. Đọc bảng và tiêu chí nghiệm thu

| Bảng/nhóm | Insight |
| --- | --- |
| *_sources/overview/quality/missing/error_examples, reviews_ids | Source/rows/schema/native types/presence/trace/IDs |
| missing_patterns/content_patterns/co_unavailable/id_normalization | Missing theo tổ hợp và collision khi trim |
| duplicates/event_examples/metadata_conflict_classes/reused_text | Copies khác repeated interactions/conflicts |
| rating_*/review_text_*/price_* | Valid distribution, support, safe item prices |
| timestamp_*/time_range/activity_*/user_history/interaction_gap* | UTC/units/activity/span/gaps/ties |
| sparsity/*_degree_*/long_tail*/user_threshold_impact/five_core_impact | Distinct pairs/history/retention, optional 5-core |
| metadata_coverage/coverage_*/matched_content | Membership/safe/unmatched/cardinality/content |
| processing_gate_impact/temporal_*/processing_actions | Impact, warm/cold, repeated pair và bước xử lý |
| comparison/categories/category_sparsity | Chỉ category đang chạy; global overlap chưa đo |

Để chốt processing:
- Scope/file list/bytes/code/config nhất quán và status đúng.
- Parse/native-type/missing/empty/placeholder không bị gộp hoặc âm thầm drop.
- Duplicate copies/repeated events/metadata conflicts riêng.
- Density/coverage đúng mẫu số, join không nhân dòng.
- Timestamp/timezone/units được xác minh, temporal leakage được nêu.
- Lọc/history/content gates có retention, không mặc định loại CF-only/cold-start.
- Bản ghi lỗi truy vết được, không export private text/ID hàng loạt.
- Mỗi hành động có bằng chứng; fixture/sample không bị ghi như full raw results.

Count/distinct chính xác có thể tốn shuffle; không bỏ insight để “chạy nhanh” mà không công bố thay đổi scope/phương pháp. Các phép đo chưa có (global overlap, availability URL, duplicate sau normalize text...) ghi chưa đo, không suy ra từ bảng khác.

## 7. Kiểm thử và nguồn tra cứu

Lệnh và mô tả test dùng chung ở [tests/README.md](../tests/README.md). Runtime mock không JVM; adversarial fixture Spark kiểm tra parser/native types, duplicate keys, seconds/milliseconds, copies/complement/conflicts, density, row-count join, coverage, temporal cold-start, optional 5-core rỗng, no persistent RDD và context stop.

Fixture đạt không bảo đảm Video Games/Industrial raw vừa RAM/đĩa. Khi được yêu cầu chạy lại raw: All Beauty trước → đánh giá tài nguyên/report → Video Games → Industrial từng lượt. Có đủ ba report mới chốt bảng so sánh và policy processing.

Nguồn schema để tra cứu: [Data Fields](https://amazon-reviews-2023.github.io/main.html), [Native JSON loading](https://amazon-reviews-2023.github.io/data_loading/jsonl.html). Code/fixture/file thực tế là căn cứ xác minh; không giả mọi dòng tuân schema mô tả. [Bảo trì Docker](operations.md) là thao tác riêng, không compact tự động sau mỗi run.

## 8. Kế hoạch EDA bổ sung trên Silver trước Gold

**Lập và triển khai ngày 18/10/2026; cả ba Silver category đã chạy complete, kết quả ở mục 8.7.** Phần 1–7 mô tả EDA raw đã có; đợt bổ sung chỉ trả lời các câu hỏi còn thiếu để thiết kế Gold cho bốn hướng: content-based, implicit ALS, NCF/NeuMF và DeepFM bổ sung nội dung. TF-IDF hay trung bình item text embeddings là lựa chọn biểu diễn của nhánh content; chưa khóa encoder. Trung bình item representations là cách tạo profile, không tự là một thuật toán thứ năm.

Theo yêu cầu hiện tại, **không làm pilot TF-IDF/encoder/mean pooling/NCF/DeepFM, không đo throughput hoặc RAM/VRAM của mô hình, không tạo embeddings, không train và không sinh Gold**. Chỉ profiling dữ liệu, diagnostic protocol và báo cáo quyết định. Chi phí mô hình/tokenizer còn là chưa đo; không kết luận hệ thống đủ tài nguyên từ kết quả này. Giám sát tài nguyên job EDA vẫn áp dụng để chạy an toàn.

### 8.1. Tái sử dụng kết quả cũ, không chạy lại EDA raw

| Đã có bằng chứng | Nguồn cũ | Cách dùng trong đợt bổ sung |
| --- | --- | --- |
| Parse/schema/native types, keys, timestamp units, metadata uniqueness/coverage | EDA raw và quality/manifest Silver | Kiểm tra hợp đồng input; không parse JSONL hay dedup lại |
| Rating distribution, rating theo verified, review length theo rating/năm | rating_distribution, rating_by_verified, review_text_length, review_text_by_rating_year | Trích dẫn kết quả; không xuất lại histogram toàn bộ tương đương |
| Sparsity, degree, long-tail, threshold retention toàn lịch sử | sparsity, user/item_degree_*, long_tail, user_threshold_impact | Làm bối cảnh; chỉ tính mới trên training portion xác định ở E2 |
| Metadata content presence, price, category/store missing | matched_content, metadata_content_patterns, price_*, missing | Không đo lại missing toàn catalog chỉ để lặp báo cáo; bổ sung chất lượng/độ dài text và coverage ở query/fold |
| History, gaps, ties và cold-start raw ở mốc 2022 | user_history, timestamp_ties, interaction_gaps, temporal_* | Không dùng tỷ lệ raw events thay tỷ lệ first-next queries; tính mới đơn vị query sau Silver |
| Exact dedup, read-back schema/counts, UTC millisecond | Silver quality và handoff ngày 16/10/2026 | Dùng final snapshots; chỉ đối soát counts/schema cần thiết khi mở input |

Mỗi chỉ số mới ghi source_run, population, unit, denominator, policy/fold và exact/approximate/sampled. Khác population hoặc policy phải giải thích; không gọi việc xuất lại bảng cũ là insight mới. Khi nguồn cũ không có trên máy, ghi unavailable, không tạo số thay thế.

### 8.2. Input, protocol thử và phạm vi

- Input là reviews_clean/catalog của ba final Silver runs trong [silver.md, mục 10](silver.md#10-kết-quả-triển-khai-ngày-16102026), đọc đúng run ID và manifest complete; không chọn latest theo mtime. Một category/process, chạy All Beauty trước rồi Video Games và Industrial sau khi lượt trước đạt.
- Reviews dùng user_id, parent_asin, event_time, event_ambiguous, rating, verified_purchase, review_title/review_text, has_review_text và quality_flags khi cần; catalog dùng product_title, features, description, main_category/store và flags khi cần. Chỉ đọc text ở pha text; không mang nested media/extras qua mọi aggregation.
- Thử protocol mốc cố định đang đề xuất ở [processing.md, mục 3](processing.md#3-chia-tập-và-xử-lý-người-ít-tương-tác): validation tại 2021-01-01 UTC, test tại 2022-01-01 UTC, kết thúc 2023-10-01 UTC. Đây là diagnostic chưa chốt. Lịch sử validation chỉ trước 2021; lịch sử test trước 2022 tương ứng kế hoạch refit, không dùng dữ liệu trong cửa sổ test.
- Tối đa một query/user/category/window; target là tập distinct parents tại timestamp đầu tiên trong window. History có event_time nhỏ hơn prediction_time. Ties không bị sắp thứ tự bằng hash; target ngoài candidates không được đổi sang event muộn hơn.
- Candidates tham chiếu: distinct items quan sát trong training portion cùng category, có repeat. Chưa mở toàn catalog hoặc ghép category. Snapshot metadata không có effective time: content/DeepFM phải công bố giả định catalog snapshot, không suy thành metadata lịch sử.
- So hai policy dữ liệu hữu hạn: A=mọi review interaction; B=chỉ verified_purchase=true ở cả history lẫn target search. B định nghĩa nhiệm vụ khác nên có query/target thay đổi; không gọi chênh lệch là metric mô hình. Dùng query của A làm tập tham chiếu để đếm user/query mất ở B, đồng thời báo tập user không có target ở A/B. Rating không lọc target; chưa chọn policy chính trước khi có diagnostic.

### 8.3. Các bước đo và quyết định cần rút ra

| Bước | Phép đo mới, phương pháp | Ý nghĩa và quyết định trước Gold | Output aggregate dự kiến |
| --- | --- | --- | --- |
| **E0 — Khóa nguồn và bằng chứng** | Kiểm tra manifest complete, schema/cột cần đọc, counts đối soát với Silver; lập bảng chỉ số cũ được tái sử dụng và thiếu hụt | Bảo đảm đo đúng snapshot; không biến EDA bổ sung thành làm sạch Silver lần hai | input_contract.json, evidence_reuse.csv |
| **E1 — Profile text sản phẩm** | Đo riêng title/features/description và hai biểu diễn tạm title-only, title+features+description: ký tự, UTF-8 bytes, số phần tử list, P50/P90/P95/P99/max, tỷ lệ vượt các ngưỡng ký tự/bytes cấu hình trước. Đếm HTML/URL/control chars, chuỗi không có chữ/số, dòng lặp giữa các phần tử/các trường, nhóm cực dài; đây là flags cần xem, không tự là lỗi | Chọn trường ghép, dấu phân cách, rule làm sạch tối thiểu, fallback và ngưỡng cần xem xét. Không tự xóa số, tên model hay phủ định; không tự loại item vì text ngắn | item_text_profile.csv, text_flags.csv, text_extremes.csv |
| **E1b — Rà ngữ nghĩa trên mẫu giới hạn** | Mẫu có seed/hash ổn định, đề xuất tối đa 200 items/category: tối đa 100 đại diện, 50 đuôi dài và 50 bị flag; lấy bù trong pool hữu hạn nếu thiếu, dedup và ghi strata. Rà ngôn ngữ, text thực sự mô tả sản phẩm hay boilerplate, title có đủ ngữ cảnh, có phá hỏng nghĩa nếu chuẩn hóa không | Chọn rule cần thiết cho text; mixed language/unknown riêng. Mẫu theo flags không đại diện tỷ lệ ngôn ngữ toàn dataset, không ngoại suy không trọng số | text_sample_manifest.json, text_review_notes.md |
| **E2 — Lịch sử và first-next queries sau Silver** | Với từng fold/policy A/B: tổng user, users có history, có target, có cả hai; bins 0/1/2/3–4/≥5 cho history events, distinct items và timestamp groups. Đếm first-next ties, target repeat, ambiguous groups liên quan history/target. Đếm queries có/không có review text trong history mà không ghép text dài | Xác định số mẫu cá nhân hóa thực tế, fallback, cách xử lý ties và tác động verified-only. Không suy history từ degree toàn dataset | query_population.csv, history_bins.csv, target_ties_repeat.csv, verification_policy_impact.csv |
| **E3 — Eligibility theo candidates và dữ liệu đầu vào** | Trên queries E2: warm/cold user/item; ties full/partial/outside candidates; số targets in/out. Trên history/candidates: title-only/content-expanded usable, tỷ lệ user có 0/1/≥2 distinct history items có content, tỷ lệ history items có content và review text usable. Báo cả query count và target count với mẫu số riêng | Chọn candidate protocol và content fallback; biết mô hình có đủ đầu vào ở đâu. Chưa gọi warm-ID/content availability là model score coverage vì chưa fit model | candidate_eligibility.csv, feature_eligibility.csv |
| **E4 — Tín hiệu train cho ALS/NCF/DeepFM** | Chỉ trên train từng fold/policy: distinct pairs và degree bins user/item, tỷ lệ query có history/user/item nằm trong vùng degree thấp. Đếm repeated pairs cần aggregate, rating null/conflicts trong từng pair và tỷ lệ coverage main_category/store trên train candidates, cardinality các fields dự định dùng cho DeepFM | Chọn pair aggregation binary làm baseline hay cần biến thể; xác định sparse fields thực sự hữu ích. Không fit ALS/NCF và không khẳng định DL đủ dữ liệu chỉ vì tổng rows lớn | train_support.csv, pair_aggregation_diagnostic.csv, deepfm_field_coverage.csv |
| **E5 — Khả năng lấy mẫu âm, không sinh bảng âm** | Với từng training user, tính eligible unseen items = số candidates train trừ distinct observed items; đếm user thiếu 1/4/10 negatives không lặp. Với quy ước một positive/distinct training pair, dự toán số rows P×(1+n) cho n=1/4/10, ghi rõ nếu sampler dùng replacement. Chỉ dùng train, không loại negatives bằng nhãn tương lai | Chọn cách lấy mẫu và giới hạn phình số rows cho NCF/DeepFM; giữ positives + sampler config có thể đủ, chưa cần materialize negatives | negative_sampling_feasibility.csv |
| **E6 — Báo cáo quyết định** | Ghép aggregate theo category/fold/policy; mỗi đề xuất kèm bằng chứng, mẫu số và phần chưa đo; kiểm tra các đẳng thức population/target counts | Chốt hợp đồng Gold hoặc nêu chính xác điểm còn thiếu; không chọn bằng model test metrics | BAO_CAO_EDA_BO_SUNG.md, gold_decisions.csv, run.json |

E1 thống kê trên catalog không nhân theo số reviews; E3 mới đo tác động theo population dùng cho mô hình. Quantiles có thể dùng percentile_approx với accuracy ghi trong run. Counts/denominators và eligibility chạy toàn category. Không đếm ký tự hoặc whitespace words rồi gọi đó là token; tokenization/giới hạn token chính thức để giai đoạn chọn encoder sau.

Review text ở đợt này chỉ bổ sung availability của lịch sử trong E2/E3 và rà nội dung trên mẫu khi cần, không lặp review_text_length/rating histograms cũ, không mã hóa toàn bộ reviews, không sentiment/aspect extraction. Kết quả không ép review text thành feature bắt buộc cho cả bốn mô hình.

Với mean item profile, E3 đo số item có content và độ phủ history trước mốc, không tính vector profile. Với DeepFM, price/average_rating/rating_number crawl snapshot chưa được đưa làm feature lịch sử; không cần profiling lại chúng. Verified_purchase/rating của target không được đưa vào inputs cho candidate scoring; chúng chỉ có thể dùng định nghĩa cohort/label policy hoặc đặc trưng quá khứ.

### 8.4. Trình tự triển khai

1. **Thiết kế:** xác nhận bảng tái sử dụng E0, definitions/mẫu số, cấu hình fold/policy và flags text; lưu kế hoạch tại đây. Không thay notebook 01 hoặc sửa output EDA cũ.
2. **Code:** entrypoint riêng notebooks/eda_model_readiness.py tái sử dụng helpers writer/session; không sửa raw parser để nhận Parquet bằng nhánh ngầm. Notebook 03_eda_model_readiness.ipynb chỉ cấu hình/xem report; batch và notebook dùng chung implementation.
3. **Runner:** tools/eda/Run-ModelReadiness.ps1 dùng nguyên tắc shared Spark exclusion/guard/cleanup của runner hiện có, input explicit Silver run, một category/lượt. Lệnh hiện hành ở mục 8.6; không chạy Run-Eda.ps1 cũ để thay thế vì nó đọc raw.
4. **Fixture trước:** kiểm tra E1–E5 bằng Spark DataFrames và Parquet nhỏ trong thư mục tạm container; không đọc dataset production. Chỉ chạy full khi các kiểm tra mục 8.5 đạt.
5. **Full:** All Beauty → kiểm tra complete/counts/report → Video Games → Industrial. E1 text có thể chạy trước E2–E5 sau khi E0 đạt; trong cùng process vẫn xử lý tuần tự, release cache giữa pha. Không chạy song song ba category.
6. **Chốt:** tổng hợp ba report, cập nhật các quyết định ở processing.md bằng bằng chứng mới; chỉ khi ấy thiết kế/triển khai Gold. Không tự đổi Silver hoặc loại records trong đợt EDA này.

Output host outputs/eda_model_readiness/run_<UTC>/<category>/ gồm các aggregate ở E0–E6; run.json ghi code/helper hashes, Silver run/manifest fingerprint, cấu hình fold/policy/UTC/Spark, quantile method và complete/failed; sample seed ở text_sample_manifest.json. Chỉ xuất sample text tối thiểu phục vụ rà soát cục bộ, có giới hạn và không commit; aggregate báo cáo dùng hash/reference, không dump text/IDs hàng loạt. Các bảng trung gian theo user/query chỉ sống trong Spark/scratch có cleanup, chưa publish thành Gold. Raw/Silver bất biến, không ghi analytics vào thư mục dataset nguồn.

Tận dụng runner nhẹ hiện có: một Spark job, column pruning, cache hẹp chỉ khi tái sử dụng, unpersist/stop trong finally; không collect/toPandas toàn bộ, không user×catalog cross join, không item×item similarity, không corpus text nối dài. Ngoài top samples hữu hạn chỉ thu aggregate có giới hạn. Phép tính sample hash có thể cần scan text, không đồng nghĩa sample chỉ đọc ít bytes; phải tách thời gian scan và số mẫu xuất. Không tải model/ngôn ngữ package mới ở đợt này; E1b rà trên mẫu.

### 8.5. Kiểm tra logic và điều kiện hoàn thành

- Fixture có missing/empty arrays, Unicode/control chars, HTML/URL hợp lệ và boilerplate, text dài, lặp trong features; flags không tự sửa Silver. Quantiles và counts có mẫu số rõ; fixture rỗng cho N/A thay vì tỷ lệ 0 giả.
- Timeline có history=0/1, user chỉ trong train, target rating thấp, verified false/null, repeated item, ties nhiều parents, cùng parent/time nhiều versions và item ngoài candidates. E2 lấy first-next, không latest; policy B lọc trước target search và công bố target đổi.
- Với mọi query: history_time < prediction_time và prediction_time ≤ target_time < window_end. Candidate membership lấy từ train. Thay text/rating của target không đổi availability/features của history; thay verification target có thể đổi policy B theo định nghĩa, không bị nhầm là leakage feature.
- Đối soát warm/cold và history bins với tổng query; full/partial/outside ties phân hoạch query có target. In-candidate targets + outside targets = tổng distinct query–target pairs. Báo NO_HISTORY/NO_TARGET/content-missing như flags có thể giao nhau, không cộng thẳng thành exclusions rời nhau.
- Negative feasibility có fixture user quan sát hết catalog, thiếu negatives, repeat pairs và empty catalog; cardinality dùng distinct items. Mẫu train giả định ở E5 không được nhầm thành số first-next queries E2.
- Nghiệm thu: ba report complete; từng quyết định Gold có evidence và scope; các lựa chọn chưa chốt được ghi rõ, không dùng tỷ lệ raw cũ làm kết quả Silver mới. Không yêu cầu model metrics/embeddings hoặc pilot tài nguyên để hoàn thành đợt EDA bổ sung.

**Đầu ra cuối cùng cần chốt:** interaction policy; split/history/target/ties/repeat; candidate population và cohort denominators; pair aggregation/mappings; trường text/cleaning/fallback; fields DeepFM; negative-sampling policy. Encoder cụ thể, token cap, chiều embeddings, batch size, cấu hình neural network và benchmark mô hình thuộc giai đoạn sau, không phải điều kiện chặn đợt này.

### 8.6. Entrypoint đã triển khai

Module [eda_model_readiness.py](../notebooks/eda_model_readiness.py), [notebook 03](../notebooks/03_eda_model_readiness.ipynb) và [Run-ModelReadiness.ps1](../tools/eda/Run-ModelReadiness.ps1) đọc Silver; EDA raw cũ được giữ nguyên. Runner dùng chung outputs/eda/.eda.lock với EDA/Silver, kiểm tra selected Silver fsck, disk reserve, exit/OOM và manifest completion; chỉ dọn container sở hữu. Python tái sử dụng Report/RuntimeMonitor/create_spark, kiểm tra schema/counts với handoff và manifest không đổi trước/sau. Mẫu text chỉ chứa excerpts giới hạn và hash, cần rà ngữ nghĩa cục bộ trước chốt cleaning policy.

PowerShell host tại project/, HDFS ba DataNode/safemode OFF và Spark image hiện có:

~~~powershell
& ./tools/eda/Run-ModelReadiness.ps1 -Category All_Beauty -SilverRun run_20261016T021632209982Z
# Chỉ sau khi lượt trước complete và đã kiểm tra báo cáo:
& ./tools/eda/Run-ModelReadiness.ps1 -Category Video_Games -SilverRun run_20261016T022209697481Z
& ./tools/eda/Run-ModelReadiness.ps1 -Category Industrial_and_Scientific -SilverRun run_20261016T053543469765Z
~~~

Nếu môi trường sandbox thay LOCALAPPDATA hoặc Docker chuyển ổ, truyền -DockerDiskPath là absolute VHDX thực đã xác minh. Không thay đường dẫn bằng ổ ảo Linux. Run failures giữ report có status failed; không coi các CSV riêng là complete. Giới hạn đo: ký tự/bytes không phải token, kiểm tra text bằng regex chỉ là flags, sampling có strata giao nhau và tối đa 200 items chứ không bảo đảm 200 distinct khi dataset nhỏ.

Kiểm tra fixture Spark và host mocks (chưa chạy batch Spark thật khác cùng lúc):

~~~powershell
& ./tests/test_model_readiness_runtime.ps1
docker compose -f compose.yaml -f compose.datanodes.yaml -f compose.eda-light.yaml run --rm --pull never --no-deps spark-notebook python -W ignore::ResourceWarning /workspace/tests/test_model_readiness.py
~~~

Fixture dùng Spark DataFrames nhỏ trong container, không đọc/ghi dataset production. Bao gồm first-next/ties/repeat/partial-outside candidates, rating target không ảnh hưởng history, verified filtering, text flags/excerpt cap, empty population và user đã thấy hết candidates. Mocks host chạy trong TestRepo riêng, kiểm tra shared lock, active job, fsck, reserve, OOM, manifest, native stderr và cleanup failures. Không có model pilot trong các kiểm tra này.

### 8.7. Kết quả full Silver ngày 18/10/2026

Ba runs/batches complete, cùng code/helper và protocol, mỗi lượt một category. Input contracts khớp handoff Silver: tổng 10.395.239 reviews và 677.423 catalog rows theo category; không phải global distinct items. Không chạy lại EDA raw, không encode/train hay tạo Gold.

| Category | Readiness run | Test queries A | Có history | Có history + toàn bộ targets trong candidates |
| --- | --- | ---: | ---: | ---: |
| All Beauty | run_20261018T083227913697Z | 71.123 | 5.114 | 2.877 |
| Video Games | run_20261018T083634881633Z | 434.097 | 95.401 | 70.794 |
| Industrial and Scientific | run_20261018T084441932113Z | 805.430 | 179.450 | 144.444 |

A là mọi review interaction; test tại 2022-01-01 UTC, first-next trước 2023-10-01. Đây là query counts, không phải raw test events hoặc model metrics. Population/candidate eligibility, ba history bins, content eligibility, ties và nominal negative rows đã đối soát; model factor/score coverage chưa đo. First-next ties nhiều distinct parents=0 ở các folds/policies hiện tại, nhưng fixture vẫn kiểm tra trường hợp có ties. Repeat target pairs test A=20/122/307, nên giữ repeat policy rõ ràng.

Text expanded P99 ký tự=2.606/6.425/3.282, max=13.625/93.999/21.414; số items >8.192 ký tự=3/570/20. Đã rà 197/189/190 excerpts theo category, không suy tỷ lệ nhiễu/ngôn ngữ toàn corpus. Có HTML, lặp nội dung, ví dụ title–description không khớp và dấu hiệu mojibake ở thông số kỹ thuật. Title-only là đề xuất baseline; expanded cần feature-cleaning nhẹ và fallback, không tự loại item hoặc sửa Silver.

Verified-only ở test làm mất 1.022/11.971/10.358 queries có history+target của A; B có thể đổi đáp án và candidates. Chọn A để đối chiếu next-interaction hoặc B cho phạm vi verified-review theo mục tiêu đề tài, không trộn hai protocol thành một bảng metric. NCF/DeepFM nên giữ positives + sampler config: với 4 negatives, train trước 2022 của Video Games/Industrial A lần lượt thành 19.923.335/20.356.575 nominal rows nếu materialize. Chưa sinh negatives.

Báo cáo local [BAO_CAO_TONG_HOP.md](../outputs/eda_model_readiness/comparison_20261018_final/BAO_CAO_TONG_HOP.md) có A/B cho cả validation/test, text/negative tables, ghi chú rà mẫu và đề xuất Gold. [summarize_model_readiness.py](../tools/eda/summarize_model_readiness.py) chỉ tổng hợp các run/batch complete được chỉ định, kiểm tra population conservation và cùng code/protocol, không chọn latest tự động. Outputs ignored không đi cùng Git clone.

Fixture cuối đạt 3 tests; host mocks đạt PowerShell 7/Windows PowerShell 5.1. SHA256 của EDA raw, notebook 01/02 và Silver producer code hiện hành giữ nguyên. Hai attempt All Beauty trước khi chạy thành công được giữ: run_20261018T082625136366Z failed do guard so thứ tự cột quá chặt; run_20261018T082804538472Z bị dừng có chủ đích do AQE chờ nested cache, exit 137 nhưng OOM=false. Đã sửa guard so theo tên/type/nullability/metadata và materialize cache hẹp tuần tự; không tăng RAM/CPU hay thay source. Ba runs được chọn sau sửa đều complete, không OOM. Bằng chứng ở outputs/validation/model-readiness-20261018/ và host/container logs từng run.
