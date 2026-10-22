# Silver: code, lưu trữ và vận hành

**Cập nhật: 16/10/2026.** Code/overlay/runner Silver đã triển khai; fixture HDFS và host fault tests đã đạt. Cả ba category đã publish sau read-back; kết quả và đường dẫn ở mục 10. [processing.md](processing.md) giữ giải thích bài toán và Gold; tài liệu này là hợp đồng và hướng dẫn Silver.

## 1. Phạm vi và kết quả cần có

Input là reviews và metadata của All Beauty, Video Games, Industrial and Scientific tại raw v1. Output là **Parquet Snappy trên HDFS**, có schema, kiểm tra chất lượng, audit và manifest. Giữ mọi tương tác có user/parent/time hợp lệ; rating/text/price lỗi là null/flags theo vai trò, không lọc rating ≥4, verified-only, history≥5 hoặc sentiment.

Silver cung cấp bốn dataset và report nhỏ:

| Dataset | Một dòng đại diện cho | Mục đích |
| --- | --- | --- |
| reviews_clean | Một record review khác nội dung sau exact dedup, trong category | Nguồn history/rating/text cho Gold |
| catalog | Một parent item duy nhất trong category | Metadata để join và tạo item features |
| quarantine | Một dòng raw không parse được hoặc không đạt core policy | Giữ lý do và trace để điều tra |
| duplicate_audit | Một nhóm canonical record xuất hiện >1 lần, phân biệt reviews/metadata | occurrences, removed_count và trace bản đại diện |
| quality.json + manifest.json | Aggregate và hợp đồng của run | Counts/schema/input lineage, trạng thái hoàn tất và vị trí output |

Quarantine là dữ liệu bị cách ly, không phải input hợp lệ cho Gold. Reviews và catalog giữ riêng; chỉ join để kiểm tra coverage/cardinality hoặc tạo view khi cần. Không ghi một bảng rộng lặp toàn metadata theo từng review. Chưa tạo embeddings, sentiment model, train/test split, user/item Int mapping hoặc Top-K ở tầng này.

## 2. Code và phần tái sử dụng

| File | Trách nhiệm | Lý do |
| --- | --- | --- |
| [processing/silver.py](../processing/silver.py) | CLI một category; schema/rules/parser; Spark dedup/catalog/quality; HDFS write/read-back/finalize | Một module, chưa cần framework ETL |
| [Run-Silver.ps1](../tools/silver/Run-Silver.ps1) | Preflight, khóa chạy, container ownership, log/resource monitor/exit/cleanup | Entry point PowerShell host |
| [compose.silver-light.yaml](../compose.silver-light.yaml) | Mount processing/ và source manifest read-only; RAM 2 GiB/CPU 2, driver 1 GiB | Giữ base/EDA runtime hiện có |
| [test_silver.py](../tests/test_silver.py) | Pure-rule checks và fixture Spark/HDFS | Counts, fields, read-back và fail-before-publish |
| [test_silver_runtime.ps1](../tests/test_silver_runtime.ps1) | Mock wrapper fault tests trong TestRepo riêng dưới outputs/validation | Native stderr, cleanup, lock và guards; không raw |
| .gitignore, docs | Ignore outputs/processing; cập nhật hướng dẫn và trạng thái | Không commit payload/logs/cache |

Schema constants và `SILVER_POLICY_VERSION` đặt trong silver.py, ghi fingerprint vào manifest; chưa thêm YAML/config library chỉ để chứa các quy tắc cố định. Các nút vận hành như category/cores/partitions/reserve là CLI args được ghi lại. Chỉ thêm file helper khi module có phần dùng chung thực sự.

Tái sử dụng [scope loader](../ingestion/scripts/dataset_scope.py), image PySpark **3.5.6**/Python **3.11** và HDFS **3.5.0** hiện có. Reuse các helper thuần `canonical`, `state`, `numeric` từ [EDA](../notebooks/eda_recommendation.py) nếu import không tạo session/action; pin SHA256 file helper vào lineage và test tính nhất quán. Timestamp output dùng integer milliseconds trực tiếp; không lấy giá trị float seconds đã làm tròn để dựng thời gian. Không dùng `parse_partition` EDA làm Silver vì compact output của nó thiếu text/catalog đầy đủ.

Wrapper áp dụng pattern đã được kiểm thử trong [Run-Eda.ps1](../tools/eda/Run-Eda.ps1): thu stderr thành text, không để lỗi thu log bỏ qua cleanup, dọn đúng container ID sở hữu. Không gọi Run-Eda để chạy Silver hoặc refactor toàn bộ EDA runner trong đợt này. Tái sử dụng khóa host `outputs/eda/.eda.lock` hiện có và kiểm tra service spark-notebook đang hoạt động để hai wrapper không chạy đồng thời; không tạo khóa Silver riêng rồi coi là đã khóa chéo. Direct CLI/notebook phải tuân thủ quy tắc không chạy song song; kiểm tra container không là khóa phân tán cho mọi cách khởi động.

## 3. Hợp đồng dữ liệu v1

Tên/kiểu dưới đây được khai báo trong module; schema và SHA256 lưu trong manifest. Kiểm tra non-null bằng dữ liệu thực sau read-back, không yêu cầu cờ nullable của Spark giống hệt trước write vì Spark đọc Parquet có thể chuyển các cột thành nullable. [Spark Parquet](https://spark.apache.org/docs/3.5.6/sql-data-sources-parquet.html).

### 3.1. reviews_clean

| Nhóm | Cột và kiểu | Quy tắc |
| --- | --- | --- |
| Identity | source_category String, user_id String, parent_asin String, asin String nullable | User/parent là native string usable; trim, giữ case; asin lỗi có flag nhưng không mất parent-level interaction |
| Time | timestamp_raw Long, event_time Timestamp | Native integer milliseconds; UTC; bounds [1996-01-01,2023-10-01) cho nguồn này; round-trip chính xác |
| Feedback | rating Double nullable, verified_purchase Boolean nullable, helpful_vote Long nullable | Rating hữu hạn [1,5], bool không phải rating; helpful nonnegative integer; giữ native-type/convertible flags |
| Text | review_title String nullable, review_text String nullable, has_review_text Boolean | Trim nhẹ; giữ từ phủ định, chữ hoa/thường/nội dung; placeholder/empty khác null ở field_states |
| Source phụ | images_json String nullable, extras_json String nullable | Giữ cấu trúc khác schema ở dạng JSON khi cần, không tải media hoặc bỏ key chỉ vì khác case |
| Chất lượng | quality_flags Array[String], field_states Map[String,String], event_ambiguous Boolean | Không tạo sentiment label; ambiguous chỉ là flag nhiều versions cùng user/parent/time |
| Lineage | record_hash String, source_path String, line_hash String | Canonical record hash trước normalize; line hash UTF-8 không gồm newline, không giả line number |

Chuỗi số convertible ở trường phụ được chuẩn hóa khi policy cho phép và luôn gắn flag native type. Timestamp khác unit/native type kỳ vọng vào quarantine, không đoán từ 12/13 digits. ID trim collisions >0 làm fail gate để xem xét, không tự gộp danh tính. Mỗi record chỉ có một outcome clean/quarantine/copy_removed, dù có nhiều flags.

quality_flags còn có marker chuẩn hóa/cấu trúc như trimmed hoặc categories:category_depth_1, không phải mọi flag đều là lỗi làm dòng không hợp lệ. Không cộng số flags thành số dòng lỗi hoặc lọc tất cả dòng có flag. Hard rejects được đối soát riêng trong quarantine/reason codes.

### 3.2. catalog

| Nhóm | Cột | Quy tắc |
| --- | --- | --- |
| Key | source_category, parent_asin | Unique trong category; không giả global unique |
| Nội dung | product_title String, features/description Array[String], main_category/store String nullable | Giữ source_category khác taxonomy; field thiếu/placeholder có states |
| Numeric snapshot | price Double nullable + price_status, average_rating Double nullable, rating_number Long nullable | Finite/domain validation; zero khác missing; không quy đổi tiền tệ, clipping hoặc impute mặc định |
| Nested/extras | categories_json, images_json, videos_json, details_json, bought_together_json, extras_json | Giữ cấu trúc nguồn, kể cả key khác hoa/thường |
| Flags/trace | quality_flags, field_states, record_hash, source_path, line_hash | Như reviews; giữ metadata item chưa có review |

V1 chỉ lưu các thành phần text đã chuẩn hóa. `item_title_text`/`item_text` trong processing.md là biểu diễn có quy tắc xác định: có thể tạo bằng view/Gold từ product_title/features/description, **không cần lưu lặp chuỗi text ghép lớn trong Silver**. Điều này giữ được lựa chọn title-only/full-content mà giảm copy.

Metadata exact copies cũng dedup trong dataset riêng. Nếu sau đó còn >1 record khác nội dung cho cùng parent, xuất diagnostic hash/fields, fail run trước khi publish catalog; chưa tự merge hoặc pick first. Đếm parse/core quarantine, copies và catalog riêng cho metadata. Batch hiện tại duplicate parent groups=0 nên expected catalog bằng parsed metadata.

### 3.3. quarantine, audit và report

- Quarantine: dataset_kind/category/source_path/line_hash/record_hash nullable, primary_reason, all_reason_codes và raw_line cho dòng bị cách ly. Raw payload nằm trong HDFS quarantine; report host chỉ aggregate/trace hash, không chép nguyên reviews hoặc IDs vào log.
- Audit: dataset_kind/category/record_hash/occurrences/removed_count/source_path/line_hash đại diện. `removed_count=occurrences−1`; không xuất mọi bản sao thành thêm một bộ raw.
- Report: input/parsed/quarantine/copies/clean counts; flags theo cột; keys/users/items/pairs; ID collisions; timestamp units/bounds; catalog membership/cardinality; file count/bytes/schema; resources và status. Mẫu diagnostic nhỏ có giới hạn, không collect raw toàn bộ.
- Dataset không có dòng vẫn có schema đọc lại được; fixture phải kiểm tra trường hợp quarantine/audit rỗng, không coi một thư mục không có file/schema là bảng hợp lệ.

## 4. Luồng chạy và publish an toàn

1. **Preflight:** scope hợp lệ, HDFS selected inputs finalized/readable và safemode OFF, image có sẵn, output không trỏ vào raw, chưa có Spark job khác, đủ reserve. Không tự bật daemon/HDFS, build/pull image hoặc nhập lại nguồn.
2. **Parse reviews một lượt:** Spark bật `spark.sql.caseSensitive=true`, `spark.sql.session.timeZone=UTC`; mapPartitions với generator, tính full-record hash trước normalize, chỉ giữ typed columns cần thiết trong DISK_ONLY cache đã đăng ký cleanup. Dòng hợp lệ không giữ thêm full raw JSON để tránh nhân bản payload; dòng quarantine giữ raw_line.
3. **Dedup và write reviews:** group/hash có counts và trace xác định, giữ event khác nội dung; đánh dấu ambiguous parent-time groups. Ghi reviews/audit/quarantine vào staging; release cache khi không còn cần.
4. **Parse metadata, dedup và tạo catalog:** cùng policy tracing, unique-parent gate. Ghi staging và release cache; không giữ cache reviews+metadata đầy đủ suốt job nếu read-back hẹp đáp ứng.
5. **Read-back/quality:** đọc output Parquet từ HDFS, aggregate exact counts/keys/hash; membership dùng distinct catalog keys; assert left join không nhân dòng. Kiểm tra optional flags và timestamp millisecond round-trip.
6. **Finalize:** quality gates đạt mới ghi manifest complete, sau đó rename cả staging run sang final trong cùng HDFS namespace. `_SUCCESS` của một dataset Spark không thay run manifest; lỗi giữa các bảng chưa phải run complete.
7. **Cleanup:** stop session trong finally, unpersist blocking, thu logs/resources, dọn đúng container sở hữu. Failure giữ staging và report reason; Gold chỉ đọc final run có manifest complete.

Luồng này tạo một snapshot full-category bất biến, chưa phải incremental merge. Rerun tạo run ID mới, không append vào run cũ. Manifest ghi input inventory/manifest checksums đã có, code/helper SHA256, policy/schema versions, Spark/runtime/UTC settings, output counts/file bytes, validation results, started/finished và run state. Inventory không được gọi là checksum mới của toàn payload nếu chưa hash lại nguồn.

## 5. Bố trí lưu trữ

### 5.1. HDFS và host

~~~text
hdfs://namenode:8020/project/raw/<category_lower>_{reviews,metadata}/v1
    (Bronze hiện có, chỉ đọc)

hdfs://namenode:8020/project/processed/silver/<category_lower>/v1/
    _staging/<run_id>/
        reviews_clean/    catalog/    quarantine/    duplicate_audit/
        quality.json      manifest.json
    <run_id>/
        reviews_clean/    catalog/    quarantine/    duplicate_audit/
        quality.json      manifest.json

Host project/outputs/processing/silver/<run_id>/
    batch.log    resources.jsonl    quality.json    run.json

Container /tmp/silver-spark/<run_id>/
    cache + shuffle tạm (không mount vào data source)
~~~

Staging/final cùng filesystem, finalize dùng rename thay vì copy nguyên Silver. Gold luôn ghi rõ silver_run_id, không tự đọc `_staging` hoặc chọn run mới nhất từ mtime. Không cần con trỏ latest ở v1. HDFS file bytes còn có checksum/metadata overhead; một file nhỏ không chiếm đủ nguyên block như một allocation cố định. [HDFS Architecture 3.5.0](https://hadoop.apache.org/docs/r3.5.0/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html).

Category/run tách bằng directory, chưa `partitionBy(user_id)`/`parent_asin`/ngày. Gold thường đọc toàn history category; partition theo năm ở Silver chỉ thêm nếu benchmark chứng minh có lợi. Dùng shuffle 64 làm khởi điểm, output coalesce/repartition điều chỉnh riêng sau đo; không biến 64 shuffle partitions thành yêu cầu 64 output files. Target 64–128 MiB/file để thử, catalog/quarantine nhỏ có thể ít file hơn; không repartition(1) reviews lớn để ép một file.

### 5.2. Không đồng nhất ba DataNode với ba ổ vật lý

Cấu hình hiện có: ba DataNode cùng host, named volumes trong Docker storage, replication=1. Tổng logical HDFS bytes gần bằng một bản dữ liệu cộng overhead khi replication=1; không tự nhân ba vì có ba DataNode. Các volume và container scratch có thể cùng nằm trong Docker VHDX trên một ổ host: **không cộng số free từng DataNode như ba nguồn dung lượng độc lập**. Giữ identity/volumes/replication hiện có.

Raw local staging và raw HDFS là hai bản đã tồn tại; ngân sách thêm Silver không tính lại chúng thành dung lượng cần cấp mới. Xóa file Linux không bảo đảm VHDX trên Windows thu nhỏ ngay; không dùng compact như một bước bắt buộc của pipeline.

## 6. Dự toán dung lượng và điều kiện dừng

### 6.1. Phần đã biết và phần chưa đo

Raw theo manifest/EDA: 7.136.947.646 bytes = khoảng **6,647 GiB** cho cả reviews+metadata. Ba run thật có tổng **4,14 GiB dataset Silver** chứa text, flags và trace; số đo từng category ở mục 10. Tỷ lệ này chỉ áp dụng cho snapshot/schema hiện tại.

Snapshot 15/10/2026 khoảng 08:42 giờ Việt Nam: C còn khoảng **218,73 GiB**, Docker khi đó chưa chạy. Ngày 16/10/2026 khi triển khai, Docker/HDFS đã hoạt động, ba DataNode live, C còn khoảng 217,49 GiB trước run; wrapper đo live VHDX/free/fsck từng nguồn. Không lấy free filesystem ảo Linux hoặc số host cũ làm dung lượng bảo đảm cho lần chạy sau.

### 6.2. Ngân sách thử, không phải dự đoán kích thước nén

Gọi R là raw bytes của một category đổi sang GiB. Đề xuất đặt **B=2R** làm ngân sách toàn bộ output Silver category, **T=4R** cho cache/shuffle/scratch, và **M=20 GiB** dự phòng còn trống. Các hệ số chỉ là allocation khởi điểm có kiểm tra; dữ liệu/overhead thực có thể vượt, không được coi là quota cứng hoặc giới hạn toán học.

| Category | R raw, GiB | B output=2R | T scratch=4R | Free host cần trước run: B+T+20 |
| --- | ---: | ---: | ---: | ---: |
| All Beauty | 0,503 | 1,005 | 2,010 | 23,015 |
| Video Games | 2,906 | 5,812 | 11,624 | 37,436 |
| Industrial and Scientific | 3,238 | 6,476 | 12,953 | 39,429 |

Tổng B của cả ba khoảng **13,294 GiB**. Chạy tuần tự, giữ ba output và chỉ có scratch của category đang chạy: ngân sách tăng footprint cộng reserve ở điểm lớn nhất khoảng **46,247 GiB so với trước khi tạo Silver**. Nếu giữ một bộ complete và chạy lại category lớn nhất, kịch bản cùng baseline khoảng **52,723 GiB**. Đây là cách cộng ngân sách, không phải kết quả benchmark.

Free-needed trước từng run là dung lượng **còn trống lúc đó**; các run cũ/raw đã chiếm đĩa rồi, không cộng lần nữa vào công thức preflight. Nếu HDFS/scratch/host outputs cùng ổ C, dùng tổng B+T+M cho ổ đó. Nếu Docker chuyển ổ khác, đo và cấp budget theo filesystem backing thực của từng phần, không dựa trên ký tự đường dẫn trong container.

### 6.3. Cách kiểm chứng và điều chỉnh

- Trước run: Windows drive free ở nơi chứa Docker VHDX và logs; HDFS df/dfsadmin/selected fsck; Linux df nơi scratch. Wrapper ghi các số và backing path đã xác minh, không đoán VHDX path khi mặc định không tồn tại.
- Monitor host/VHDX mỗi khoảng 5 giây và giữa các pha; đo cache/scratch/output bytes khi có thể. Dừng run nếu reserve bị chạm, output/scratch vượt budget thử, Docker/OOM/exit lỗi hoặc không thu được phép đo cần thiết; giữ report và staging.
- Guard theo polling không bảo đảm action Spark không overshoot giữa hai lần đo. B/T là budget kiểm tra, không hard filesystem quota. Không auto retry job nặng hoặc tự tăng RAM/disk khi fail.
- All Beauty là lượt hiệu chỉnh đầu: đo S_review/S_catalog/S_audit/S_quarantine, peak scratch, wall time và RAM thực. Cập nhật B/T từ measurements và schema, không chỉ suy tuyến tính từ một category có text/nested khác; preflight lại cho hai bộ lớn.
- Default container 2 GiB/2 CPU, driver 1 GiB chỉ là thử đầu. EDA từng chạm cgroup gần 2 GiB nên phải thử fixture/Silver nhỏ; chỉ tăng hoặc đổi plan sau khi có số đo, không cam kết raw ETL luôn vừa.

## 7. Hive ở đâu trong kế hoạch?

V1 lưu Parquet trên HDFS và Spark đọc trực tiếp. Chưa cài Hive/metastore chỉ để lưu Silver. Nếu có yêu cầu SQL dùng chung/môn học, thêm **external tables** trỏ đến final run đã validated, không copy payload lần hai. Cần metastore có persistence và cấu hình dùng chung; không dùng metastore_db tạm trong container rồi coi là catalog bền vững. External table đăng ký sau publish, trỏ đúng run/version, không trỏ staging. [Spark 3.5.6 Hive tables](https://spark.apache.org/docs/3.5.6/sql-data-sources-hive-tables.html).

Đây là nhánh tùy chọn tách khỏi nghiệm thu Parquet, không chặn Gold đọc Silver. Chưa thêm HiveServer2, database metastore hoặc service mới trong đợt đề xuất này.

## 8. Mốc kiểm tra và lệnh vận hành

| Mốc | Công việc | Điều kiện đạt |
| --- | --- | --- |
| S0: thiết kế đã áp dụng | Schema, policy rating/null, input trace, budgets và layout | Các quyết định ở mục 9 |
| S1: pure rules | Parser/native types/field states/canonical hash/time/text preservation | Fixture đúng ở IDs/case/duplicate JSON keys/bool numeric/null/negative/seconds/milliseconds |
| S2: Spark fixture | Dedup/catalog uniqueness/quarantine/audit/HDFS write/read-back/finalize | Count conservation, zero-row datasets có schema, joins không nhân dòng, failed run không publish |
| S3: wrapper | Mount processing, shared job exclusion, monitor/log/exit/cleanup | Mock lỗi log/docker/disk/SIGTERM và cleanup đúng ID, EDA không bị đổi hành vi |
| S4: Silver All Beauty | Full raw với expected counts và budgets đã xem xét | 694.253 reviews; catalog 112.590; copies removed 7.275 nếu input/policy như hiện tại; measurements đủ |
| S5: hai category lớn | Video Games rồi Industrial, mỗi category read-back trước chạy tiếp | Counts/bytes/flags/key/time/coverage/resource đạt; manifest complete |
| S6: bàn giao Gold | Schema/run path/manifest contract, người khác đọc thử | Load explicit run, rating/text/flags/trace có đủ, raw bất biến |

Expected counts từ [EDA evidence local](../outputs/validation/processing-plan-20261014/evidence.json):

| Category | Input reviews | Exact copies removed | reviews_clean dự kiến | catalog dự kiến |
| --- | ---: | ---: | ---: | ---: |
| All Beauty | 701.528 | 7.275 | 694.253 | 112.590 |
| Video Games | 4.624.615 | 53.646 | 4.570.969 | 137.269 |
| Industrial and Scientific | 5.183.005 | 52.988 | 5.130.017 | 427.564 |

Counts khác kỳ vọng phải điều tra source/policy/parser, không tự sửa expected để pass. Review rating thấp/thiếu text vẫn giữ; helpful invalid 0/1/1 và metadata price invalid 0/36/22 chỉ là flags. Native ID/time core reject hiện dự kiến 0. Metadata chưa có review 25/20/56 vẫn ở catalog.

Fixture bổ sung: exact copies khác dict order; same pair/time khác rating; same parent khác asin; ID trim collisions; metadata parent conflicts; source key khác case; timestamp round-trip có millisecond; rating invalid không làm mất interaction; forced failure giữa write và publish. Kiểm tra logic/schema/counts, không viết test chỉ để phản chiếu số lượng hàm hoặc độ dài code.

**PowerShell host tại project/, Docker/HDFS và image hiện có phải sẵn sàng; không bật notebook/Spark batch khác đồng thời:**

~~~powershell
& ./tools/silver/Run-Silver.ps1 -Category All_Beauty
# Chỉ khi lượt trước đạt và budgets đã hiệu chỉnh:
& ./tools/silver/Run-Silver.ps1 -Category Video_Games
& ./tools/silver/Run-Silver.ps1 -Category Industrial_and_Scientific
~~~

Wrapper gọi **Python trong container**, `python -m processing.silver --category All_Beauty`; input/output production roots cố định, fixture chỉ được dưới /project/testing/{raw,processed}/silver/<id>. Có thể override DockerDiskPath nếu Docker chuyển ổ; wrapper không build/pull image hoặc khởi động HDFS. Python CLI không có host Windows/VHDX guard, ưu tiên wrapper cho raw.

**Kiểm thử, PowerShell host tại project/:**

~~~powershell
& ./tests/test_silver_runtime.ps1
docker compose -f compose.yaml -f compose.datanodes.yaml -f compose.silver-light.yaml run --rm --pull never --no-deps -e SILVER_TEST_HDFS=1 spark-notebook python -W ignore::ResourceWarning /workspace/tests/test_silver.py
~~~

Chỉ chạy fixture khi không có batch raw đang chạy. Runtime tests dùng Docker mock/TestRepo riêng, không khởi động container thật. Fixture giữ bằng chứng dưới /project/testing và outputs/validation; không tự xóa. ResourceWarning suppression ở lệnh test chỉ giảm nhiễu socket của PySpark, không bỏ qua lỗi test hoặc Spark.

## 9. Các quyết định đã áp dụng và giới hạn vận hành

1. **Lưu trữ:** Parquet Snappy trên HDFS, chưa có Hive bắt buộc; giữ reviews/catalog riêng.
2. **Schema:** text và nested/extras cần thiết giữ ở Silver, không duplicate full raw JSON cho dòng hợp lệ; raw là nguồn phục hồi. Text ghép tạo bằng view/Gold thay vì lưu lặp.
3. **Policy:** keys/time là core; rating/text/price lỗi là field flags; exact dedup riêng category; metadata conflicting parent làm fail gate thay vì pick first.
4. **Tài nguyên:** một category/process, 2 GiB/2 CPU thử đầu; B=2R/T=4R/M=20 GiB là budget để đo lại, không bảo đảm output hay peak.
5. **Publish/retention:** giữ raw và một bộ Silver complete được chọn mỗi category; khi chạy lại giữ bản cũ đến khi nghiệm thu bản mới. Run đã được Gold tham chiếu phải bất biến. Failed staging chỉ dọn có kiểm kê đường dẫn sau review, không tự xóa; nhiều failed/old runs phải tính thêm budget.
6. **Thứ tự:** code + fixture trước, All Beauty trước để hiệu chỉnh storage, rồi hai category lớn; Gold/model là bước sau, không gộp vào một job Silver.

Gold/model/Hive và incremental merge vẫn ngoài đợt Silver này. Không đổi raw, format HDFS hoặc xóa volumes.

## 10. Kết quả triển khai ngày 16/10/2026

Fixture HDFS đạt các trường hợp: canonical copies khác dict order, giữ low/invalid rating, whitespace text, milliseconds 12/13 digits và round-trip, metadata key khác case, quarantine/audit, unmatched review, catalog conflict, budget failure và fail-before-publish. Run lỗi không có final output; SparkContext dừng sau job. Host mocks đạt trên PowerShell 7 và Windows PowerShell 5.1: native stderr, active Spark/shared lock, selected fsck, host reserve, exit/OOM, manifest/log/rm failures và nhả lock. Đây không phải benchmark multi-host hoặc kiểm thử mọi điểm có thể bị forced kill.

| Category | Run ID | Reviews sạch | Catalog | Exact copies loại | Dataset GiB | Scratch GiB quan sát cao nhất |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| All_Beauty | run_20261016T021632209982Z | 694,253 | 112,590 | 7,275 | 0.273 | 1.048 |
| Video_Games | run_20261016T022209697481Z | 4,570,969 | 137,269 | 53,646 | 1.869 | 8.234 |
| Industrial_and_Scientific | run_20261016T053543469765Z | 5,130,017 | 427,564 | 52,988 | 1.999 | 7.675 |

Tổng **10.395.239 reviews, 677.423 catalog rows, loại 113.909 copies, quarantine=0**. Catalog tổng theo category, chưa là distinct toàn scope. Dataset bytes=4.446.160.227 (~4,14 GiB), chưa tính JSON báo cáo/manifest. Metadata coverage 100% và join giữ đúng số reviews ở cả ba. Count conservation, unique keys/hash, timestamp milliseconds và schema đọc lại đạt. Helpful/price lỗi được null/flags; review rating thấp vẫn giữ.

Final HDFS dùng prefix `hdfs://namenode:8020/project/processed/silver/`, sau đó `<category_lower>/v1/<run_id>` theo bảng. Xem tên dataset bên trong ở mục 5. Gold phải tham chiếu manifest complete và run cụ thể.

Báo cáo local: `outputs/processing/silver/<run_id>/run.json`, `quality.json`, `resources.jsonl`, `batch.json`, `batch.log` và host resource log. [Biên bản bàn giao](../outputs/validation/silver-20261016/handoff.json) ghi counts/path/fingerprints/checks; chỉ có trên máy này, ignored Git. Ba final paths đều fsck HEALTHY. Chín file raw trước/sau giữ nguyên bytes và modification time; không hash lại toàn payload. Cluster còn một missing block lịch sử ở testing legacy; kết quả fsck Silver không đồng nghĩa toàn cluster sạch lỗi.

Cgroup peak khoảng 2 GiB, không có run exit/OOM failure. Peak là quan sát polling, không phải hard bound. Video Games có khoảng trống timestamp log ~10.981 giây, chưa xác minh nguyên nhân; không dùng tổng wall time này làm benchmark throughput. Không có benchmark multi-host.

Ba full runs dùng producer SHA256 `0f08a7159ed0af7630cd67ef00766e0dcac49a88ee9648de283fc8fbc994f33f`. Sau đó code bổ sung parse integer string bằng Decimal, finite reserve validation, schema type gate và bounded hashed metadata-conflict diagnostics; current SHA256 `cd43f2505bbfe5224311de57dfe3242775f41674e4d07698d32f071b5a48dd39`. Source producer được lưu trong `outputs/validation/silver-20261016/producer_source_0f08a715/`. Fixture HDFS cuối với code hiện hành đạt 2 tests; runtime mocks đạt cả PowerShell 7/5.1. Ba output thật đã đối chiếu schema với gate mới; counter đầu vào native/null, không có cờ wrong-original-type, nên thay đổi integer string không đổi kết quả này. Không chạy lại raw chỉ để bổ sung guard. Gold/features/split và Hive chưa triển khai.
