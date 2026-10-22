# Processing: từ EDA đến Silver, Gold và dữ liệu đánh giá

**Cập nhật: 18/10/2026.** Silver đã nghiệm thu cả ba category; kết quả từng run ở [silver.md](silver.md). EDA bổ sung trên ba Silver snapshots đã complete; xem [eda.md mục 8.7](eda.md#87-kết-quả-full-silver-ngày-18102026). Gold, đặc trưng model và protocol đánh giá bên dưới vẫn là đề xuất chưa triển khai. Mục tiêu: **khuyến nghị sản phẩm tiếp theo; đánh giá qua tương tác review, dùng rating/review quá khứ làm đặc trưng khi có**. Phạm vi chỉ mua được xác minh cần chọn policy verified-only; timestamp hiện có là thời điểm review.

Đọc mục 1–3 để hiểu bài toán; mục 4–5 là các bước sẽ làm; mục 6 là những lựa chọn cần xem xét; mục 7–8 là nghiệm thu và bằng chứng. [EDA](eda.md) giữ đặc tả phân tích; [kế hoạch tổng quát](../../doc/KE_HOACH_TONG_QUAT.md) giữ backlog học thuật.

Hợp đồng và vận hành Silver nằm trong [silver.md](silver.md): code, schema Parquet, HDFS staging/final, output/scratch budgets, Hive tùy chọn và kết quả thực tế. Các quyết định Gold ở đây chưa tự trở thành code huấn luyện hoặc evaluation.

## 1. Mục tiêu, đặc trưng và ground truth

### 1.1. Đầu vào khác với đáp án cần dự đoán

Mục tiêu chính đề xuất là xếp hạng sản phẩm để tìm sản phẩm người dùng sẽ tương tác tiếp theo. Với dữ liệu hiện có, tương tác được quan sát qua việc **viết review**.

| Khái niệm | Ý nghĩa | Ví dụ |
| --- | --- | --- |
| Lịch sử đầu vào | Sản phẩm và reviews đã có trước thời điểm dự đoán | An đã review A và B |
| Đặc trưng | Thông tin trích từ lịch sử được phép dùng | An chê pin A, khen độ thoải mái B; đã tương tác 2 sản phẩm |
| Candidates | Danh sách sản phẩm hệ thống được phép xếp hạng | Catalog đã quan sát trong train của category |
| Nhãn / ground truth | Sản phẩm tương tác tiếp theo, giữ lại để kiểm tra | An review C sau đó; đáp án là C |
| Top-K | K sản phẩm được mô hình xếp hạng cao nhất | Kiểm tra C có nằm trong Top-10 không |

Ví dụ minh họa, không phải bản ghi thật:

| Thời gian | Review của An | Vai trò khi dự đoán tương tác tháng 6 |
| --- | --- | --- |
| Tháng 1 | A: “Âm thanh tốt nhưng pin yếu”, 2 sao | Lịch sử đầu vào; có thông tin khen và chê |
| Tháng 3 | B: “Pin lâu, đeo thoải mái”, 5 sao | Lịch sử đầu vào |
| Tháng 6 | C: bất kỳ rating nào | C là nhãn; rating/text của review C bị che khỏi đầu vào của mẫu này |

Review C có 1 sao thì C **vẫn là nhãn tương tác tiếp theo**. Nó không chứng minh An thích C. Sản phẩm chưa có review là chưa quan sát được tương tác, không mặc nhiên là người dùng ghét sản phẩm. Negative sampling, nếu dùng khi train, là kỹ thuật của objective riêng, không phải nhãn “không thích” do dataset cung cấp.

### 1.2. Những điểm sửa so với kế hoạch trước

- **Bỏ rating ≥4 làm điều kiện chọn ground truth chính.** Giữ cả rating thấp/cao. Ngưỡng này chỉ có thể dùng tạo nhóm phản hồi đầu vào hoặc bài toán phụ về sở thích được đặt tên riêng.
- **Review tích cực/tiêu cực là đặc trưng; tương tác tiếp theo là nhãn.** Hai thứ kết hợp được trong cùng bài toán.
- **Không mặc định loại sản phẩm từng tương tác khỏi candidates.** Tương tác tiếp theo có thể lặp cùng parent. Discovery chỉ gợi ý sản phẩm mới với user là thí nghiệm khác.
- **Không lấy latest rating của toàn cửa sổ test làm nhãn chính.** Đề xuất nhãn là tương tác đầu tiên sau mốc dự đoán.
- **Không gọi toàn bộ reviews là giao dịch mua.** `verified_purchase` là cờ xác minh mua hàng; timestamp là thời điểm review, không có log đầy đủ đơn hàng/thời điểm mua. Mua, tương tác và thích là các mục tiêu khác nhau. [Schema Amazon Reviews 2023](https://amazon-reviews-2023.github.io/).

Đối chiếu BLaIR **v2**: mục 4.1 định nghĩa dự đoán sản phẩm tương tác tiếp theo; C.1 nêu split thời gian cho sequential recommendation, còn CF dùng tỷ lệ 4:3:3. Các phần đã kiểm tra không quy định rating ≥4 hoặc chỉ `verified_purchase=true`. Review sentiment là hướng mở rộng của nhóm; cutoff/cách tạo mẫu dưới đây cũng là đề xuất của nhóm, **chưa phải tái lập nguyên bản benchmark**. [BLaIR v2, mục 4.1 và phụ lục C](https://arxiv.org/html/2403.03952v2).

## 2. Bronze, Silver và Gold có vai trò gì?

Ba tầng phân biệt nguồn gốc, dữ liệu đã sạch và dữ liệu dành cho một thí nghiệm. Không cần thêm ba dịch vụ hoặc sao chép nguyên bộ dữ liệu ba lần.

~~~mermaid
flowchart TD
    B[Bronze: raw reviews và metadata bất biến] --> S[Silver: chuẩn hóa, kiểm tra, exact dedup]
    S --> C[reviews_clean và catalog]
    S --> Q[Quarantine và duplicate audit]
    C --> T[Gold: chia thời gian và chọn mốc dự đoán]
    T --> X[Lịch sử quá khứ và đặc trưng đầu vào]
    T --> Y[Nhãn tương tác tiếp theo, lưu riêng]
    X --> M[Train hoặc tạo Top-K]
    M --> E[Đánh giá từng nhóm]
    Y --> E
~~~

| Tầng | Chứa gì? | Khi nào thay đổi? |
| --- | --- | --- |
| Bronze / raw hiện có | Sáu dataset reviews/metadata đúng nguồn và manifests | Thêm batch/version có chủ đích; không sửa để làm sạch |
| Silver | Reviews sạch, catalog, quality flags, quarantine và audit bản sao | Nguồn hoặc policy làm sạch đổi |
| Gold | Split, lịch sử từng mẫu, đặc trưng, nhãn riêng, mappings/candidates/cohorts và protocol manifest | Cutoff, cách tạo nhãn, encoder hoặc thí nghiệm đổi |

Silver giữ text/rating/verified_purchase/event_time. Gold quyết định **mẫu nào được nhìn thấy review nào** và cách biểu diễn chúng. Tạo embeddings toàn bộ review rồi join theo user vẫn có thể gây leakage nếu không ràng buộc thời gian.

Bronze dùng `/project/raw/<category_lower>_{reviews,metadata}/v1` như hiện tại. Layout output đề xuất:

~~~text
/project/processed/silver/<category_lower>/v1/<silver_run_id>/
    reviews_clean/       catalog/
    quarantine/          duplicate_audit/       manifest.json

/project/processed/gold/<category_lower>/v1/<gold_run_id>/<protocol_id>/
    train_examples/      validation/            test/
    mappings/            candidates/            cohort_report/
    feature_manifest.json                       manifest.json
~~~

Gold dự kiến có `inputs/`, `labels/`, `eligibility/` tách riêng và tham chiếu Silver bằng run/version/record hash để tránh copy text. Silver đã ghi theo layout trên; Gold chưa được tạo. Report `outputs/processing/` được ignore Git; fixture dưới `/project/testing/...`.

## 3. Chia tập và xử lý người ít tương tác

### 3.1. Train học quá khứ, validation chọn cấu hình, test kiểm tra cuối

| Tập | Khoảng đề xuất, UTC | Mục đích |
| --- | --- | --- |
| Train | Trước 01/01/2021 | Học model và fit các phép biến đổi |
| Validation | [01/01/2021, 01/01/2022) | Chọn cấu hình bằng đáp án chưa cho model học |
| Test | [01/01/2022, 01/10/2023) | Kiểm tra sau khi đã khóa lựa chọn |

**Các mốc là đề xuất để xem xét, không phải kết luận bắt buộc từ EDA hoặc cutoff nguyên bản BLaIR.** EDA mới đo ranh giới 2022, chưa đo đủ mẫu Gold cho validation 2021. Diagnostic phải đi trước khóa protocol; không chọn ngày bằng metric test.

Khi tuning, lịch sử validation chỉ lấy trước 2021. Sau khi chọn cấu hình, refit trên dữ liệu trước 2022, rebuild transforms/history/mappings rồi test một lần. “Đã thấy” lúc test được tính theo dữ liệu huấn luyện cuối đó. Mọi model so sánh dùng cùng lịch sử được phép, nhãn, candidates và cohort.

### 3.2. Cách tạo mẫu tương tác tiếp theo

Đề xuất khởi đầu **đánh giá tại mốc cố định**, tối đa một query/user/category trong mỗi cửa sổ validation/test:

1. `prediction_time` là đầu cửa sổ. Inputs chỉ lấy events có `event_time < prediction_time`.
2. Tìm timestamp tương tác sớm nhất của user trong cửa sổ. Parent item ở timestamp đó là ground truth, không phụ thuộc rating/verified_purchase.
3. Nếu cùng timestamp có nhiều parent items, lưu tập nhãn đồng thời và đánh dấu `TIED_NEXT_ITEMS`; không dùng hash giả lập thứ tự. Báo riêng ties và dùng metric hỗ trợ nhiều nhãn.
4. Lưu `target_event_time`, target IDs và source references trong labels; không join target text/rating sang inputs.
5. Events muộn hơn vẫn ở Silver, nhưng không làm thêm nhãn cho query mốc cố định này. Rolling evaluation cập nhật lịch sử sau mỗi event là protocol khác, có cost/report riêng.

Nhiệm vụ cụ thể là “tương tác đầu tiên sau mốc từ lịch sử đã biết tại mốc”, không dùng tập mọi sản phẩm của cả năm như một nhãn next-item.

Train examples cũng có cặp lịch sử–nhãn: trong khoảng train, mỗi timestamp group có lịch sử trước đó có thể làm target; history chỉ lấy events có thời gian nhỏ hơn target. Mọi events train vẫn dùng được cho popularity/history; user chỉ có một timestamp chưa tạo được mẫu tuần tự có lịch sử. Việc fit vocabulary từ tập train không thay kiểm tra history theo từng query.

### 3.3. Một sản phẩm khác một review duy nhất

EDA đo **số distinct parent items**. User có một sản phẩm có thể có nhiều reviews về sản phẩm đó. Phải đo lại số events/timestamp groups sau exact dedup; không suy 93,22% user All Beauty chỉ có đúng một review.

| Dữ liệu sau làm sạch | Vai trò | Cách đánh giá |
| --- | --- | --- |
| Đúng một event ở train, không có held-out event | Giữ để tính popularity/history; chưa tạo được cặp lịch sử–next-item cho user đó | `NO_HELDOUT_EVENT`, không có ground truth tương lai, metric N/A |
| Đúng một event ở test, trước đó không có lịch sử | Có nhãn test, history rỗng | Cold-user; thử popularity fallback nếu target thuộc candidates, không gọi là cá nhân hóa |
| Một event trước mốc và một event sau mốc | Một review đầu vào và một tương tác đáp án | History=1; đây là hai events tổng cộng |
| Nhiều events trên một parent | Có lịch sử lặp, target sau có thể cùng sản phẩm | Giữ repeat trong protocol chính, báo repeat rate |

Giữ user ở Silver không đồng nghĩa mọi user có mẫu đánh giá cá nhân hóa. Sentiment không tạo thêm lịch sử cho người chưa có event nào trước mốc.

### 3.4. Các nhóm đánh giá nằm trong cùng tập test

| Có lịch sử trước mốc? | Target item có trong train? | Nhóm và xử lý |
| --- | --- | --- |
| Có | Có | Warm user–warm item; mô hình hoặc fallback theo khả năng thực tế |
| Không | Có | Cold user; popularity từ train |
| Có | Không | Cold item; target ngoài candidates của benchmark train-catalog, báo coverage |
| Không | Không | Cold user–cold item; thiếu cả history và item factors/candidates |

Đây là nhãn cho các query, không phải bốn tập train riêng. Báo thêm history events và distinct items (0 / 1 / 2 / 3–4 / ≥5). User có history khác user có factor của từng model; model preprocessing có thể làm coverage khác nhau.

Candidates chính: parent items cùng source_category đã thấy trong training portion, **giữ item từng tương tác**. Target ngoài candidates vẫn giữ trong population/labels, có `TARGET_OUTSIDE_CANDIDATES`; không chọn event muộn hơn chỉ để có target hợp lệ. Ties phải báo target coverage và nhóm full/partial in-catalog riêng; không bỏ nhãn ngoài catalog rồi gọi metric đó là toàn bộ query.

Đề xuất Recall@10/20, NDCG@10/20, eligible-query/target coverage, model score coverage, fallback usage và catalog coverage. Một nhãn: Recall@10=1 nếu tìm trúng Top-10, bằng 0 nếu trượt; NDCG xét thêm vị trí. Không có nhãn hợp lệ thì N/A. Query đủ điều kiện nhưng model không trả được gợi ý phải báo failure/miss, không âm thầm drop khỏi mẫu số. Báo warm-model metric và hệ thống có fallback riêng.

## 4. Từ phát hiện EDA đến hành động

| Phát hiện đã đo | Ý nghĩa | Hành động |
| --- | --- | --- |
| Core keys/rating/time hợp lệ, parse errors=0 | Chưa có căn cứ loại hàng loạt reviews vì các trường này | Validate và chuẩn hóa Silver; core reject hiện dự kiến 0 |
| Timestamp milliseconds, có 12/13 digits | Đếm chữ số đơn thuần có thể đổi sai | Giữ integer gốc, đổi UTC chính xác tới millisecond |
| Có 113.909 exact-copy excess rows | Có reviews bị đếm nhiều lần | Dedup full-record canonical hash theo category, ghi audit |
| Repeated pairs và 15 user–parent–timestamp groups nhiều versions | Bản sao khác tương tác lặp/khác nội dung | Giữ event khác nội dung, gắn cờ ties/conflicts |
| Metadata coverage 100%, parent keys không trùng từng category | Có thể join an toàn nếu giữ uniqueness | Catalog unique, assert join không tăng/giảm reviews |
| Price có ở 15,73% / 45,15% / 52,14% matched items | Bắt buộc giá làm mất nhiều tương tác | Null/status, không impute 0 hoặc loại item |
| Text không usable: 904 / 4.934 / 8.192 dòng | Một số mẫu thiếu kênh văn bản | Giữ event/rating, has_review_text và fallback |
| User một distinct item rất nhiều; user≥5 chỉ giữ 2,01% / 21,73% / 16,75% reviews | Lọc mạnh thay đổi population | Không k-core mặc định, Gold đo history/cold-start |
| Warm user–warm item chỉ 4,20% / 17,91% / 20,68% raw test events ở cutoff 2022 | Warm CF metric chưa phản ánh toàn population | Báo eligibility, cold-start và fallback |
| Source category khác main_category; bought_together toàn null | Taxonomy/graph không luôn đầy đủ | Giữ hai category fields, chưa dùng bought_together làm feature |

Thứ tự tỷ lệ: All Beauty / Video Games / Industrial and Scientific. Đây là số trước processing. Số **first-next queries** Gold phải đo lại, không lấy tỷ lệ raw test events làm kết quả đánh giá mô hình.

## 5. Các bước processing cụ thể sẽ thực hiện

Các bước 1–5 đã được triển khai trong Silver và có read-back gates; trạng thái raw từng category nằm ở silver.md. Bước 6–9 thuộc Gold/model vẫn là kế hoạch, policy mục 6 cần được diagnostic trước huấn luyện.

### Bước 1. Khóa input, schema và cấu hình run

- Đọc scope/sáu namespace raw v1; kiểm tra manifests/revision, finalized files và inventory. Một category/process; không ingest lại batch cũ.
- Version schema/Silver policy/Gold protocol, UTC bounds, cutoff, item identity và output run; ghi hash code/config, tham chiếu EDA.
- **Output:** run manifest running, expected counts/inventory. **Đạt khi:** đúng scope, không có file đang ghi, đủ reserve tài nguyên, raw không đổi.

### Bước 2. Parse và chuẩn hóa Silver

- Kiểm tra JSON object/duplicate JSON keys/native types. ID trim, giữ case; giữ asin khác parent_asin. Giữ timestamp_raw Long milliseconds và tạo event_time UTC chính xác tới milliseconds.
- Chuẩn hóa text nhẹ, giữ field-state/usable flags; raw giữ giá trị gốc. Không bỏ từ phủ định như “not” hoặc xóa số/cụm từ tùy ý. Review title/text khác product title/description.
- Parse lỗi, invalid user/parent/time vào row quarantine với primary_reason và all_reason_codes. Rating phải hữu hạn [1,5], bool không phải rating: lỗi rating **null/flag và giữ interaction nếu core keys/time hợp lệ**. Rating-feature/explicit-rating view có eligibility riêng; rating không quyết định event có xảy ra.
- Helpful_vote/media/text/price lỗi là field-level flags, không loại core interaction. Batch hiện tại không có invalid rating nhưng policy batch mới phải rõ.
- **Output:** typed reviews/metadata và quarantine. **Đạt khi:** count conservation, không mất âm thầm dòng lỗi hay dòng thiếu text/rating.

### Bước 3. Loại exact copies và ghi audit

- Hash JSON canonical như EDA, trước chuẩn hóa text: sort dict keys, giữ list order/khác biệt nội dung. Giữ một bản mỗi (source_category,record_hash), ghi occurrences/removed_count/provenance đại diện xác định.
- Giữ event khác nội dung dù cùng user–parent. Với ties ghi versions/parent/rating conflicts, không tự xóa next-item nhãn vì khác rating. Hash dòng không phải line number hoặc thứ tự thời gian.
- **Output:** reviews_clean/duplicate_audit. **Đạt khi:** loại 7.275 / 53.646 / 52.988 copies; dự kiến còn 694.253 / 4.570.969 / 5.130.017 reviews nếu không có core reject mới.

### Bước 4. Tạo catalog Silver và kiểm tra join

- Một row/(source_category,parent_asin), giữ title/features/description, main_category/store, nullable price/status, flags/provenance; extras/details bảo toàn ở cấu trúc an toàn khi cần.
- source_category từ namespace, không thay bằng main_category. Metadata duplicates/conflicts batch mới phải resolve hoặc tách unresolved trước join, không pick first.
- Tạo item_title_text và item_text mở rộng riêng có version. Giữ metadata items chưa có review; không tự coi chúng có trong catalog lịch sử. Chưa dùng price/average_rating/rating_number crawl snapshot làm feature lịch sử.
- **Output:** catalog và coverage/quality report. **Đạt khi:** keys unique, left_join_rows=reviews_clean_rows, thiếu price/text vẫn giữ item.

### Bước 5. Ghi và đọc lại Silver

- Ghi Parquet Snappy theo run, đọc lại từ đích để kiểm tra schema/counts/key/hash/UTC/flags.
- Đối soát `input_reviews = parse_quarantine + core_quarantine + exact_copy_removed + reviews_clean`; nhóm loại trừ không chồng nhau. Optional flags không trừ thêm.
- **Output:** Silver manifest complete khi checks đạt; fail ghi failed, Gold không dùng output đang viết.
- All Beauty trước; đạt mới Video Games rồi Industrial. Nghiệm thu Silver chưa cần train model.

### Bước 6. Tạo split và mẫu lịch sử–nhãn Gold

- Chia events theo UTC trước khi tạo sequence/history/latest-rating views. Tạo train examples và query validation/test theo mục 3.2.
- Mỗi input chỉ chứa events trước prediction_time của chính mẫu. Target text/rating không vào inputs. Không ép một event thành cả history và label.
- **Output:** history references, labels riêng, ties và eligibility reports. **Đạt khi:** không có future event trong history; target không được chọn bằng rating/candidate availability.

### Bước 7. Tạo đặc trưng lịch sử, giữ cả phản hồi tích cực/tiêu cực

- Đặc trưng nền: events/distinct items, last interaction time/gaps tới prediction_time, rating summaries từ history, verified_purchase và tỷ lệ text usable.
- Proxy theo rating đề xuất: 1–2 unfavorable, 3 neutral, 4–5 favorable, null unknown. Đây **không phải sentiment phân tích từ text** và không phải nhãn target.
- Giữ review text để làm TF-IDF hoặc encoder được chọn sau. Đề xuất kênh favorable/unfavorable riêng cùng kênh all-history; thiếu text dùng rating/history/item content với flags. Review 2 sao vẫn có thể khen một khía cạnh; không coi mọi câu đều tiêu cực. NLP sentiment/aspect là mở rộng cần kiểm tra mẫu/version, chưa tự thêm LLM job nặng.
- Fit vocabulary/IDF/scaler/encoder thích nghi từ train của fold. Khi tạo input một query, chỉ transform/aggregate review quá khứ được phép; không dùng target text/rating hoặc review tương lai của user.
- Chưa tổng hợp reviews mọi user thành product-review feature mặc định. Nếu bổ sung, phải lọc as-of cho từng query để tránh reviews tương lai.
- **Output:** input features/references và feature manifest (method/version/fit period/history bounds). **Đạt khi:** thay text/rating target trong fixture không làm đổi input features.

### Bước 8. Tạo mappings, candidates và cohort report

- String→Int mappings xác định từ training portion cho model cần Int; unknown IDs giữ gốc/flag, không gán chung một factor ID, không hash ID thành Int có collision.
- Candidate manifest theo category/fold, có repeat; giữ target ngoài candidates trong labels/report, không thay target thuận lợi hơn.
- Đếm history events/distinct items, warm/cold, query có nhãn, full/partial in-catalog targets, feature availability và model-factor coverage khi model được fit.
- **Output:** mappings/candidates/cohort_report. **Đạt khi:** population đối soát, exclusions có lý do, models so sánh cùng queries/labels/candidates.

### Bước 9. Đóng Gold và bàn giao cho bước model

- Read-back kiểm tra input–label keys, time bounds, mapping/candidate versions; inference inputs không chứa labels. Manifest complete mới cho downstream dùng.
- Khóa protocol trước tuning; chọn cấu hình xong mới refit train+validation và tạo test inputs/features/mappings theo cùng quy tắc trước mốc test.
- **Output:** versioned Gold, feature manifest, metric toy examples và report mà người khác load lại được.
- Đề xuất thí nghiệm sau processing: A=Popularity từ distinct users/item trong train; B=model dùng history item; C=cùng backbone thêm rating-history; D=cùng backbone thêm review text/sentiment. So B/C/D trên cùng protocol để đo đóng góp; chưa khẳng định sentiment tăng metric hoặc chọn sẵn thuật toán.

## 6. Các lựa chọn để nhóm xem xét

| Quyết định | Đề xuất | Trạng thái |
| --- | --- | --- |
| Mục tiêu chính | Next observed review interaction, giữ rating thấp/cao | Hướng cập nhật theo trao đổi, không gọi purchase prediction |
| Silver/Gold | Raw là Bronze; clean Silver; split/features/labels Gold | Silver ba category đã complete; Gold chưa triển khai |
| Exact dedup | Canonical full-record hash trong source_category | Có EDA expected counts |
| Rating lỗi/thiếu | Giữ interaction với null/flag nếu keys/time hợp lệ | Sửa policy cũ từng reject vì rating |
| Ground truth | First next timestamp sau mốc, ties là tập items | Đề xuất cụ thể, cần đo số mẫu/ties |
| Repeat | Giữ item từng thấy trong candidate chính | Discovery-only là thí nghiệm khác |
| Cutoff | Train trước 2021, validation 2021, test từ 2022 | Cần diagnostic, không coi là split của paper |
| Review features | Rating proxy trước, text theo kênh riêng, giữ phủ định | Chọn TF-IDF/encoder và đo cost sau Silver |
| History cap | Silver đầy đủ; thử 50 events gần nhất nếu cần ở Gold | Chưa chốt, báo impact; không cắt ties bằng hash |
| Minimum history | Không k-core/≥5 ở Silver; history=0 dùng fallback | Train sequence cần history; đo events/timestamp groups |
| Candidates | Items observed train của source_category, có repeat | Cold-item coverage riêng; mở catalog là protocol khác |
| Train riêng/chung | Baseline riêng category trước | Chung cần đo overlap/conflicting metadata |
| Verified purchase | Giữ thuộc tính, không lọc bắt buộc | Chỉ lọc khi đặt bài toán verified-review riêng |

Metadata không có effective-time: kể cả title/description là crawl snapshot. Content/hybrid phải khai báo giả định **catalog snapshot được cung cấp**, không chứng minh nội dung tồn tại ở review time. Mở rộng cold-item candidates cần giả định/manifest chung cho các model được so.

Explicit ALS nếu thử là baseline phụ dùng ratings quá khứ; rating labels khác next-item labels. Latest rating chỉ tạo as-of từ training portion cho view phụ. Ties ratings mâu thuẫn làm rating label unknown/excluded riêng, **không làm mất interaction hoặc next-item target**. Không dùng signed sentiment trực tiếp như implicit-ALS confidence khi chưa xác định objective.

### 6.1. Bộ phương pháp để thiết kế Gold theo trao đổi ngày 18/10/2026

| Phương pháp | Dữ liệu cần chuẩn bị | Phần chưa khóa |
| --- | --- | --- |
| Content-based | Text item; history item trước mốc; content availability/fallback | TF-IDF hay text embeddings; title-only hay expanded; mean item profile là một lựa chọn biểu diễn |
| Implicit ALS | Distinct train user–item pairs; mappings Int; strength nếu có policy riêng | Binary pairs làm đề xuất khởi đầu; không tự dùng rating 1–5 làm confidence |
| NCF/NeuMF thuần | Train pairs, user/item ID mappings, train-only negative sampler | Baseline chỉ dùng user ID và item ID; chưa thêm rating, metadata hoặc text; cấu hình neural network để giai đoạn model |
| DeepFM chỉ ID, sau đó bổ sung đặc trưng | Khởi đầu cùng user/item IDs và train pairs như NCF; chuẩn bị thêm các nhóm đặc trưng cho ablation | Đo bản chỉ ID trước; sau đó thêm từng nhóm theo mục 6.2; cách đưa content vector vào mô hình cần đặc tả, chưa chọn encoder |

Popularity là baseline/fallback chung; ItemKNN và hybrid trộn điểm không còn là hai thí nghiệm chính trong bộ bốn này. Bốn thuật toán chính là content-based, implicit ALS, NCF thuần và DeepFM; các mức thêm đặc trưng là biến thể của DeepFM. Chưa train hay tạo embeddings trong EDA.

[EDA bổ sung trên Silver](eda.md#8-kế-hoạch-eda-bổ-sung-trên-silver-trước-gold) đo first-next queries, verification-policy retention, candidate/content eligibility, train support, text chất lượng/đuôi dài và negative feasibility. Không lặp EDA raw, không pilot mô hình hoặc tài nguyên encoder. Hai policy mọi review và verified-only là diagnostic riêng: false không phải nhãn chưa mua; thay policy có thể đổi target, lịch sử và candidates. Các thuật toán phải so sánh trên cùng policy/fold/candidates khi đánh giá.

### 6.2. Ghi nhớ kế hoạch so sánh sau trao đổi ngày 18/10/2026

**Ý định đã thống nhất:** so hai phương pháp cơ sở content-based và implicit ALS với **NCF thuần ID** và **DeepFM chỉ ID** trước, rồi lần lượt thêm các nhóm đặc trưng vào DeepFM để thử. Chưa mặc định mở rộng NCF; giữ NCF thuần để làm mốc so sánh. Đây là kế hoạch thực nghiệm, chưa phải kết quả hoặc cam kết mô hình nào tốt hơn.

| Thí nghiệm | Đầu vào | Câu hỏi cần trả lời |
| --- | --- | --- |
| Content-based | Nội dung sản phẩm và history item trước mốc | Nội dung và lịch sử ngắn cung cấp baseline tốt đến đâu? |
| Implicit ALS | Distinct train user–item pairs, khởi đầu binary | Collaborative filtering tuyến tính theo latent factors đạt mức nào? |
| NCF thuần | User ID + item ID | Tương tác phi tuyến từ ID có giúp hơn các baseline không? |
| DeepFM D0 | Chỉ user ID + item ID | Với cùng thông tin ID, DeepFM khác NCF thuần như thế nào? |
| DeepFM D1 | D0 + metadata dạng trường, ví dụ category/store nếu có ích và đủ coverage | Các thuộc tính sản phẩm bổ sung được gì? |
| DeepFM D2 | D1 + thống kê lịch sử hành vi trước prediction_time | Thông tin số lượng/tần suất/lần tương tác gần nhất giúp đến đâu? |
| DeepFM D3 | D2 + đặc trưng rating quá khứ | Phân biệt phản hồi thuận lợi/bất lợi có cải thiện dự đoán next-item không? |
| DeepFM D4 | D3 + biểu diễn nội dung sản phẩm | Text item có thêm giá trị ngoài metadata dạng trường không? |
| DeepFM D5 | D4 + biểu diễn review quá khứ của user | Nội dung đánh giá cá nhân có thêm giá trị ngoài item text và rating không? |

**D1–D5 là thứ tự thử đề xuất để nhớ việc**, chưa khóa encoder, cách tổng hợp hoặc danh sách trường cụ thể. Mỗi bước giữ các nhóm trước và thêm một nhóm; so với bước ngay trước và D0. Nếu cần xác định đóng góp độc lập, thử thêm từng nhóm riêng trên D0 hoặc bỏ từng nhóm khỏi bản đầy đủ; kết quả cộng dồn phụ thuộc thứ tự nên không tự coi là đóng góp độc lập. NLP sentiment/aspect là mở rộng riêng, không mặc định đã có trong review vectors.

Quy tắc so sánh và diễn giải:

- Giữ mục tiêu **next observed review interaction**, kể cả target rating thấp. Rating quá khứ là feature; không đổi sang dự đoán rating hoặc tự lọc target ≥4 sao. Explicit ALS/rating prediction, nếu thử, là bài toán phụ được đặt tên riêng.
- Dùng cùng fold thời gian, policy review/verified, labels, candidates và cohort cho mọi model. NCF và các bản DeepFM dùng cùng quy tắc/lượng negative sampling và seed để hạn chế nhiễu so sánh; ghi cấu hình và ngân sách tuning của từng model. Content-based vốn dùng nội dung, không coi toàn bộ bảng bốn thuật toán là so sánh cùng lượng thông tin đầu vào.
- Không dùng rating/text của target hoặc thống kê tương lai. Metadata là crawl snapshot phải khai báo giả định; chưa dùng average_rating/rating_number/price như đặc trưng lịch sử khi chưa kiểm soát thời điểm. Không loại query chỉ vì thiếu feature ở một biến thể; dùng missing flags/fallback và báo coverage.
- Báo Recall/NDCG@10/20 cùng coverage, fallback, thời gian train/chấm điểm và bộ nhớ. Tách nhóm history=0/1/2/3–4/≥5 và warm/cold để kiểm tra lợi ích khi lịch sử ít; không chỉ báo metric trung bình.
- User có một interaction trong train vẫn có thể đóng góp mẫu user–item cho NCF/DeepFM, nhưng tín hiệu cá nhân hóa yếu. User chỉ có một event trong toàn dữ liệu không đồng thời cung cấp history train và target tương lai độc lập. Không tự lọc k-core chỉ để làm đẹp kết quả; nếu thử phải báo retention và cohort.
- NCF/DeepFM có user ID đều phải lưu embedding theo số user train. Lookup không tạo one-hot dense cho mỗi batch, nhưng tổng tham số vẫn tăng theo số user/item. Trước train, tính ngân sách bằng distinct train users/items, số chiều, số bảng embedding, gradient và optimizer; giảm batch không làm nhỏ bảng tham số. Khởi đầu riêng từng category, thử embedding nhỏ theo ngân sách thực tế.
- Thêm nhiều feature không bảo đảm cải thiện; giữ và báo cả biến thể không cải thiện. User cold chưa có embedding được học cần fallback theo protocol, không dùng ID mới như đã cá nhân hóa.

## 7. Triển khai và nghiệm thu

### 7.1. Thứ tự và trách nhiệm

| Pha | Owner đề xuất | Đầu ra | Chuyển pha khi |
| --- | --- | --- | --- |
| P0 | B + C, A rà tài nguyên | Policy/schema/protocol/reason codes, fixture design | Rõ inputs/targets, Silver/Gold và lựa chọn đang thử |
| P1 | B, A hỗ trợ | Silver fixture + runner + quality/audit/read-back | Parser/dedup/join/count/time/flags tests đạt |
| P2 | B chạy, A theo dõi | Silver All Beauty → Video Games → Industrial | Từng category đạt counts trước chạy tiếp |
| P3 | B + C | Gold history/next-target/features/mappings/candidates/cohorts | Đủ queries, leakage checks đạt, khóa protocol |
| P4 | C kiểm tra, B xuất | Snapshot, load chéo và metric toy | Người khác tái lập inputs/labels/candidates được; sau đó mới train |

Owner theo [kế hoạch tổng quát](../../doc/KE_HOACH_TONG_QUAT.md), chưa gán ngày cam kết. EDA compact frame không giữ đầy đủ text/catalog, Silver phải đọc raw. Chỉ tái sử dụng định nghĩa canonical/type/time đã kiểm thử, không dùng report EDA làm training dataset. Chưa cần thêm service hoặc scaffold trống.

### 7.2. Kiểm tra bắt buộc

- Silver: JSON hỏng/duplicate keys/bool numeric, ID collisions, ms 12/13 digits và round-trip millisecond, missing/null/placeholder, optional invalid rating/helpful/price, exact copies khác dict order, metadata conflicts/join cardinality và Parquet read-back.
- Gold: target rating 1 vẫn là label; repeat trong candidates; một-event train/test; first next khác latest-in-window; timestamp ties nhiều item/khác rating; target ngoài candidates không bị đổi sang event sau.
- Leakage: history times nhỏ hơn prediction_time; fit transforms từ train; target text/rating không có trong inputs; thay future review không đổi input; normalization giữ phủ định. Refit chỉ mở rộng tới trước test.
- Reporting: đối soát rows clean/quarantine/copies và query population/eligibility, full/partial ties; không có observation thì N/A. Metric toy có hit/miss/rank/multiple targets/model failure. Model không score được không âm thầm drop test rows.
- Ablation: cùng backbone/history/labels/candidates khi thêm rating/text; ghi seed/config/cost. Không chọn cutoff/model/feature bằng test labels.
- Artifacts: complete sau read-back, fail/running không dùng downstream; rerun không append trùng; source/run/policy/protocol/encoder version truy vết được.

### 7.3. Tài nguyên

Một category/process, image Spark hiện có; cấu hình thử đầu local[2], driver 1 GiB, container 2 GiB/2 CPU, guard host/free disk và vòng đời session/cache/container theo runner EDA. Không collect/toPandas toàn raw hoặc tạo toàn tích users×items trong processing. Gold tham chiếu Silver để tránh nhân bản text.

EDA mất khoảng 6,44 / 33,59 / 14,42 phút với local[1]/local[1]/local[2]; cgroup peak gần 2 GiB gồm JVM/Python/page cache, không phải heap DataFrame riêng. Không suy ETL/encoder/model sẽ vừa RAM hoặc hai CPU nhanh gấp đôi. Đo Silver rồi feature job nhỏ; cap history/partitions sau đo. Parquet Snappy, tránh partition theo user/item và file nhỏ; 64–128 MiB/file là target thử. Không format HDFS, xóa volumes, sửa raw routing hoặc compact để thực hiện kế hoạch.

## 8. Bằng chứng EDA và phần chưa đo

### 8.1. Ba run complete

| Category | Run | Reviews | Metadata | Users | Items có review | Distinct pairs |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| All Beauty | [055611](../outputs/eda/run_20261014T055611280385Z/run.json) | 701.528 | 112.590 | 631.986 | 112.565 | 693.929 |
| Video Games | [060728](../outputs/eda/run_20261014T060728726293Z/run.json) | 4.624.615 | 137.269 | 2.766.656 | 137.249 | 4.555.500 |
| Industrial and Scientific | [064207](../outputs/eda/run_20261014T064207289175Z/run.json) | 5.183.005 | 427.564 | 3.435.544 | 427.508 | 5.116.105 |

Cùng raw v1/scope/code/UTC, diagnostic cutoff 2022-01-01, không 5-core. Hoàn tất 13:02:38 / 13:41:05 / 13:56:32 giờ Việt Nam ngày 14/10/2026. Scope SHA256: `22d0830d7180b7de268fa042d25b833160615f9a3758d9318d31cc4b4022029c`; EDA code SHA256: `527257e876f71e96d4b89bc1b8263337fb32e68039bca8f6d5e0e806894bd549`.

Tổng nguồn **10.509.148 reviews + 677.423 metadata**, chín file / **7.136.947.646 bytes**. Silver đọc lại ngày 16/10/2026 xác nhận **10.395.239 reviews**, loại 113.909 exact copies, quarantine=0. Global distinct/overlap chưa đo; không cộng users/items thành unique toàn scope. Inventory/bytes không thay checksum payload mới.

| Diagnostic trước processing | All Beauty | Video Games | Industrial and Scientific |
| --- | ---: | ---: | ---: |
| Parse errors | 0 | 0 | 0 |
| Review/item safe metadata coverage | 100% / 100% | 100% / 100% | 100% / 100% |
| Metadata duplicate parent groups | 0 | 0 | 0 |
| Metadata items chưa có review | 25 | 20 | 56 |
| User có 1 distinct parent item | 589.163 (93,22%) | 2.022.860 (73,12%) | 2.676.271 (77,90%) |
| Reviews giữ khi user≥2 items, một pass toàn lịch sử | 15,07% | 55,60% | 47,67% |
| Reviews giữ khi user≥3 items | 5,09% | 37,43% | 30,09% |
| Reviews giữ khi user≥5 items | 2,01% | 21,73% | 16,75% |
| Top 1% items chiếm distinct pairs | 27,85% | 40,69% | 42,58% |
| Train reviews trước 2022 | 625.542 | 4.044.953 | 4.124.015 |
| Test reviews từ 2022 | 75.986 | 579.662 | 1.058.990 |
| Warm user–warm item test events | 3.193 (4,20%) | 103.799 (17,91%) | 219.020 (20,68%) |
| Test events có cold user | 91,76% | 74,24% | 72,01% |
| Test events có cold item | 38,73% | 27,70% | 23,74% |
| Test events trên pairs đã thấy train | 21 | 239 | 456 |

Cold-user/cold-item rates giao nhau, không cộng. Threshold retention là toàn lịch sử trước Gold; không dùng làm train eligibility. Raw test events chưa là first-next queries của protocol mới.

### 8.2. Trường phụ và nguồn tra cứu

- Time UTC: All Beauty 2000-11-01→2023-09-09; Video Games 1998-11-17→2023-09-12; Industrial 1998-04-26→2023-09-12. Bounds snapshot [1996-01-01,2023-10-01), không áp dụng máy móc cho app events tương lai.
- Helpful_vote invalid: 0 / 1 / 1; metadata price không parse: 0 / 36 / 22. Null/flags cấp trường; các loại lỗi có thể giao nhau.
- Matched items có title/features/description usable: 112.555/112.565; 137.246/137.249; 427.505/427.508. Đây là presence, chưa đo language/ngữ nghĩa.
- All Beauty features rỗng 84,57%, description rỗng 82,98%, categories rỗng 100% trên metadata. Bought_together null 100% cả ba, chưa phù hợp feature chung.
- Video Games có 32 valid-price metadata bằng 0; Industrial max price 129.266,64, P99≈675. Chưa clipping/impute/quy đổi tiền tệ.
- Source khác main_category: Video Games 2.880.571/4.624.615 reviews mang Video Games; Industrial 2.562.216/5.183.005 mang Industrial & Scientific; All Beauty có 7.162 reviews Premium Beauty. Không tự lọc lại nguồn theo nhãn metadata.
- main_category null: 0 / 11.035 / 20.283; extras author/subtitle ở Video Games/Industrial: 416 / 121 rows. Store không tự là brand. Không có collisions ở review_user/review_parent/metadata_parent đã đo.

Nguồn từng run: summary/overview/quality, duplicates/timestamp_units/time_range/id_normalization, user_degree_bins/user_threshold_impact/long_tail, metadata_coverage/matched_content, price_population/summary/bins, sparsity_by_category, processing_gate_impact và temporal_split_diagnostic/temporal_cold_start. [Biên bản đối soát local](../outputs/validation/processing-plan-20261014/evidence.json) giữ checks/SHA256 bảng; outputs ignored không đi cùng clone. Log batch All Beauty mất do lỗi wrapper lịch sử, run/summary/resources còn đủ; xem [history.md](history.md).

### 8.3. Bằng chứng bổ sung và các phần còn lại

- Đã đo trên Silver ngày 18/10: first-next queries/history events-items-timestamp groups, no-target/cold-user, candidate/content eligibility, repeat/ties, train support/rating conflicts, policy A/B retention và negative feasibility. [Kết quả](eda.md#87-kết-quả-full-silver-ngày-18102026) dùng mẫu số query, không thay bằng raw event rates ở mục 8.1.
- Đã đo text sản phẩm: độ dài ký tự/bytes từng trường và sau ghép, flags và rà excerpts có giới hạn. Chưa đo tokenization, tỷ lệ ngôn ngữ/encoding lỗi toàn corpus, chất lượng embeddings hoặc review sentiment. Không có bằng chứng sentiment tăng metric.
- Gold chưa tạo train examples/mappings/labels hoặc kiểm tra model score coverage; cần khóa mục tiêu A/B, đặc tả feature cleaning và triển khai read-back/leakage gates trước huấn luyện.
- TF-IDF/encoder/model cost và pilot tài nguyên nằm ngoài đợt EDA bổ sung theo yêu cầu, chưa được đo hay cam kết vừa tài nguyên.
- Global overlap/conflicting metadata nếu train/catalog chung; RAM/đĩa thực, Parquet read-back, leakage tests, baseline/ablation và model coverage.

**Bước tiếp theo:** dùng bằng chứng EDA bổ sung để khóa interaction policy, split/query/candidates và feature contract, rồi triển khai Gold đọc ba run Silver complete. Giữ title/content references, binary pair baseline và train-only sampler; encoder/model config là giai đoạn sau. Gold/model chưa triển khai; sentiment chưa được trích thành feature tại Silver.
