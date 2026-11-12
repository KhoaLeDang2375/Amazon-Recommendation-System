# Kế hoạch notebook ALS, Content-based, NCF, DeepFM và Two-Tower trên Kaggle

Ngày lập: 04/11/2026. Hai notebook đã triển khai và kiểm tra bằng fixture,
gồm ALS CPU fit thật. Chưa chạy full training/evaluation trên Kaggle T4.
Nguồn chuẩn: Gold protocol fixed_origin_first_next_all_reviews_6m_v1 và gói
gold_training_export_v1_566ed959afa207d6 đã xuất. Gắn Dataset vào Kaggle input,
chọn version cụ thể trước benchmark; không tải lại data về working nếu đã có input mount.

## 1. Phạm vi và bằng chứng ALS đã có

Notebook 04/module als_baseline.py đã chạy Spark implicit ALS trên mẫu Video Games:
5.000 user (6.438 train pairs, 703 queries, 50 warm-full) và 50.000 user
(63.936 train pairs, 6.850 queries, 812 warm-full), ngày 22/10/2026.
Cấu hình pilot: rank 16, 5 iterations, reg 0,1, alpha 1, seed 42; test chưa chạy.
Ở mẫu 50k, warm-full NDCG@10: ALS 0,003261, Popularity 0,010610.
Chi tiết và artifact links: [pilot ALS](als-baseline.md).

Pilot dùng validation cả năm 2021, Gold dùng sáu tháng đầu 2021. Mẫu làm giảm
support user/item; backend Kaggle cũng dự kiến đổi sang implicit. Vì thế kết
quả full Gold là thực nghiệm mới, không so trực tiếp mức tăng/giảm với số pilot.

Hai notebook: [07 ALS](../notebooks/07_kaggle_als_full.ipynb) và
[08 content-based](../notebooks/08_kaggle_content_based.ipynb).
Từ 07/11/2026, mỗi notebook tự chạy tuần tự CATEGORIES_TO_RUN: validation → test
→ Dataset private cho từng ngành. Không gộp mappings hoặc factor indices của ba ngành.

| Category | Train pairs trước 2021 | Refit pairs trước 2022 | Val queries / warm-full | Test queries / warm-full |
|---|---:|---:|---:|---:|
| All_Beauty | 495.387 | 618.845 | 69.848 / 2.681 | 32.466 / 1.751 |
| Video_Games | 3.511.273 | 3.984.667 | 242.638 / 44.239 | 161.663 / 36.893 |
| Industrial_and_Scientific | 3.237.493 | 4.071.315 | 394.153 / 72.503 | 325.569 / 74.055 |

Tất cả train pairs đều được dùng. Smoke test bằng dữ liệu giả phục vụ kiểm tra
implementation; số liệu chính phải dùng đủ population của category/fold tương ứng.

## 2. Dataset và kiểm tra trước khi chạy

| Input | Dùng cho |
|---|---|
| thoandanh/gold-training-v2 | Cả hai notebook |
| thoandanh/gold-emb-all-beauty-v2 | Content-based, All_Beauty |
| thoandanh/gold-emb-video-games-v2 | Content-based, Video_Games |
| thoandanh/gold-emb-industrial-and-scientific-v2 | Content-based, Industrial_and_Scientific |

Ghi version, dataset/category manifest hashes, code hash và package versions.
Gold phải complete, đúng ba ngành, protocol và counts/schema/checksums đã định.
Chỉ đọc shard của category đang chọn. Kiểm tra index Int trong train đúng
range mapping, keys không trùng và counts không thay đổi sau join.

Content-based chỉ nhận embedding bundle complete đúng category và source run;
signature.export_manifest_sha256 phải bằng SHA256 của text_export_manifest.json
trong category Gold. Kiểm tra encoder/revision, dimension 384, cleaning và pooling
policy. Không chỉ dựa vào Dataset slug hoặc số chiều vì nhiều bundle có thể cùng tên.

ALS không cần Dataset embedding. Content-based chỉ dùng item vectors của title +
description; các review vectors trong cùng Dataset được giữ cho DeepFM D2, không
tạo thêm biến thể content-based trong giai đoạn đầu.

## 3. Protocol và bộ đo dùng chung

- Validation: fit trước 01/01/2021, query tại 01/01/2021; labels trong
  [01/01/2021, 01/07/2021). Refit trước 01/01/2022, query test tại 01/01/2022;
  labels trong [01/01/2022, 01/07/2022). Tất cả mốc UTC.
- Dùng nguyên queries/labels/candidates/cohorts của Gold. Labels là các distinct
  item tại timestamp tiếp theo đầu tiên, giữ ties nếu có. Không random split lại.
- Candidates là toàn bộ train items của fold, gồm item từng tương tác.
  Không tự bật filter seen-items; nếu gọi implicit.recommend, đặt
  filter_already_liked_items=False. Xếp hạng exact trên toàn candidates, không
  sampled-negative evaluation hoặc ANN trong benchmark chính.
- Giữ nhãn ngoài candidates trong mẫu số, dùng parent_asin cho ground truth;
  không drop labels do item_idx null. Query thiếu dự đoán vẫn tính miss.
- Rank theo score giảm dần, tie-break item_idx tăng dần. Top-K không trùng item.

Với L_q là toàn bộ target items của query q, R_q^K là danh sách Top-K:
Recall_q@K = |R_q^K giao L_q| / |L_q|.
DCG_q@K = tổng relevance nhị phân / log2(rank + 1).
IDCG dùng min(K, |L_q|), kể cả target nằm ngoài candidates.
NDCG = DCG/IDCG. Lấy trung bình mỗi query, không weighted theo số event user.

| Độ đo | Vai trò |
|---|---|
| NDCG@10 | Metric chính để chọn ALS config trên warm-full validation; xem item đúng nằm cao đến đâu |
| NDCG@20 | Đánh giá chất lượng thứ tự với danh sách dài hơn |
| Recall@10, Recall@20 | Tỷ lệ target được tìm thấy trong Top-K |
| HitRate@10 | Kiểm tra hỗ trợ; không coi là bằng chứng độc lập khi mỗi query có một target |
| CatalogCoverage@10 | Số distinct items được đề xuất / số candidates, báo riêng native và hệ thống có fallback |
| Native prediction coverage và fallback rate | Tỷ lệ query có tín hiệu cá nhân hóa hợp lệ trước fallback và tỷ lệ phải dùng Popularity |
| Candidate target coverage | Số target thuộc candidates / tổng target, kèm query full/partial/outside |
| Thời gian và tài nguyên | Tách load/join, fit/profile build, scoring, metric; peak RAM/VRAM và score throughput |

Manifest hiện tại có labels count bằng queries count ở từng fold của cả ba
ngành; Gold bảo đảm query có nhãn nên dữ liệu này có một target/query.
Recall và HitRate trùng nhau trong trường hợp này; evaluator vẫn hỗ trợ multi-label.

Báo metric trên all_queries và cùng warm_full của Gold cho mọi model. Không đổi
warm_full thành tập có content khi đánh giá content-based. Báo chẩn đoán theo
history_items=0/1/2/3–4/≥5, target repeat/novel và coverage, kèm query counts.
Nếu có mixed repeat targets thì giữ nhóm mixed riêng.

Popularity train-only theo distinct users/item là baseline và fallback chung.
Mỗi fold tính lại từ train_pairs, tie-break item_idx tăng. Báo năm hàng:
Popularity, ALS native, ALS + Popularity, Content native, Content + Popularity.
Native không có profile/factor hợp lệ trả rỗng và tính miss; bản hệ thống dùng
fallback và ghi lý do. Cold user không bị xóa khỏi all_queries. Cold item ngoài
candidates vẫn không được xếp hạng, kể cả khi cache text có vector của item đó.

Chọn ALS bằng warm-full validation NDCG@10; hòa thì Recall@10, rồi ít factors hơn.
Khóa config/seed trước test. Với kết quả cuối, lưu per-query metrics để tính
paired bootstrap 95% CI (1.000 lần, seed cố định) cho chênh lệch NDCG@10 với
Popularity và giữa ALS/content trên cùng query cohort. All Beauty test chỉ có
1.751 warm-full queries, cần đọc CI và subgroup counts cùng metric.

## 4. Notebook ALS

1. Chọn category và PHASE=validation; pin input versions, runtime/device, seed 42.
   Kiểm tra CUDA extension của implicit bằng fixture nhỏ. Dự kiến dùng implicit
   GPU; chỉ chốt/pin version sau preflight trên đúng môi trường Kaggle. Nếu GPU
   backend không khả dụng, dừng và chọn CPU rõ ràng, không âm thầm đổi backend.
2. Đọc đúng các cột user_idx/item_idx/strength từ train_pairs, dựng CSR user ×
   item float32. Shape từ mappings, nnz phải bằng số pairs; không lọc user theo
   history, rating, verified hoặc k-core. Không cần sample negative rows.
3. Dùng p_ui=1 cho pair quan sát, 0 cho phần còn lại; c_ui=1+alpha*p_ui.
   Sparse entries nhận confidence 1+alpha, các entry thiếu có confidence 1.
   Giữ alpha của implementation ở 1 nếu đã đưa confidence vào CSR để không
   nhân hai lần. Giá trị alpha/reg của Spark và implicit không mặc định tương
   đương; fixture phải kiểm chứng confidence và orientation trước full fit.
4. Chạy ngân sách validation tối đa năm configs dưới đây, fit đủ pairs mỗi lần.
   Chọn trên toàn warm-full validation (không lấy mẫu query/candidate). Sau khi
   chọn, đánh giá config tốt nhất trên all_queries và các subgroup.
5. Khi config đã khóa, PHASE=test dựng CSR/mappings mới từ test/train_pairs,
   train lại từ đầu trước 2022 và evaluate nguyên query/label test. Không tiếp
   tục tuning sau khi xem test. Khóa cả điều kiện fallback/tie-break trước test.
6. Lưu config/trial metrics sau từng trial; chỉ giữ model tốt nhất và model refit
   để tránh đầy ổ. Checkpoint fit theo iteration khi backend cho phép; nếu không,
   resume ở ranh giới trial và không hứa tiếp tục giữa iteration. Lưu factors,
   mapping/source hashes, metrics và Top-20 có query_id/item IDs.

| Trial | Factors | Reg | Confidence alpha | Iterations | Mục đích |
|---|---:|---:|---:|---:|---|
| A0 | 16 | 0,1 | 1 | 5 | Cấu hình tham chiếu gần pilot; không gọi là tái lập Spark |
| A1 | 64 | 0,1 | 10 | 20 | Cấu hình chính ban đầu |
| A2 | 64 | 0,1 | 40 | 20 | Kiểm tra trọng số positive |
| A3 | 128 | 0,1 | Tốt hơn giữa A1/A2 | 20 | Kiểm tra latent dimension |
| A4 | Tốt hơn trong A1–A3 | 0,01 | Theo cấu hình đó | 20 | Kiểm tra regularization |

Chọn tốt nhất trong cả A0–A4; ngân sách đề xuất, chưa phải best hyperparameters.
Loss hội tụ và ranking quality được ghi riêng. ALS không vượt Popularity vẫn
là kết quả hợp lệ, không đổi protocol để làm mô hình thắng.

## 5. Notebook content-based

1. Dùng cùng loader/evaluator/ranker, chọn category và fold, ghép đúng Dataset
   embedding. Đọc shards theo batch và giữ item vectors qua item_text_refs;
   không nạp toàn bộ review embeddings vào pandas/RAM/GPU.
2. Xây ma trận V theo item_idx của fold, giữ vector chuẩn hóa 384 chiều và mask
   has_text. has_text=True mà không join được vector trong bundle complete là
   lỗi dữ liệu phải dừng. has_text=False là thiếu nội dung đã biết, được báo coverage.
3. Dùng các distinct item trong train_pairs làm history, mỗi item trọng số bằng
   nhau. Profile user = L2_normalize(mean các usable history item vectors).
   Một item xuất hiện nhiều review không được tăng trọng số ở baseline này.
   Chỉ dựng profile cho query users để tiết kiệm RAM, dùng đủ lịch sử hợp lệ của
   họ; không lấy sample hoặc dùng target review/rating trong profile.
4. Score cosine = tích vô hướng profile đã chuẩn hóa với item vector đã chuẩn
   hóa. Query không có history/content hoặc profile norm gần 0 dùng chính sách
   native/fallback đã khóa. Item thiếu vector được xếp sau item có score; nếu
   cần điền đủ K thì dùng popularity cho phần còn lại, báo rõ số slot điền.
5. Chạy một cấu hình mean-distinct baseline ở validation, sau đó dựng lại profile
   với refit history trước 2022 và evaluate test. Chưa thêm recency/rating/review
   variants hoặc fine-tune encoder trong notebook đầu tiên.
6. Lưu item/profile coverage, profile/source metadata, Top-20, per-query/group
   metrics và biểu đồ so sánh với Popularity/ALS. Content không có optimizer
   train như ALS; thời gian fit ở đây là join và xây profile, ghi rõ chi phí
   encoder đã phát sinh ở giai đoạn tạo embedding trước đó.

Metadata sản phẩm là static title+description snapshot; chưa chứng minh đúng
phiên bản nội dung tại review time. Đây là giả định phải ghi trong báo cáo
content/hybrid. Model content nhận thêm thông tin, nên bảng so sánh ALS/content
không được diễn giải như một ablation chỉ khác architecture.

## 6. T4 × 2, tốc độ và checkpoint

ALS fit ban đầu dùng một GPU; hai T4 không tự gộp VRAM hay chia ALS thành
distributed training. Scoring của cả hai model có thể chia query batches cho
hai GPU; mỗi GPU giữ cùng item matrix và xử lý query khác nhau. Không song song
hai category trong lượt chuẩn. Log backend, dtype, GPU và batch sizes.

Ranker exact dùng query batches và item blocks, giữ Top-20 qua từng block.
Khởi điểm query_batch=512, item_block=16.384, float32: score block khoảng
32 MiB. Full Industrial refit item vectors khoảng 534 MiB, chưa tính overhead.
Known-user test queries của Industrial × candidates tạo hơn 100 GB score float32
nếu materialize toàn bộ, nên bắt buộc chấm theo block. Không dự đoán thời gian
full run từ throughput encode MiniLM; đo riêng scoring thực tế.

Lưu tiến độ scoring theo shard query_ids, có checksum/config/source signature;
run lại bỏ qua shard hợp lệ, rồi tính metrics streaming từ kết quả đủ. Giảm
batch/block chỉ thay cách tính, không thay tập candidates. Ghi dtype và kiểm
tra parity top-K/metrics giữa các block sizes trên fixture trước benchmark.

Artifact tách validation/test/category/model/trial. Chỉ lưu model và arrays cần
thiết, không copy lại toàn bộ input Gold/embedding vào working. Kiểm tra dung
lượng trước save. Lưu Kaggle Output/Dataset riêng tư sau mỗi category hoặc trial
dài; filesystem phiên chạy không tự là bản lưu bền vững.

## 7. Thứ tự triển khai và nghiệm thu

| Pha | Công việc | Điều kiện đạt |
|---|---|---|
| P0 | Loader Gold/embedding, config/version/signature | Đúng snapshot/category/fold, counts và embedding provenance |
| P1 | Evaluator + Popularity + exact block ranker dùng chung | Metric tính tay đúng, ties/outside/empty/cold/repeat đúng; block size parity |
| P2 | ALS notebook và preflight GPU | CSR/nnz/confidence đúng; save/load giữ ranking; backend ghi rõ |
| P3 | Full ALS validation từng ngành, khóa config | Đủ train pairs, đủ warm-full selection; best config có all-query metrics |
| P4 | Content notebook mean-distinct | Join không rơi row, history đúng mốc, missing/content coverage rõ |
| P5 | Refit/test hai model và Popularity | Chỉ config đã khóa; không chọn lại theo test; đủ query counts |
| P6 | Báo cáo và lưu artifacts | CSV metrics, per-query scores, 95% CI, coverage/fallback, timing/memory, manifest |

Dùng một module Python nhẹ cho loader/ranker/metrics, nhúng cùng source vào hai
notebook tự chứa để Kaggle không cần clone repo hoặc upload phụ thuộc rời.
Không sửa notebook pilot 04. Test tập trung vào multi-label/ties, target ngoài
candidates, null model indices, repeat items, missing content và future-history
leakage. Kiểm tra kỹ thuật trên fixture trước; không tune bằng test hoặc bằng
mẫu khác population Gold rồi gọi là full benchmark.

Nguồn API: [implicit GPU ALS](https://benfred.github.io/implicit/api/models/gpu/als.html),
[implicit CPU ALS](https://benfred.github.io/implicit/api/models/cpu/als.html),
[MiniLM model card](https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2).
Pin phiên bản cụ thể và kiểm tra CUDA ở P2; tài liệu hiện tại không bảo đảm wheel
GPU tương thích sẵn với mọi image Kaggle.

## 8. Cách dùng notebook đã triển khai

Cập nhật 07/11/2026: Run All không còn yêu cầu đổi PHASE/CATEGORY thủ công.
Mỗi ngành chạy validation, khóa best config, refit/test, rồi upload bằng KaggleHub
API sang Dataset private riêng model/ngành. Chỉ chuyển ngành sau khi API nhận
upload. Cách chạy từng phase trong các mục thiết kế trước vẫn mô tả protocol,
không phải thao tác bắt buộc trong notebook mới.

Source chung: [kaggle_recommendation.py](../notebooks/kaggle_recommendation.py).
[Builder](../tools/gold/Build-RecommendationNotebooks.py) nhúng đúng source vào
cả hai notebook; nếu regenerate bản cũ, builder sao lưu trước vào outputs/validation.
Notebook tự chứa, không cần upload helper Python, clone repo, Spark hoặc GPU
encoder model. Gold và embedding đọc trực tiếp từ input mount Kaggle.

1. Upload notebook lên Kaggle. Dùng kernel mới Python >=3.10, GPU T4 × 2,
   Internet bật. Thêm Dataset private Gold bản có đầy đủ dataset_manifest.json.
   Content thêm embedding đúng ngành đã complete. Pin numpy 1.26.4, scipy 1.14.1,
   pandas 2.2.3, pyarrow 20.0.0 cho Python 3.10–3.12, ALS dùng implicit 0.7.2.
   Python 3.13+ dùng các bản scientific stack có wheel tương thích và implicit
   0.7.3, mặc định ALS fit CPU nhưng vẫn chấm điểm bằng hai GPU. Muốn fit GPU
   với 0.7.3 cần CUDA 13/RMM: xác minh driver/runtime, bật INSTALL_ALS_GPU_073
   ở ô cài đặt rồi chạy lại. Content không cần implicit. Giữ Torch của image.
   Ô cài đặt chạy pip/import bằng đúng sys.executable; nếu lỗi phải sửa trước,
   ô train sẽ chặn khi ENVIRONMENT_READY chưa bật.
2. Chọn CATEGORIES_TO_RUN (mặc định cả ba ngành). GOLD_ROOT/EMBEDDING_ROOT tự tìm khi
   None; nhiều input cùng nguồn thì cần chọn đường dẫn hoặc BUNDLE_NAME rõ ràng.
   INPUT_VERSIONS ghi số version để audit, còn manifest hashes khóa nội dung.
3. Bật Internet, giữ PUBLISH_RESULTS=True, AUTO_RESUME=True. RESULT_OWNER mặc
   định thoandanh; sửa nếu dùng tài khoản khác. Mỗi category có handle riêng
   gold-als-results-<category>-v1 hoặc gold-content-results-<category>-v1.
   Secrets có thể dùng KAGGLE_API_TOKEN hoặc cặp KAGGLE_USERNAME/KAGGLE_KEY;
   bật quyền notebook dùng Secret. Không ghi credential vào cell/config/Output.
   KaggleHub cũng nhận auth notebook sẵn có. Kiểm tra quyền/private tất cả
   handles và embedding của các ngành được chọn trước khi train.
   Run All. ALS preflight thử fit GPU trên ma trận giả rất nhỏ trước khi đọc full
   Gold; không có CUDA thì dừng, không âm thầm đổi CPU. Có thể chọn ALS_BACKEND='cpu'
   rõ ràng; SCORE_DEVICES vẫn dùng CUDA để xếp hạng, hoặc ['cpu'] nếu muốn CPU.
4. Validation ALS chạy A0–A4 đầy đủ theo ngân sách, lưu trials.csv và best_config.json.
   Content dựng mean-distinct profiles. Cả hai xuất metric/subgroup, native và
   Popularity fallback, CI với Popularity, biểu đồ NDCG và Top-20 compact indices.
5. Sau khi validation complete, tự chạy test. ALS dùng config đã khóa để refit;
   content dựng lại profile từ test/train_pairs. Mapping refit riêng từng fold.
6. Upload Dataset sau khi cả hai phase complete; Dataset mới private, Dataset
   hiện có phải private. HTTP 401/5xx dừng. Metadata HTTP 403 được đối chiếu
   bằng danh sách Dataset của tài khoản đã xác thực (group MY, có pagination):
   chỉ cho tạo mới khi không tìm thấy đích và xác nhận owner qua Dataset đã
   thuộc tài khoản; đích có sẵn phải private. Inventory lỗi, owner không khớp
   hoặc không xác minh được thì dừng, không tự coi mọi 403 là chưa tồn tại.
   Snapshot chỉ gồm run của đúng ngành/model, có inventory SHA256, không copy
   input Gold/embedding. API tự tạo mới hoặc tạo version; submitted_processing
   chưa có nghĩa Kaggle đã xử lý xong. Upload receipt ở ngoài run để không tự
   thay đổi payload. Cùng payload/receipt hợp lệ không gửi lại; upload_uncertain
   yêu cầu kiểm tra remote trước khi xóa receipt và retry.
   AUTO_RESUME tải Dataset kết quả hiện có về, khôi phục recommendation_results.
   Có thể Save Version/Output nếu bị ngắt trước upload; RESUME_ROOT nhận Output cũ.
   Checkpoint model/trial và
   scoring kiểm tra nguồn/config/hash trước khi dùng lại; thiếu các file của bản
   mới trong thư mục output có thể được bổ sung từ resume, không overwrite tiến
   độ mới hơn. Fit bị dừng giữa iteration chạy lại trial đó, không resume giữa fit.
7. Tự chuyển ngành tiếp theo. Phase complete có inventory artifact đúng SHA/source
   được bỏ qua mà không load/train/scoring lại. Báo cáo cuối gom cả hai phases
   và mọi ngành. Mỗi category/algorithm có fingerprint riêng; không đổi seed/backend
   giữa resume. Source hash thay đổi sẽ tạo run khác; Output notebook phiên bản
   cũ không được tự coi là checkpoint tương thích với source mới.

Kiểm tra local: fixture content chạy validation/test → snapshot API mock →
restore phiên mới không train lại; thứ tự ba ngành và validation failure gate;
upload lỗi giữ local/receipt; privacy/auth errors; schema và source đồng bộ hai
notebook. Chưa upload Dataset thật hoặc benchmark lại full Kaggle trong thay đổi này.

Scoring dùng exact block Top-K, tie-break item_idx; luôn giữ repeat candidates.
Mỗi shard query tối đa 4.096 queries, tính trong batch 512 và item block 16.384.
Giảm QUERY_BATCH/ITEM_BLOCK khi cần, giữ score_shard_queries và nguồn để resume.
Chỉ giữ factors trial ALS tốt nhất, còn metric/checkpoint ranking giữ cho mọi trial.
Root run.json ghi hardware/versions; Monitor đo peak RSS và VRAM toàn thiết bị
theo chu kỳ 1 giây, không phải hard quota hoặc VRAM độc quyền của tiến trình.

Đầu ra trong recommendation_results/<algorithm>_<category>_<signature>/:

- validation/ hoặc test/: mappings, ranking checkpoints, evaluation/metrics.csv,
  các file per_query.parquet và top20.parquet, confidence_vs_popularity.json,
  tài nguyên theo phase và phase_complete.json.
- ALS: trials/, trials.csv, best_config.json; factors float32 NPY kèm hash và
  fit manifest, test/model chứa factors refit. Source/seed/confidence/fold được ghi rõ.
- Content: item_vectors.npy, query_profiles.npy, profile_query_positions.npy và
  content_profiles.json (profile chỉ lưu cho query có content hợp lệ).

Top-20 lưu item_indices; giải mã với mappings/items của chính phase đó. Report
fallback_rate tính cả điền một phần danh sách, per-query có fallback_reason để
phân biệt cold_user, thiếu profile/factor và partial_list_fill. Native score
coverage không được thay bằng coverage của hệ thống đã fallback.

Notebook content có ô tùy chọn ALS_EVALUATION_ROOT để so trực tiếp với Output
ALS cùng category/Gold/fold, ghép theo query_id và tính paired CI trên bản hệ
thống có fallback. Không so hai kết quả khác population hoặc khác nguồn.

Kiểm tra: [test_kaggle_recommendation.py](../tests/test_kaggle_recommendation.py)
bao phủ metric tính tay multi-label/outside/missing predictions, ties/negative
cosine/missing content, exact block parity, checkpoint corruption, future
history, validation/test gate, resume qua phiên, notebook schema/source sync
và thực nghiệm ALS CPU thật 5 configs + cached model + refit/test. Loader còn
được thử trên toàn fold validation All Beauty thật, counts khớp Gold. Không có
GPU T4 tại máy local để đo throughput hoặc xác nhận đường CUDA; notebook thử
GPU trong runtime Kaggle trước khi fit toàn bộ.

## 9. Kế hoạch notebook 09 NCF và 10 DeepFM

Ngày lập: 06/11/2026; cập nhật ngày 07/11/2026 theo yêu cầu triển khai tiếp sau
ALS và Content-based. Đã triển khai source và hai notebook; kiểm tra training
CPU trên fixture ở mục 10, chưa benchmark full trên Kaggle. Phạm vi theo processing.md mục 6.2 và gold_contract.json:
NCF/NeuMF thuần ID và DeepFM D0/D1/D2; không phải hai bản NCF nông/sâu.
Người dùng xác nhận đã chạy được ALS và Content-based. Khi lập bảng so sánh,
kiểm tra lại artifacts cùng ngành/fold/source; xác nhận chạy được chưa cho biết
đã đủ validation/test của cả ba ngành. Chưa có bằng chứng mô hình neural sẽ
tốt hơn baseline.

### 9.1. Hai notebook và những gì tái sử dụng

| Notebook | Lượt thực nghiệm | Input |
|---|---|---|
| 09_kaggle_ncf.ipynb | NCF_ID, cụ thể NeuMF train từ đầu | Gold training, không cần text embeddings |
| 10_kaggle_deepfm.ipynb | Chọn VARIANT D0, D1 hoặc D2 mỗi lượt | Gold; D1/D2 thêm bundle embedding cùng ngành |

Chọn CATEGORIES_TO_RUN, VARIANTS_TO_RUN (DeepFM), TRAIN_DEVICE và SCORE_DEVICES
rõ ràng. Orchestrator chạy tuần tự từng ngành, bắt đầu All Beauty; mỗi model/
variant chạy validation → khóa config/epoch → refit/test → lưu kết quả private,
theo cách dùng của notebook 07/08. Có chế độ chỉ validation và resume từng
phase; test chỉ mở khi best-config manifest hợp lệ. CPU dùng để kiểm tra kỹ thuật; full CPU chỉ chạy
khi người dùng chủ động chọn sau khi đo throughput. Một GPU train; scoring
có thể chia query cho một/hai GPU, mỗi GPU giữ bản model riêng. Không mặc định
hai GPU tồn tại hoặc có tổng VRAM dùng chung; không thay Torch/CUDA có sẵn
trong image và không âm thầm đổi backend khi CUDA lỗi.

Tái sử dụng loader/checksum Gold, fold mappings, Popularity, evaluator,
tie-break, cohort và paired bootstrap của kaggle_recommendation.py. Phần mới
ở notebooks/kaggle_neural_recommendation.py và builder notebook tương
ứng; mỗi notebook vẫn tự chứa source cần thiết để không upload Python rời.
Không viết lại metric hoặc thay đổi ALS/content đang có. Không thêm framework
recommendation khi PyTorch và các dependencies hiện tại đã đủ.

### 9.2. Kiến trúc và ý nghĩa từng lượt

| Lượt | Kiến trúc/feature được phép | Câu hỏi |
|---|---|---|
| NCF_ID | NeuMF: nhánh GMF và MLP có bảng user/item embedding riêng, nối hai nhánh rồi xuất một logit | Quan hệ phi tuyến từ ID có giúp hơn ALS/Popularity? |
| D0 | Linear + FM bậc hai + MLP; FM và MLP dùng chung ID embeddings | Với cùng đầu vào ID, DeepFM khác NeuMF ra sao? |
| D1 | D0 + lớp chiếu item vector MiniLM 384 chiều và missing flag vào nhánh Deep | Item text thêm giá trị gì so với D0? |
| D2 | D1 + rating-history features và lớp chiếu mean review profile quá khứ vào nhánh Deep | Nhóm phản hồi cá nhân thêm giá trị gì so với D1? |

NeuMF không pretrain GMF/MLP riêng trong lượt đầu; ghi rõ đây là lựa chọn triển
khai, không tuyên bố tái lập toàn bộ paper NCF. DeepFM D1/D2 là phần mở rộng
text của nhóm: giữ backbone ID FM/Deep, không đưa 384 tọa độ text thành 384
categorical fields. Không gọi đây là bản nguyên xi DeepFM gốc dùng cho CTR.
Mục tiêu vẫn là tương tác review tiếp theo; output sigmoid chỉ phục vụ objective
positive/negative đã sampling, không diễn giải thành xác suất mua hàng thực tế.

MiniLM được giữ frozen, đọc vectors từ Dataset đã có; không encode lại hoặc
fine-tune encoder. D1/D2 dùng cùng item vector title+description với content.
Không thêm giá, category/store, average_rating, rating_number hoặc sentiment.
D2 thay rating và review cùng lúc nên chỉ kết luận về cả nhóm feature này.

### 9.3. Đầu vào và feature D2 còn thiếu

Gold exporter đã khai báo train_examples, history_events, item_text_refs,
review_text_refs và history_ratings(record_hash,rating). Loader mới phải kiểm
tra các bảng đó thực sự có trong manifest Dataset gắn vào Kaggle; thiếu thì
dừng với tên bảng, không tự đi đọc Silver/raw hoặc thay split.

NCF/D0 đọc whitelist ID/anchor cần thiết. D1 kiểm tra đúng export hash, encoder,
revision, chiều/dtype/checksum như content. D2 mở rộng loader để lấy cả review
vectors qua text_id; refs record_hash quyết định số events, không dedup lịch
sử chỉ vì hai events có cùng text_id. Cần vector mà bundle complete không có
là lỗi dữ liệu; has_text=False là thiếu nội dung hợp lệ, giữ mẫu và missing flag.

Feature D2 theo đặc tả đã chốt: rating_count_valid, mean_rating, tỷ lệ rating
1–2 và 4–5 trên valid ratings; mean usable review vectors rồi L2-normalize;
text/rating counts và missing flags. Chốt schema cùng cách điền trong feature
manifest trước full run. Rating thiếu không được hiểu là 0 sao; dùng giá trị
điền trung tính kèm flag, mọi scaler/imputer học từ train examples của fold.

Train anchor là first_pair_time của từng positive: chỉ lấy events có
event_time < prediction_time, loại toàn bộ events đồng timestamp. Không dùng
profile cuối kỳ train cho tất cả mẫu; không dùng review/rating của chính target.
Validation profile chỉ lấy trước 2021; test dựng lại trước 2022. Thay rating/text
target hoặc future events trong fixture không được làm đổi feature của anchor.
Với query repeat, những review thực sự xảy ra trước cutoff vẫn là history hợp lệ.

Dựng prefix aggregates theo user/timestamp group: xuất feature cho anchors
trước khi cộng events của group. Đọc vectors theo shard, dùng cache/memmap có
index khi cần; không join mỗi positive với toàn bộ history và không nhân bản
384 chiều cho từng negative. Cùng user/time dùng một feature record và các
negative chỉ đổi candidate item. Cache gắn source/fold/schema/hash, tính dung
lượng trước khi tạo: bốn triệu profile float32 × 384 đã khoảng 6,1 GB thập phân,
chưa tính vector nguồn, indices và checkpoint. Không coi cache disk là RAM miễn phí.

### 9.4. Positives, negatives và objective chung

- Dùng đủ distinct train pairs/positive anchors, kể cả history rỗng; không k-core,
  không lọc rating cao, không tự random split lại Gold.
- Mỗi positive có 4 negatives uniform từ train candidates, loại toàn bộ train
  items đã quan sát của user trong chính fold. Không nhìn labels/future split
  để loại negative; đây là sampler train-only đã chốt trong thiết kế hiện tại.
- Sampler tái lập theo seed=42, epoch, example_id; cùng positives và negatives
  cho NCF/D0/D1/D2, không phụ thuộc thứ tự DataLoader hoặc variant.
- Lấy negatives không trùng trong một anchor nếu đủ pool; pool nhỏ thì dùng
  min(4, số item hợp lệ) và ghi số thực tế. Pool rỗng giữ positive, không lặp vô
  hạn hay xóa user. Không materialize bảng users × items hoặc 5 lần feature D2.
- BCEWithLogitsLoss trên positives/negatives, không sigmoid trước loss; Adam,
  FP32 làm mốc. Log loss để chẩn đoán, chọn model bằng ranking validation.

### 9.5. Scoring neural là phần cần triển khai riêng

Ranker hiện tại nhận query/item vectors rồi nhân ma trận. Không dùng phép đó
để thay logit NeuMF/DeepFM vì nhánh MLP phụ thuộc cặp user–item. Viết scorer
theo query shard, candidate block và pair microbatch, tính logit đúng model rồi
merge Top-20. Có thể tái sử dụng top-K/tie-break và quy tắc checkpoint hiện có.
Không tạo tensor [toàn bộ queries, toàn bộ items, hidden_dim].

Pilot bắt đầu query_batch=8, item_block=1024, pair_batch=8192; đây là cấu hình
đo, chưa phải tối ưu. Logit FP32, model.eval(), inference không gradient.
Tách cache projection item/review bất biến trong một checkpoint nếu kiểm chứng
parity; cache phải bị invalidated khi trọng số projection đổi. Checkpoint
ranking chứa model-state hash, variant, fold, feature hash và query IDs; checkpoint
epoch/trial khác nhau không được dùng chung kết quả scoring.

Đánh giá exact trên mọi candidates, giữ seen items và tie-break item_idx tăng.
Không đổi sang sampled-negative hoặc ANN để làm benchmark nhanh hơn. Trước
full tuning đo số pairs/giây trên một shard query thật với đầy đủ candidates,
tách khỏi thời gian fit. Nếu thời gian không khả thi trong phiên Kaggle, lưu
tiến độ và chạy tiếp qua phiên; mọi thay đổi ngân sách tuning phải khóa trước
full search và áp dụng nhất quán, không giảm tập query/candidate của kết quả cuối.

### 9.6. Ngân sách train/tuning ban đầu (đã thay bằng mục 11)

Ngân sách trước bản compact: ba cấu hình cho mỗi NCF_ID/D0/D1/D2 và mỗi ngành, giữ cùng danh sách
giữa D0/D1/D2. ID dim là chiều mỗi bảng embedding; NeuMF có hai bộ bảng.

| Trial | ID dim | MLP hidden | Learning rate | Weight decay |
|---|---:|---|---:|---:|
| N0 | 16 | 64 → 32 → 16 | 0,001 | 0,00001 |
| N1 | 32 | 64 → 32 → 16 | 0,001 | 0,00001 |
| N2 | 32 | 64 → 32 → 16 | 0,0003 | 0,00001 |

Các projection text/review xuất 32 chiều; dropout=0 cho lượt đầu để giảm biến
số. Train batch 4096 cặp sau sampling, có thể giảm sau đo RAM/VRAM và ghi rõ.
Tối đa 5 epochs, exact warm-full validation sau epochs 1, 3, 5. Chọn checkpoint
bằng NDCG@10, rồi Recall@10, số tham số ít hơn và epoch sớm hơn khi hòa.
Giữ cùng ngân sách tìm kiếm, nhưng best config có thể khác giữa các variants;
báo cả trial table để không che tác động lựa chọn cấu hình khi diễn giải ablation.
Nếu pilot cho thấy cần đổi lịch/budget, cập nhật manifest trước full tuning.

Fit một GPU và scoring có thể khác devices nhưng phải ghi rõ. Preflight tính
số tham số từ distinct users/items, số bảng, dim; FP32 Adam cần khoảng 16 bytes
mỗi tham số cho weights, gradient, hai moments, chưa tính activations và optimizer
intermediates. NeuMF dùng hai bộ embeddings nên không ước lượng như một bảng.
Đo peak sau cả backward/optimizer step; giảm batch không giảm bảng tham số.
Chỉ mở AMP/distributed sau khi baseline và parity thực sự cho thấy cần thiết.

Sau validation, lưu config + selected epoch và đánh giá best checkpoint trên
all_queries/subgroups. Test refit từ đầu với mapping mới, cùng hyperparameters
và đúng selected epoch; không dùng test để early-stop hay chọn lại cấu hình.
Model ID có user unknown dùng Popularity theo protocol hiện tại; D1/D2 vẫn
giữ query/item thiếu text với missing features. Chưa thử OOV personalization
hoặc cold-item candidates: ngoài phạm vi benchmark đang khóa.

### 9.7. Checkpoint và đầu ra

Root riêng algorithm/variant/category/source signature. Lưu model state,
optimizer state, epoch đã hoàn tất, RNG states/sampler seed, best config,
feature schema/scaler, source/package versions, dataset versions và mappings.
Resume ở ranh giới epoch; giữa epoch bị ngắt chạy lại epoch đó từ checkpoint,
không hứa tiếp tục từ batch bất kỳ. Chỉ giữ last và best cần resume, dọn model
trial không thắng sau khi result complete, không xóa metrics/source evidence.

Đầu ra tương thích 07/08: trials.csv, loss curve, metrics.csv, Top-20 compact
indices, per-query metrics, fallback reasons, confidence intervals, load/feature/
fit/scoring resource logs. Phải phân biệt cold_user với feature thiếu và scorer
lỗi; output NaN/Inf là lỗi phải dừng, không dùng fallback để che train hỏng.
So sánh trực tiếp phải cùng Gold/category/fold/query population. Báo history
bins 0/1/2/3–4/≥5, warm_full và all_queries; không gọi D1/D2 là đã giải quyết
cold-start chỉ vì thêm text. Chưa thêm deploy/UI vào đợt notebook này.

### 9.8. Thứ tự triển khai và nghiệm thu

| Bước | Công việc | Điều kiện đạt |
|---|---|---|
| P0 | Loader train_examples/refs/ratings, shared sampler, ngân sách bộ nhớ | Counts/keys/hash đúng; các variants nhận cùng positives/negatives |
| P1 | NeuMF + DeepFM D0, neural block ranker và notebook builder | Forward/loss/gradient hữu hạn; save/load parity; exact Top-K bằng brute force fixture |
| P2 | NCF/D0 All Beauty pilot trên đủ candidates, đo chi phí | Full one epoch khả thi; scoring throughput/RAM/VRAM có log; không đổi population benchmark |
| P3 | D1 đọc item embeddings đã có | Missing flag đúng; không encode lại; kiểm tra ghép keys/source |
| P4 | D2 prefix features và review embedding loader | Strict-before/timestamp ties/first positive/missing/repeated text tests đạt; không target leakage |
| P5 | Full validation bốn lượt trên All Beauty, rồi Games và Industrial | Đủ ba trials/lịch đã khóa; best có all-query metrics/CI và checkpoint |
| P6 | Refit/test sau khi khóa từng lượt | Test gate/source/feature schema đúng; không tune trên test; lưu Output riêng tư |

Fixture bổ sung tập trung vào sai số có thể làm sai kết luận: sampler không
lấy observed pairs, deterministic qua resume/variants; FM tính tay và NeuMF
forward; prefix strict-before kể cả ties/negative anchors; target/future mutation
invariance; missing text/rating và event multiplicity; scoring block/microbatch
parity với brute force; checkpoint corruption/source/variant mismatch; một lượt
CPU nhỏ end-to-end train → validation → resume → refit/test. Các kiểm tra này
không thay benchmark full thật. Không khẳng định CUDA hoặc thời gian full trước
khi chạy pilot trên đúng Kaggle runtime.

Nguồn kiến trúc: [NCF/NeuMF](https://arxiv.org/abs/1708.05031),
[DeepFM](https://arxiv.org/abs/1703.04247). Ngân sách Adam cần tính thêm
intermediates theo [PyTorch Adam](https://docs.pytorch.org/docs/stable/generated/torch.optim.Adam.html).
Negative sampling, temporal protocol, extensions D1/D2 và ngân sách trial ở trên
là thiết kế cho đồ án, không phải tuyên bố tái lập nguyên bản hai paper.

### 9.9. Bố cục notebook và thứ tự chạy thực tế

Hai notebook dùng cùng bố cục để người chạy chỉ cần thay cấu hình đầu vào.
Notebook 10 mặc định liệt kê D0/D1/D2 nhưng chạy tuần tự, mỗi variant có model,
checkpoint và best-config riêng; không dùng checkpoint D0 để train tiếp D1/D2.

| Nhóm cell | Nội dung và kết quả cần thấy |
|---|---|
| 1. Giới thiệu và runtime | Bài toán, protocol, GPU/dependencies; kiểm tra import và một forward/backward nhỏ trên device đã chọn |
| 2. Cấu hình | Categories/variants, Dataset roots/versions, seeds, trial budget, train/score batch sizes, output và resume roots |
| 3. Source tự chứa | Nhúng source dùng chung và neural source theo mẫu builder hiện có; đồng bộ với file Python được kiểm tra ở local |
| 4. Preflight dữ liệu | Manifest/hash, mappings, positives/anchor counts, negative pool; D1/D2 kiểm tra bundle, D2 thêm refs/ratings và ngân sách cache |
| 5. Chuẩn bị features | NCF/D0 chỉ ID; D1 ánh xạ item vectors; D2 tạo prefix features/cache theo fold và anchors, xuất coverage/missing report |
| 6. Validation | Chạy trials/lịch epoch đã khóa, lưu loss và exact warm-full metrics; chọn best rồi đánh giá all_queries/subgroups |
| 7. Refit và test | Khởi tạo model với mappings refit; dùng config/epoch đã chọn, tạo lại D2 features/scaler từ phần train của fold test; chấm đúng một cấu hình đã khóa |
| 8. Kết quả và lưu bền vững | Bảng so Popularity/ALS/Content/NCF/D0/D1/D2, per-query CI, thời gian/RAM/VRAM; lưu Output và Dataset private theo model/variant/ngành |

Thứ tự viết code: shared sampler + neural scorer → notebook 09 NeuMF → D0
trong notebook 10 → D1 → D2. Sau mỗi phần, chạy fixture nhỏ trước khi mở full
data. All Beauty là lượt đầu để đo khả năng chạy và hoàn thiện resume; sau đó
mới chạy Video Games và Industrial and Scientific. Mỗi ngành dùng đủ pairs,
queries và candidates theo protocol cho kết quả chính.

Ngân sách ban đầu tối đa 3 trials × 3 mốc đánh giá × 4 model/variant × 3 ngành,
tức 108 lượt exact warm-full validation nếu chạy hết lịch. Ngoài ra còn lượt
all-query validation của best checkpoint và test. Do đó chưa chốt số giờ cho
toàn bộ thí nghiệm: pilot phải đo cả một epoch train và scoring shard trên đủ
candidates. Dùng throughput đo được cùng số query/candidates để dự toán từng
ngành và dung lượng checkpoint, rồi khóa ngân sách trước full tuning.
Từ bản compact ngày 08/11, ngân sách mặc định được thay bằng mục 11.

Kế hoạch hoàn thành khi hai notebook Run All trên Kaggle theo cấu hình đã chọn,
resume không đổi kết quả, D0/D1/D2 giữ cùng mẫu và sampler, D2 vượt kiểm tra
strict-before, và artifacts cuối đủ để so sánh các model trên cùng Gold/fold.

## 10. Vận hành notebook NCF và DeepFM đã triển khai

[09 NCF](../notebooks/09_kaggle_ncf.ipynb) và
[10 DeepFM](../notebooks/10_kaggle_deepfm.ipynb) được tạo bằng
[Build-NeuralNotebooks.py](../tools/gold/Build-NeuralNotebooks.py), nhúng source
chung và [neural source](../notebooks/kaggle_neural_recommendation.py).
Builder sao lưu notebook cũ trước khi tạo lại. Không cần Spark/HDFS trên Kaggle.

Từ ngày 08/11/2026, mặc định compact_full_users: một trial C8, ID dim 8,
MLP [32,16], projection 16, max_epochs=20 và early stopping theo full warm_full
NDCG@10. Thông số, lịch validation và resume ở mục 11; giữ đủ user/item/pairs.

1. Chọn T4 × 2, Internet ON; gắn gold-training-v2. DeepFM D1/D2 cần các Dataset
   embedding ở mục 2; D0 và NCF không cần. Bundle phải complete và đúng hash.
2. Sửa RESULT_OWNER, INPUT_VERSIONS và các roots nếu auto-discovery không rõ.
   Secrets và quyền Secret dùng như 07/08. Không ghi token trực tiếp vào cell.
3. Mặc định chạy CATEGORIES_TO_RUN tuần tự cả ba ngành; notebook 10 chạy
   VARIANTS_TO_RUN=['D0','D1','D2'] tuần tự trong mỗi ngành. D0/D1/D2 train từ
   đầu, có trial/selection/model riêng. Có thể chọn một ngành/variant mỗi phiên.
4. TRAIN_DEVICE='cuda:0'; SCORE_DEVICES=['cuda:0','cuda:1']. Một GPU thì đổi
   score_devices thành ['cuda:0']. CPU phải chọn rõ cả hai; không tự fallback.
5. PHASES=['validation','test'] chạy đủ pipeline và publish sau mỗi variant/ngành.
   Chỉ validation: PHASES=['validation'], PUBLISH_RESULTS=False, dùng Save Version/
   Output. Test-only cần khôi phục validation hoàn tất cùng source/config.

Train dùng một positive mỗi distinct pair và tối đa 4 uniform negatives không
trùng/anchor. Pool nhỏ dùng đủ phần có thể lấy, pool rỗng vẫn giữ positive.
Sampler độc lập model/row order theo seed, epoch, example_id; không nhìn labels
để loại negatives. Logs ghi positive/negative counts thực tế mỗi epoch.

D2 đưa đúng bảy thống kê vào Deep: rating_count_valid, mean_rating,
low_rating_fraction, high_rating_fraction, review_text_count, rating_missing,
review_missing; thêm projected review profile 384 → 16 và item projection
384 → 16. Counts log1p, continuous statistics standardize từ train anchors;
flags giữ binary. Mean rating thiếu điền 3 kèm flag, review thiếu điền zero.
Một text_id có thể dùng bởi nhiều events, mỗi event vẫn đóng góp vào history.
Feature store dùng memmap theo unique user/time; negatives tham chiếu cùng
feature row với positive, không nhân bản dense profiles trên disk.

Output tại /kaggle/working/neural_results/<variant>_<category>_<signature>/:

- best_config.json khóa trial/epoch và best checkpoint hash; trials.csv giữ
  kết quả chọn model cùng stop_reason/stopped_epoch/selected_epoch.
  trials/<trial>/ có trial.json, loss.csv, validation.csv, fit_budget.json
  và checkpoint/ranking. Trial không thắng được bỏ model payload sau khi trial
  complete, giữ metrics/loss/ranking. Trial thắng giữ best.pt và last.pt. Khi
  train chưa xong, last có optimizer/RNG để resume; trial/refit complete thu gọn
  last còn weights/epoch/training state vì lịch train đã khóa, giảm dung lượng snapshot/upload.
  Training state lưu cả loss history và bộ đếm early stopping; JSON mirror
  có thể được phục hồi từ last checkpoint khi ngắt giữa hai lần ghi.
- validation/ và test/ giữ mappings của chính fold, metrics/Top-20/per-query/CI,
  checkpoint scoring và resource logs. Test/model/last.pt là model refit đúng
  selected epoch. Model state dùng torch.load(weights_only=True), kiểm tra
  SHA256 và source signature trước restore.
- D1/D2 giữ item vectors/mask theo item_idx. D2 trong lúc chạy giữ prefix
  features/scaler; sau khi scoring và metrics xong chỉ giữ query_stats.npy,
  query_reviews.npy, query_keys.parquet cùng scaler/schema/source, thu gọn train prefix cache.
  Nếu cần chạy lại phase chưa hợp lệ, cache này được dựng lại từ Input.
- phase_complete.json chứa artifact hashes; phase complete hợp lệ được bỏ qua
  khi resume. Run/source mismatch và checkpoint hỏng bị từ chối. Epoch đang
  dở chạy lại từ checkpoint cuối epoch; scoring tiếp tục theo query shard.

RESULT_DATASETS dùng khóa VARIANT/Category, mặc định slug
<owner>/gold-<variant>-results-<category>. Mỗi Dataset riêng chứa cả validation
và test, gồm tệp .pt. Auto-resume tải Dataset kết quả đã complete. Với phiên
đang dở, gắn Output cũ và đặt RESUME_ROOT vào thư mục chứa các run, không đặt
vào một run đơn lẻ. RESUME_ROOT được ưu tiên hơn auto-resume từ Dataset.
Upload receipt giữ quy tắc private/uncertain của 07/08.

Chưa có đo full trên T4. Scoring exact cần chạy mạng cho mọi pair; throughput
được ghi theo shard để dự toán, không thay bằng sampled evaluation. Disk guard
tính cả cache/model/snapshot upload; ba ngành và nhiều variants có thể cần
nhiều phiên. Output đã upload vẫn được giữ local, không tự xóa cả run để lấy
chỗ. Khi hết budget, lưu Output rồi tiếp tục với category/variant phù hợp trong
phiên mới. Không gọi hai GPU là một vùng VRAM chung.

Kiểm tra offline, PowerShell host tại project/:

```powershell
python tools/gold/Build-NeuralNotebooks.py
python -m unittest discover -s tests -p test_kaggle_neural_recommendation.py -v
```

Fixture dùng Parquet thật và optimizer CPU cho cả bốn variants, bao phủ
strict-before/timestamp ties, mutation target/future, missing/multiplicity,
sampler, FM tính tay, block scoring so brute force, checkpoint interruption/
resume, validation/test gate, publish/auto-resume giả lập và source notebook.
Không dùng fixture để suy chất lượng mô hình hoặc thời gian full Kaggle.

## 11. Kế hoạch giảm tham số và early stopping cho notebook 09–10

Ngày lập và triển khai: 08/11/2026. Notebook/source đã áp dụng profile
compact_full_users và early stopping; các thông số dưới đây là mặc định hiện
hành thay ngân sách cũ trong mục 9.6. Giữ nguyên Gold, positives/sampler,
temporal features, candidates và evaluator. Profile shared rare-user ở mục
11.4 vẫn là thí nghiệm tùy chọn chưa triển khai. Chưa benchmark full trên T4.

### 11.1. Mục tiêu và cơ sở dữ liệu

Giảm bảng tham số và chi phí chấm từng pair, tập trung ngân sách vào một model
nhỏ được train đủ lâu thay vì ba lượt ngắn. Không đồng nhất số user với số
chiều cần thiết; chiều phù hợp phải được kiểm chứng trên validation.

Đã đếm trực tiếp train_pairs của Gold export đã pin, đối chiếu manifest/hash,
counts và toàn bộ user mapping. Trong phần refit trước 2022, tỷ lệ user có
đúng một distinct item: All Beauty 93,45%, Video Games 73,42%, Industrial
78,90%. Bảng đầy đủ theo sáu bins và hai folds ở
[báo cáo số lượng user](../outputs/analysis/user-history-20261108/USER_HISTORY_REPORT.md).
Đây là số distinct items/user train, không phải số raw reviews hoặc chỉ nhóm
query warm_full. Không dùng tương tác sau cutoff để quyết định ngưỡng user.

### 11.2. Profile mặc định: compact_full_users

| Tham số | Hiện tại | Đề xuất mặc định |
|---|---|---|
| Trials mỗi model/variant/ngành | 3 | 1 |
| User/item ID dimension | 16 hoặc 32 | 8 |
| MLP hidden | [64,32,16] | [32,16] |
| Projection item/review D1/D2 | 32 | 16 |
| Learning rate | 0,001 hoặc 0,0003 | 0,001 |
| Adam weight decay | 0,00001 | 0,00001 |
| Train dtype | FP32 | FP32 |
| Negatives/positive | 4 | 4, cùng sampler hiện tại |
| Trần epoch | 5 | 20 |
| Full warm_full validation | [1,3,5] mỗi trial | [1,3,6,9,12,15,18,20] |
| Early stopping | Chưa có | NDCG@10 plateau, theo mục 11.3 |

NCF vẫn giữ hai bộ GMF/MLP embeddings riêng, DeepFM vẫn dùng ID embeddings
chung cho FM/Deep. D0/D1/D2 cùng backbone nhỏ và cùng budget; D1/D2 chỉ thêm
các feature đã chốt. Projection thay chiều không thay vector MiniLM nguồn
384 chiều; raw vectors và prefix features không nhỏ đi theo ID dimension.
Giữ đủ mọi train pair/user/item, không thêm k-core hoặc đổi cohort benchmark.

Cấu hình dự phòng dim=16, cùng MLP/projection/lr, có thể bật thành profile so
sánh hai trials từ trước khi tuning. Mặc định không tự chạy dự phòng sau mỗi
ngành và không chọn thêm trial dựa trên test. Dim=4 là thí nghiệm mở rộng sau
khi đã có kết quả compact, không cộng vào ngân sách mặc định. Thay đồng thời
dim và MLP nhằm tối ưu tổng chi phí; không diễn giải kết quả như ablation chỉ
đổi ID dimension.

Ước tính trọng số FP32 cho phần refit/test, giữ đủ user/item:

| Ngành | NCF dim32 hiện tại | NCF compact | D2 dim32 hiện tại | D2 compact |
|---|---:|---:|---:|---:|
| All Beauty | 171,03 MB | 42,76 MB | 88,32 MB | 24,11 MB |
| Video Games | 651,93 MB | 162,98 MB | 336,28 MB | 91,73 MB |
| Industrial and Scientific | 812,52 MB | 203,13 MB | 419,08 MB | 114,32 MB |

MB thập phân, chỉ tensor weights, chưa tính optimizer/gradient, feature data,
activations hoặc overhead tệp. Cột hiện tại lấy cấu hình dim32, không giả định
đây là cấu hình đã thắng validation. Số tham số NCF giảm khoảng 75%, D2 khoảng
73% so với cấu hình đó. Không suy thời gian train giảm cùng một tỷ lệ.

### 11.3. Quy tắc epoch và early stopping

| Control | Giá trị đề xuất | Ý nghĩa |
|---|---:|---|
| MAX_EPOCHS | 20 | Trần ngân sách, không phải mục tiêu phải chạy đủ |
| EARLY_STOP_START_EPOCH | 6 | Trước epoch 6 chỉ theo dõi và lưu best, chưa cộng bad-check counter |
| EARLY_STOP_PATIENCE | 2 | Hai lần full validation đủ điều kiện liên tiếp không cải thiện đáng kể |
| EARLY_STOP_MIN_DELTA | 0,00001 | Mức tăng tuyệt đối NDCG@10 tối thiểu để reset counter |
| MONITOR | warm_full NDCG@10 | Cùng cohort và full candidates của Gold |

Patience đếm số lần đánh giá, không đếm số epochs. Với lịch ở trên, model có
thể dừng sớm nhất ở epoch 9 nếu hai lần kiểm tra tại 6 và 9 đều không cải
thiện. Không full validation mỗi epoch chỉ để có early stopping.

Sau mỗi epoch, ghi loss, số positives/negatives, optimizer steps, thời gian
và peak memory; sau mỗi mốc validation, lưu NDCG/Recall và quyết định stop.
Train BCE có negatives được lấy lại mỗi epoch nên là tín hiệu chẩn đoán;
không dừng chỉ vì một epoch train loss tăng hoặc giảm rất ít. NaN/Inf là lỗi
phải dừng với trạng thái failed, không phải early stopping thành công.

Phân biệt hai trạng thái best:

- best checkpoint thực tế: luôn giữ ranking tốt nhất theo NDCG@10, rồi
  Recall@10 và epoch sớm hơn khi hòa; kể cả mức tăng nhỏ hơn min_delta.
- significant-best dùng cho patience: chỉ reset khi NDCG tăng ít nhất min_delta
  so với mốc cải thiện đáng kể trước đó. Mức tăng nhỏ tích lũy có thể vượt
  ngưỡng này; không bỏ mất best weights vì ngưỡng dừng.

Ví dụ: epoch 3 có best NDCG, epoch 6 và 9 đều không cải thiện đáng kể → dừng
ở 9, dùng best checkpoint epoch 3. Refit/test train ba epochs, không train chín.
Nếu chạm epoch 20 khi metric vẫn tăng, ghi stop_reason=max_epochs và cảnh báo
ngân sách đã hết; không ghi là model đã hội tụ. Mọi quyết định mở rộng ngân
sách phải được lập trước test, không tự tiếp tục tuning sau khi xem test.

### 11.4. Giảm số user embedding: thí nghiệm riêng sau compact

Profile compact_full_users là bước đầu để đo riêng hiệu quả của model nhỏ.
Profile tùy chọn compact_shared_rare_users mới thử MIN_USER_ITEMS=2 theo
distinct train pairs của từng fold. Đếm trên toàn train, không dùng mức lịch
sử của future labels hoặc chỉ các query có kết quả thuận lợi.

- Giữ user mapping Gold gốc cho sampler, history, cohort và audit. Tạo thêm
  mapping user_idx → embedding_row; user có ít nhất hai item dùng hàng riêng,
  user ít hơn dùng một hàng chung. NCF áp dụng cho cả hai bảng user, DeepFM
  áp dụng cho user embedding và user bias; item mapping giữ nguyên.
- Giữ mọi train pair. Negative sampler vẫn loại observed items theo user gốc,
  không theo user bucket chung; D2 vẫn lấy history của từng user gốc.
- User thực sự unknown ở query vẫn dùng Popularity theo protocol. User biết
  nhưng dùng shared row phải được ghi shared_user_id, không đổi thành cold
  trong Gold. Với NCF/D0/D1, cùng hàng ID không còn phân biệt danh tính giữa
  các user đó; D2 có thể khác nhờ feature lịch sử. Báo riêng tỷ lệ có embedding
  ID riêng, shared-row coverage và metric theo history bins.

Giả định compact + shared row, trọng số refit NCF khoảng 8,91 / 48,89 / 61,26
MB cho All Beauty / Games / Industrial; D2 khoảng 5,07 / 27,56 / 34,52 MB.
Đây là tính toán dung lượng, chưa có bằng chứng chất lượng giữ nguyên. Không
xóa nhóm một item: nếu xóa, All Beauty chỉ giữ 14,53% train pairs refit.

Chỉ chạy profile này như experiment riêng với source signature riêng, cùng
queries/candidates/fallback của Gold. Không thay mặc định compact bằng kết
quả đã lọc cohort và không trộn metric với baseline full-user. Không mở thêm
ngưỡng 3/4/5 trong lượt triển khai đầu.

### 11.5. Checkpoint, refit/test và khả năng tiếp tục

Bổ sung vào config/signature: kiến trúc compact, projection dim, profile user,
lịch validation, monitor, min_delta, patience, start epoch và max epoch.
Notebook/source mới có run signature mới; không nạp weights cũ sai shape hoặc
warm ranking checkpoints từ plan trước. Giữ artifacts và backups cũ.

Last checkpoint của trial chưa hoàn tất cần lưu model/optimizer/RNG, epoch
đã train, loss record tương ứng, best-score/epoch, significant-best, bad-check
counter và các mốc validation đã hoàn tất. Resume sau scoring chưa xong phải
tiếp tục đúng shard; không cộng patience hai lần cho cùng một epoch.
Lưu stop_reason, stopped_epoch và selected_epoch riêng. Khi early-stop phải
lưu epoch thực tế đã chạy, không dùng limit=MAX_EPOCHS làm epoch trong last.pt
như đoạn hoàn tất vòng lặp hiện tại. Chỉ thu gọn optimizer sau khi đã có quyết
định hoàn tất và checkpoint selection hợp lệ.

Validation xong đánh giá best trên all_queries/subgroups như hiện tại, khóa
architecture/config/selected_epoch. Test dựng model/mappings mới và refit từ
đầu đúng selected_epoch, không early-stop theo test. Không đổi learning rate
scheduler hoặc negatives trong lượt đầu, giúp replay refit rõ ràng.
Mỗi model/variant/ngành vẫn validation → refit/test → Dataset private; output
giữ loss curves, trial/checkpoint/stop metadata, Top-20, per-query metrics,
coverage/CI và resource logs. Resume/upload/private guards của 07/08 giữ nguyên.

### 11.6. Ngân sách mới và thứ tự triển khai

| Ngân sách khi chạy đủ ba ngành | Cũ | Compact một trial |
|---|---:|---:|
| Validation fits: NCF + D0/D1/D2 | 36 | 12 |
| Refit/test fits | 12 | 12 |
| Full warm_full validation checks tối đa | 108 | 96 |
| Epoch validation train tối đa cộng tất cả fits | 180 | 240 |

Vẫn có một lượt all-query validation của best và một lượt test cho mỗi
model/variant/ngành. Số epoch tổng ở trần mới có thể nhiều hơn; mức tiết kiệm
phải đến từ model nhỏ, ít trials và dừng khi plateau, không hứa chỉ tăng trần
epoch là nhanh hơn. Tách log load/features/train/warm scoring/final scoring/
upload để biết bottleneck. D2 feature build và input vectors chưa giảm theo dim.

| Bước | Công việc | Điều kiện nghiệm thu |
|---|---|---|
| P0 | Khóa compact config và schema stopping | Mặc định một trial, giữ protocol/data, thông số có giải thích trong notebook |
| P1 | Thêm state machine early stop và sửa last epoch | Chuỗi metric giả lập kiểm tra plateau, tiny gains, warm-up, max cap, tie và best khác last |
| P2 | Sửa checkpoint/resume/refit | Ngắt trước/sau validation cho kết quả model/selected epoch/stop counter như chạy liên tục; refit đúng selected_epoch |
| P3 | Cập nhật builder và notebook 09/10 | Có controls mới, backup cũ, không còn mô tả fixed-five-epochs hoặc ba trials mặc định; source sync đạt |
| P4 | CPU end-to-end cho NCF/D0/D1/D2 và regression | Finite gradients, loss/metrics/Top-K đúng, publish/restore mock đúng, không thay ALS/Content logic |
| P5 | Đo Kaggle All Beauty, rồi Games/Industrial | Có thời gian từng phase, memory, stop reason; không giảm population hoặc candidates để làm nhanh |
| P6 tùy chọn | Thử shared rare-user row | Mapping train-only, không drop pairs, sampler/history dùng user gốc, cohort/coverage rõ ràng |

Sau P5 mới quyết định có bật profile user bucket hay không dựa trên tradeoff
chất lượng/coverage/thời gian. Ngưỡng chất lượng chấp nhận cần được chốt trước
phép so sánh validation; không chọn dựa trên test. Chưa cam kết cấu hình nhỏ
thắng baseline hoặc tiết kiệm số giờ cụ thể trước khi có đo thực tế.

Tham khảo khái niệm min_delta/patience/warm-up/restore-best:
[EarlyStopping](https://keras.io/api/callbacks/early_stopping/). Implementation
nằm trong vòng lặp PyTorch hiện có, không thêm dependency Keras.

## 12. Tối ưu training và scoring NCF/DeepFM (08/11/2026)

Notebook 09/10 giữ TRAIN_BATCH=4096, dim8, learning rate và early stopping.
Mục tiêu là giảm thời gian chuẩn bị batch và truyền dữ liệu; chưa đo mức tăng
tốc hay GPU utilization trên T4 thực tế.

| Phần | Thay đổi |
|---|---|
| Negative sampling | Sinh negatives theo mảng, tra cứu train pairs đã sắp xếp; tránh tạo RNG Python cho từng anchor thưa |
| Chuẩn bị batch | PREFETCH_TRAIN=True chuẩn bị trước một batch CPU, pin memory khi dùng CUDA và truyền non_blocking |
| Training | ADAM_FOREACH=True; gộp kiểm tra finite loss/gradients thành một lần đồng bộ mỗi step; đọc loss về CPU khi log/cuối epoch |
| Scoring | QUERY_BATCH=32, ITEM_BLOCK=8192, PAIR_BATCH=65536; gộp Top-20 trên device, trả kết quả cuối mỗi query batch về CPU |
| DeepFM D1/D2 | SCORE_FEATURE_CACHE=True cache vectors sau projection theo checkpoint; D2 chỉ cache feature rows cần cho queries |

Scoring vẫn FP32, quét đủ candidates, tie theo score giảm dần rồi item_idx
tăng dần. Cache projection chỉ dùng trong scoring một checkpoint, không dùng
qua các lần cập nhật weights. Nếu thiếu VRAM, giảm PAIR_BATCH rồi
QUERY_BATCH/ITEM_BLOCK hoặc tắt SCORE_FEATURE_CACHE. ADAM_FOREACH=False
giảm bộ nhớ tạm optimizer. Hai T4 vẫn có VRAM riêng.

Sampler vẫn uniform không hoàn lại, loại mọi item train đã thấy của user gốc,
không dùng labels val/test. Chuỗi negatives cho user thưa đổi sang phiên bản
stateless_splitmix64_uniform_v1; cùng seed/epoch/anchor cho kết quả độc lập
batch/order. Weights mới không được kỳ vọng giống bitwise lượt cũ. Source/config
signature mới tách checkpoint cũ, không resume trộn hai sampler.

Loss log có seconds, pairs_per_second, sample_prepare_cpu_seconds,
waiting_for_batch_seconds và step_host_seconds. CPU preparation có thể chồng
thời gian GPU; không cộng các cột thành tổng. step_host_seconds gồm enqueue
và chờ finite check, không phải đo riêng kernel GPU.

Có thể thử TRAIN_BATCH=8192 rồi 16384 nếu còn VRAM; batch khác làm đổi số
optimizer steps/epoch nên phải chọn lại epoch bằng validation. Mặc định scoring
lớn hơn không đổi batch training hay số tham số. Chạy lại từ đầu session sau
khi cập nhật notebook để dùng source/config mới.

Kiểm chứng local: 19 neural tests chạy, 18 đạt và một CUDA test bỏ qua trên
PyTorch CPU. Bao phủ sampler uniform/order/batch, Top-K có ties và FP32 sát
nhau, cache projection đối chiếu forward trực tiếp, prefetch đối chiếu đồng bộ,
finite trước optimizer update, checkpoint/resume và notebook source sync.
Chưa xác nhận throughput, peak VRAM hay GPU utilization thực tế trên Kaggle.

## 13. Kế hoạch Hybrid Two-Tower (08/11/2026)

Trạng thái lúc lập kế hoạch: chưa có notebook/model Two-Tower hoặc kết quả
training. Triển khai tiếp theo được ghi tại mục 14. Căn cứ là protocol/contract Gold và source loader, temporal features,
sampler, evaluator hiện có; chưa truy cập tài khoản Kaggle để xác minh version
Dataset đang gắn trong phiên. Phải pin version và verify bundle trước chạy thật.

### 13.1. Mục tiêu và phạm vi

Huấn luyện hai tower độc lập, học từ tương tác user–item và bổ sung nội dung
MiniLM đã encode. Điểm cuối là dot product giữa hai vector 32 chiều. Mục tiêu
đo chất lượng Top-K và chi phí scoring so với Popularity, ALS, Content, NCF và
DeepFM; chưa ghép thêm reranker hay triển khai dịch vụ ANN.

Two-Tower là kiến trúc; bản có ID và nội dung dưới đây là hybrid. Không gọi
hai tower lần lượt là collaborative và content. Không fine-tune MiniLM trong
giai đoạn đầu, chỉ học ID embeddings, projections và MLP của hai tower.

### 13.2. Đầu vào và protocol giữ nguyên

Dataset: `thoandanh/gold-training-v2` và ba Dataset embedding trong mục 2.
Chạy riêng từng category, bắt đầu All_Beauty; không gộp mappings/corpus.
Kiểm tra status complete, checksums, category, Gold/text export lineage,
encoder revision, pooling policy, dimension 384 và manifest hash trước join.
Dataset slug đúng chưa đủ bảo đảm vectors tương thích. Không cần export lại
Gold hoặc import embedding về HDFS để huấn luyện trên Kaggle.

| Nguồn | Cách dùng |
|---|---|
| train_pairs, train_examples | Giữ mọi positive distinct pair, anchor ở first_pair_time; strength=1 |
| users, items, candidates | Mappings train-only riêng fold; candidates giữ cả repeat items |
| item_text_refs + text_id/vector | Item title+description 384 chiều; thiếu text dùng zero vector và missing flag |
| history_events + review_text_refs | Review profile đúng lịch sử tại từng anchor/query |
| history_ratings | Thống kê rating lịch sử; không thay rating thấp thành negative |
| queries | Chỉ input và prediction_time cho suy luận |
| labels, cohorts | Chỉ evaluator; không đi vào features, sampler hay scaler |

Validation: train trước 2021-01-01, query tại cutoff đó, nhãn trong
[2021-01-01, 2021-07-01). Test: refit từ đầu trước 2022-01-01, query tại
cutoff đó, nhãn trong [2022-01-01, 2022-07-01). Mọi mốc UTC. Không random split,
lọc k-core, bỏ user ít tương tác hoặc đổi positive policy all observed reviews.
Counts thực nghiệm dùng bảng mục 1, không dùng mẫu pilot ALS thay full Gold.

### 13.3. Ba biến thể để tách đóng góp nội dung

| Biến thể | User tower | Item tower | Vai trò |
|---|---|---|---|
| TT0-ID | User ID embedding | Item ID embedding | Baseline kiến trúc, collaborative |
| TT1-Item | User ID embedding | Item ID + projection item text + has_text | Hybrid cơ bản, đo lợi ích item content |
| TT2-History | User ID + projection review profile + rating/history statistics | Giống TT1 | Hybrid đầy đủ, tái sử dụng features D2 |

TT1 và TT2 là hai phương pháp hybrid cần báo; TT0 là ablation bắt buộc.
So sánh TT1–TT0 đo đóng góp nhánh item text, TT2–TT1 đo đóng góp gói lịch sử
review/rating. TT2 không tách riêng tác dụng rating với review; muốn khẳng định
riêng từng nguồn cần ablation bổ sung, chưa nằm trong ngân sách ban đầu.
Không chọn trước TT2 là tốt nhất khi chưa có validation.

Cấu hình khởi điểm cho cả ba: ID dim=8, text projection=16, mỗi tower một
hidden layer 64 với ReLU, output linear 32. TT0 vẫn có MLP cùng hidden/output
để giữ cấu trúc gần nhau; input dimension và tổng tham số khác do thêm features.
Review và item text dùng hai projection riêng. Không thêm cross-user/item
features hoặc MLP sau concat hai tower, vì sẽ mất khả năng cache item vectors.

Ví dụ TT2, với e là embedding ID và P là projection có thể học:

```text
review_profile(u,t) = L2-normalize(mean(review vectors của events trước t))
user_input = concat(e_user[8], P_review(profile)[16], history_stats[7])
item_input = concat(e_item[8], P_item(item_text)[16], has_text[1])
z_user = MLP_user(user_input) -> 32
z_item = MLP_item(item_input) -> 32
score(u,i,t) = dot(z_user(u,t), z_item(i))
```

Input MiniLM/profile được normalize theo contract; output hai tower ban đầu
không L2-normalize và không thêm temperature/item bias, để dùng raw dot-product
logit với BCE. Ghi rõ đây không phải cosine score. Khi thiếu text vẫn giữ item
ID và xếp hạng item đó; không tái dùng mask has_text của Content để loại candidates.
User đã biết nhưng history rỗng vẫn dùng ID và flags, không tự chuyển fallback.
Unknown user ở query dùng Popularity theo chính sách chung; không dùng một
embedding OOV chưa được huấn luyện để tuyên bố có cá nhân hóa.

### 13.4. Temporal features và chống rò rỉ

Tái sử dụng `load_examples`, `build_d2`, `prefix_features` sau khi kiểm thử
tương thích. Review mean là event-weighted, giữ multiplicity của events thật;
không dedup events chỉ vì cùng text_id. Bảy features giữ nguyên FEATURE_SCHEMA:
rating_count_valid, mean_rating, low_rating_fraction, high_rating_fraction,
review_text_count, rating_missing, review_missing. Thiếu review dùng zero
profile và review_missing=1. Scaler fit trên positive train anchors của đúng
fold; không fit trên queries hoặc tập refit khi đang validation.

Mỗi anchor chỉ nhận events có event_time < prediction_time; loại toàn bộ nhóm
cùng timestamp, gồm review/rating của positive hiện tại. Không lấy một profile
cuối kỳ gán ngược cho mọi train example. Query lấy lịch sử trước cutoff fold.
Embedding cache chứa review trước 2022 không đồng nghĩa validation được dùng
review năm 2021. Kiểm tra join record_hash với user/time và text_id đầy đủ;
has_text=True nhưng thiếu vector là lỗi phải dừng, không coi là missing hợp lệ.

Không dùng event_count toàn kỳ, target IDs/times, future eligibility/cohort
hoặc target review để làm feature. Item metadata là một snapshot tĩnh, không
chứng minh nội dung tồn tại tại thời điểm lịch sử; công bố giới hạn này cho
TT1/TT2 và Content/DeepFM D1/D2. Protocol hiện chỉ xếp hạng train items nên chưa
đánh giá khả năng khuyến nghị item hoàn toàn mới, dù model có nhánh nội dung.

### 13.5. Loss, negatives và cấu hình huấn luyện

Lượt đầu dùng PyTorch và BCEWithLogitsLoss với raw dot-product. Mỗi positive
có bốn negatives uniform không hoàn lại, loại mọi observed train items của user
gốc; lấy lại theo seed/epoch/anchor như sampler neural hiện tại. Nếu pool nhỏ
thì lấy tối đa số item khả dụng và log số negatives thực tế. Batch loss là mean
trên các positive/negative rows, không thêm class weighting ở lượt đầu.

Đây là baseline Two-Tower hợp lệ, không tuyên bố tái lập loss của paper Mixed
Negative Sampling. Chọn sampler/loss này để giữ tương thích với NCF/DeepFM và
tách ảnh hưởng kiến trúc/content trước. Unobserved item chỉ là negative giả định,
không có bằng chứng user không thích. Quy tắc loại toàn bộ train positives
tuân theo contract Gold, không phải negative pool causal riêng từng anchor;
sampler không dùng nhãn validation/test.

| Thông số | Khởi điểm |
|---|---|
| Seed | 42 |
| Positive anchors / batch | 4096; tối đa 20.480 scored pairs khi đủ bốn negatives |
| Optimizer | Adam, lr=0,001, weight_decay=0,00001 |
| Compute | FP32; một GPU train, không cài lại Torch/CUDA của Kaggle |
| Max epochs | 20 |
| Full warm_full validation | Epoch 1, 3, 6, 9, 12, 15, 18, 20 |
| Early stopping | Bắt đầu đếm tại epoch 6, patience=2 checks, min_delta=0,00001 |
| Checkpoint selection | NDCG@10, rồi Recall@10, rồi epoch sớm hơn |

Best thực tế và significant-best cho patience phải tách như mục 11.3. Với mỗi
variant/category chỉ một config ban đầu; chọn epoch bằng validation. Refit
dựng model/mappings mới và train đúng selected_epoch, không dùng test để dừng.
Khóa cấu hình, ngân sách, fallback và metric của toàn lượt trước khi xem test;
nếu mở rộng tuning thì phải quyết định bằng validation và ghi thành experiment
mới. Không tăng batch/đổi loss trong lúc resume cùng signature.

Sau benchmark đầu mới cân nhắc thí nghiệm loss riêng: in-batch sampled softmax
hoặc mixed negatives. Thí nghiệm đó cần xử lý repeated item IDs, nhiều positives
cùng user và mask observed positives, xác định đúng sampling probability/
expected count cho correction. Không thêm logQ cơ học vào loss BCE hiện tại.
Đổi sampler/loss phải đổi signature, train lại và chọn lại epoch bằng validation.

### 13.6. Scoring và đánh giá

Mỗi checkpoint được đánh giá: encode toàn candidates thành ma trận I×32 một
lần ở eval mode; encode query users thành Q×32, sau đó chấm điểm exact theo
blocks bằng z_user @ z_item.T. Cache phải gắn checkpoint/fold/manifest hash,
không dùng tiếp sau optimizer update. Không chạy lại hai MLP cho từng user–item
pair như pairwise NCF/DeepFM. Tái sử dụng `rank_exact`, `rank_checkpoints`,
`evaluate`, `compare_results`; adapter truyền candidate-valid mask toàn True
với mọi item ID hợp lệ, không lấy mask text coverage.

Khởi điểm query_batch=512, item_block=16.384, FP32, Top-20; score block khoảng
32 MiB. Với 364.229 candidates Industrial refit, item output 32 chiều khoảng
46,6 MB, còn input text 384 chiều khoảng 559,5 MB (đơn vị thập phân). Đây chỉ
là kích thước tensors, chưa gồm optimizer/activations và không phải đo VRAM.
Hai T4 chia query batches cho scoring; train dùng một GPU, không cộng VRAM hai
card. Đo All Beauty trước để điều chỉnh kích thước batches trước full benchmark.

Giữ toàn candidates, repeats và target ngoài candidates trong mẫu số; query
không có native prediction tính miss. Tie theo score giảm rồi item_idx tăng.
Không dùng sampled evaluation/ANN cho bảng chính. ANN có thể đo riêng về sau
bằng Recall so với exact Top-K và latency, không trộn với metric recommendation.

Metric và cohort theo mục 3: NDCG@10 chính, Recall@10/20, NDCG@20,
CatalogCoverage@10, native coverage/fallback rate, target coverage. Báo
all_queries và nguyên warm_full Gold; nhóm history 0/1/2/3–4/≥5, repeat/novel,
item có/thiếu text và user có/thiếu review history. Nhóm phụ chỉ chẩn đoán,
không dùng để đổi cohort chọn model. Báo count từng nhóm; HitRate trùng Recall
khi mỗi query có một target như manifests hiện tại.

Bảng chính gồm Popularity, ALS, Content, NCF, DeepFM D0/D1/D2, TT0/TT1/TT2,
có native và +Popularity tương ứng. Chỉ ghép baseline có cùng data version,
queries/candidates/protocol. Nếu chưa chạy baseline full thì ghi chưa có kết
quả; không lấy pilot hoặc tự điền score. Giữ per-query metrics, paired bootstrap
1.000 lần, seed cố định, CI 95% cho delta NDCG@10 của TT1–TT0, TT2–TT1 và
hybrid đã khóa với các baseline trên cùng cohort. CI này mô tả bất định theo
query của seed đã chạy, chưa đo bất định training giữa nhiều seeds. Không kết
luận hybrid thắng khi CI/độ lớn chênh lệch chưa hỗ trợ.

Log riêng load/verify, join/profile build, train, encode towers, exact scoring,
metrics và export. Báo peak RAM/VRAM, disk, pairs/s và queries/s với cùng phần
cứng và batch policy; chi phí encoder MiniLM là chi phí offline đã phát sinh.
So sánh scoring cả phần encode để không che chi phí bằng cách chỉ đo matmul.

### 13.7. File cần triển khai và nghiệm thu

Không triển khai model trong lượt lập kế hoạch này. Khi thực hiện, thêm ít nhất:

- `notebooks/kaggle_two_tower.py`: two encoders, dot-product loss, adapter training/
  prediction; dùng loader/evaluator/sampler và temporal helpers hiện có. Mở rộng
  validator/signature cho TT0/TT1/TT2 có kiểm soát, không nhét vào PairNetwork.
- `notebooks/11_kaggle_two_tower.ipynb`: self-contained như 09/10; controls
  category, variant, Dataset versions, train/score devices, resume và epoch.
  Builder nhúng helper sources; không cần upload Python hỗ trợ riêng.
- `tests/test_kaggle_two_tower.py`: fixture CPU kiểm tra leakage/missing text,
  gradient hai tower, exact Top-K, checkpoint/resume và notebook source sync.
  Mở rộng builder hiện có nếu phù hợp thay vì tạo một framework training mới.

| Bước | Công việc | Điều kiện hoàn tất |
|---|---|---|
| P0 | Verify inputs và khóa experiment | Bundle complete/lineage đúng, counts giữ nguyên, config và budgets đã lưu |
| P1 | TT0/TT1 model, loss và sampler | CPU fixture train finite, gradients ở cả hai tower, missing text vẫn có item score |
| P2 | TT2 temporal features | Future/cùng-time event không đổi feature anchor; zero history giữ positive; scaler train-only |
| P3 | Exact scoring và evaluator | Cached dot-product bằng forward trực tiếp trong sai số FP32, ties/multi-label/outside-candidate đúng |
| P4 | Resume và notebook | Ngắt/resume tái lập model/epoch/stop counter trong môi trường deterministic fixture; source sync đạt |
| P5 | Full All Beauty validation | Đủ pairs và warm_full, log bottleneck/RAM/VRAM; chưa chỉnh bằng test |
| P6 | Full validation ba ngành, khóa lựa chọn | TT0/TT1/TT2 cùng budgets, giữ best config/epoch và ablations |
| P7 | Refit/test và báo cáo | Refit mới đúng selected_epoch; full queries/candidates, Top-20/per-query metrics/CI/resource logs |

Ngân sách đầu: ba variants × ba ngành = 9 validation fits + 9 refit fits;
tối đa 72 full warm_full checks ở validation, thêm 9 all-query validation của
best checkpoints và 9 full test evaluations. Đây là trần, early stop có thể
giảm; không dự đoán số giờ khi chưa đo pilot full All Beauty trên Kaggle.
Profile TT2 giữ bằng memmap/batches, không nạp toàn review corpus lên GPU.
Riêng 4.071.315 anchor rows × 384 FP32 đã khoảng 6,25 GB trước dedup anchors,
chưa có query rows, vectors tra cứu và checkpoint; disk/profile I/O cần preflight
và có thể là bottleneck lớn hơn item-output matrix.

Artifact dự kiến: config/source signatures, Dataset versions/hashes, best và
last checkpoint (optimizer/RNG/stopping state), loss/validation curves,
selected_epoch, mappings, scaler/feature schema, item output vectors của model
cuối, Top-20, per-query/group metrics, paired CIs và resources. Last checkpoint
giữa epoch cần cursor/RNG tương ứng; nếu chỉ checkpoint cuối epoch thì khai báo
resume ở ranh giới epoch, không hứa tiếp tục đúng giữa minibatch. Không publish
Dataset hay chạy GPU trong lượt kế hoạch; giữ outputs ngoài Git và chỉ dùng
Dataset private khi thực hiện bước lưu/upload đã được yêu cầu.

Tài liệu nền: [Two-Tower retrieval](https://www.tensorflow.org/recommenders/examples/basic_retrieval),
[Mixed Negative Sampling, WWW 2020](https://yangli-cs-ucsb.github.io/paper/www20_yang.pdf),
[Sampling-Bias-Corrected Neural Modeling, RecSys 2019](https://research.google/pubs/sampling-bias-corrected-neural-modeling-for-large-corpus-item-recommendations/).
Đây là nguồn kiến trúc và hướng nâng cấp loss, không phải cam kết tái lập paper.

## 14. Vận hành notebook Two-Tower (cập nhật 09/11/2026)

Đã triển khai [notebook 11](../notebooks/11_kaggle_two_tower.ipynb), tự chứa source
[neural](../notebooks/kaggle_neural_recommendation.py) và loader/evaluator chung.
Để giảm trùng lặp, TwoTowerNetwork, model factory, TT variant adapters và scorer
nằm trong neural source hiện có; không tạo module training riêng như dự kiến
ở mục 13.7. PairNetwork vẫn dành cho NCF/DeepFM. Builder 09/10/11 là
[Build-NeuralNotebooks.py](../tools/gold/Build-NeuralNotebooks.py); regenerate
luôn backup notebook cũ trước khi ghi, các notebook sinh ra không có outputs.

TT0/TT1/TT2 đúng kiến trúc mục 13: ID dim8, projection linear16, hidden64/ReLU,
output32 không normalize, raw dot-product, BCE với bốn uniform negatives.
TT2 tái sử dụng nguyên temporal profile/scaler D2, không đổi cutoff hoặc Gold.
Mọi candidate có ID đều đủ điều kiện scoring dù thiếu item text.

Notebook 11 hiện có hai lượt chạy để tiết kiệm working disk mà vẫn khóa
validation trước test:

1. Gắn Gold và đủ embeddings, pin Dataset versions, chọn GPU T4 × 2. Đặt
   `RUN_MODE='validation'`; suite chạy/reuse validation cho cả ba ngành và ba
   variants, không upload. Với các validation đã hoàn tất, gắn Output cũ và đặt
   `RESUME_ROOT` để reuse thay vì train lại. Save Version/Output để giữ checkpoint best.
2. Mỗi lượt tiếp theo đặt `RUN_MODE='category_test'`, chỉ định một
   `TEST_CATEGORY`, gắn Output validation vào Input và đặt `RESUME_ROOT` đến
   thư mục `two_tower_results`. Giữ nguyên Gold/embedding versions, code và
   thông số train. Suite reuse validation, refit đúng selected_epoch, chạy test
   cho TT0/TT1/TT2 của category đó. Không điều chỉnh cấu hình sau khi xem test.
3. Test mode bật upload private riêng cho từng variant/category. Dùng owner và
   Secrets đúng; có chín Dataset handles tổng cộng. Trước mỗi upload cần chỗ
   trống tạm xấp xỉ 2× bundle + 5 GiB. `submitted_processing` chỉ xác nhận
   KaggleHub đã nhận request, chưa chứng minh Dataset đã xử lý xong.
4. Cell phân tích lưu `analysis/<category>/metrics.csv`, per-query metric files
   được giữ trong run/Dataset đầy đủ, paired CI, checkpoints chọn và manifest.
   Giữ output local cho tới khi mỗi Dataset hiển thị version đã xử lý cùng
   `result_bundle.json` và thư mục `recommendation_results/`.
5. Cell cleanup mặc định tắt. Chỉ bật hai cờ cleanup sau khi đã xác minh đủ ba
   Dataset của category; cell xác nhận receipt/path/category và chỉ xóa run
   folders của category đó. Analysis gọn, receipts và validation Output được
   giữ để tổng hợp cuối. Chuyển sang category kế tiếp rồi lặp lại.

Các validation outputs của lượt trước đã hoàn tất theo log, vì vậy có thể dùng
chúng làm Output nguồn nếu code, data hashes và training signature còn khớp.
Không cần chạy lại validation để chuyển sang từng lượt category test.

Kết quả gồm best_config, trial/epoch/loss/stop metadata, model/mappings, feature
schema/scaler, item output vectors, Top-20, per-query/group metrics, CI với
Popularity và resource reports. Cell cuối vẽ loss và tính TT1–TT0/TT2–TT1
paired CI cho native và +Popularity trên cùng query population. Chỉ so baseline
khác khi source/fold/population trùng. Các nhóm history/repeat/coverage dùng
evaluator chung; item-text/review coverage có trong feature reports, chưa tạo
metric subgroup riêng theo text coverage như định hướng mở rộng ở mục 13.

Kiểm tra CPU: test_kaggle_two_tower.py bao phủ gradient hai tower, parameter
budget, missing item text, cached scores đối chiếu forward, ties trên nhiều
item blocks, train/validation/refit thật trên Parquet fixture, unknown-user
fallback, temporal anchors, epoch resume bitwise, test gate, thứ tự validation
trước test và notebook source/config sync. Chạy thêm regression neural cho
NCF/D0/D1/D2. Chưa đo inference/training CUDA, throughput hoặc peak VRAM trên T4.
