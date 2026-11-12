# Gold G0–G2 và notebook Kaggle

G0 là [hợp đồng dữ liệu](../processing/gold_contract.json); cutoff lấy từ
[protocol đã khóa](../processing/gold_protocol.json). G1 tạo dữ liệu nền cho
Popularity/ALS/NCF và ba bản DeepFM. G2 xuất text để encode. G3 import embeddings,
G4 tính rating/review profiles và train model là các bước sau.

Implementation: [gold.py](../processing/gold.py), [runner](../tools/gold/Run-Gold.ps1),
[overlay](../compose.gold-light.yaml). Gold không sửa raw/Silver, không đổi
cutoff, không tạo negatives sẵn và không chọn cấu hình bằng test score.

## Chạy trên máy local

PowerShell host tại `project/`, HDFS safemode OFF, ba DataNode đang hoạt động,
không có notebook/kernel/job Spark khác. Runner đọc source Silver cụ thể từ
protocol, kiểm tra fsck và dùng shared lock EDA. Không dùng CLI để bỏ guard host.

```powershell
& ./tools/gold/Run-Gold.ps1 -Category All_Beauty
# Chỉ chạy tiếp sau khi run trước complete và đã xem budget/resource report:
& ./tools/gold/Run-Gold.ps1 -Category Video_Games
& ./tools/gold/Run-Gold.ps1 -Category Industrial_and_Scientific
```

Một category/process; local[2], driver 1 GiB, container 2 GiB/2 CPU. Budget
HDFS output khởi đầu 4×Silver bytes, scratch 8×; preflight cộng thêm 2× cho
export/ZIP và reserve 20 GiB. Đây là dự toán và guard polling, không hard quota.
HDFS output/scratch được giám sát mỗi 5 giây; export bytes kiểm tra sau write,
host/container filesystem reserve cũng được đo. Log guard dùng prefix SILVER do
tái sử dụng Resources, còn tên phase có `gold:`. Đo All Beauty trước, không tự
tăng RAM khi có lỗi. Truyền `-DockerDiskPath` nếu VHDX nằm ở ổ khác đã xác minh.

Run mới không append/overwrite run cũ. Staging chỉ được rename khi toàn bộ
read-back và đối soát EDA đạt. Lỗi trước publish giữ staging/manifest failed;
downstream chỉ dùng đường dẫn final với manifest complete. Nếu ZIP lỗi sau
publish, Gold vẫn bất biến và local run báo complete_export_failed; không coi
gói export đó là đã bàn giao thành công.

## Đầu ra

```text
hdfs://namenode:8020/project/processed/gold/<category>/v1/<run>/<protocol_id>/
  validation/ và test/
    history_events/   train_pairs/    train_examples/
    users/            items/          candidates/
    queries/          labels/         cohorts/       sampler.json
  item_text_refs/     review_text_refs/              manifest.json

outputs/processing/gold/<run>/
  run.json           batch.json       batch.log       resources*.json*
  host.resources.jsonl
  kaggle_texts/
    export_manifest.json   README.txt   texts/*.parquet   pilot/*.parquet
  <category>_kaggle_texts.zip
```

`test/` là dữ liệu phục vụ refit trước 2022 và đánh giá nửa đầu 2022, chưa phải
kết quả model. Queries chỉ chứa inputs; target time/IDs/source reference ở
labels, thông tin eligibility tương lai ở cohorts và chỉ dùng cho evaluation.
Mỗi train pair có một positive, mốc là first_pair_time; giữ positive history
rỗng và không đưa rating/text của event đó vào input của chính mẫu.

`train_pairs` và `train_examples` là bảng positive/anchor, không phải tất cả
cột đều là feature. `event_count` là số event của pair trong toàn phần train,
chỉ dùng audit; không được đưa nó vào input D2 tại first_pair_time. `strength`
là nhãn/strength binary, còn example_id/first_pair_time/prediction_time dùng
làm khóa và mốc lookup. Reader mô hình phải chọn rõ whitelist: D0/NCF chỉ IDs,
D1 thêm item vector, D2 thêm đúng rating-history/review profile as-of; không
tự lấy mọi cột số làm feature. Các feature D2 thực tế chưa được tạo ở G0–G2.

Mappings dùng lexical sort + distributed rank; không collect toàn bộ user IDs
về driver. Chúng riêng từng fold, không dùng mã của refit để giải mã validation.
Text refs chứa native IDs/record_hash và chỉ giữ ở HDFS; không đưa vào ZIP.
Reviews trong cache trước 2022 vẫn phải lọc theo cutoff của fold hoặc theo
prediction_time của train example khi tính profile ở G4.

Gói Kaggle chỉ có text_id, cleaned text, loại item/review và độ dài ký tự.
Item dùng title + description trong hợp candidates hai fold, theo contract v2
từ 03/11/2026. Làm sạch từng đoạn và bỏ đoạn trùng chính xác (kể cả trùng title),
giữ thứ tự; thiếu description dùng title, không thêm features. Review ghép title/body
trước 2022, giữ phủ định, số/model/đơn vị; NFC, bỏ các HTML tags đã đặc tả và
chuẩn hóa whitespace. Text giống nhau chỉ encode một lần; các sự kiện thật có
text giống nhau vẫn có refs riêng. Blank text có nullable text_id và flag.
Pilot chọn theo loại text và bins độ dài; không dùng mẫu này để suy tỷ lệ toàn
corpus. Encoder được pin revision trong contract, chưa fine-tune.

`description_assessment.json` đo toàn bộ refit candidate catalog: coverage sau
cleaning, mô tả chỉ lặp title, số item thêm nội dung, HTML, độ dài và mẫu cố định
để đọc. Các chỉ số này xác nhận có nội dung bổ sung, chưa chứng minh tăng metric.
Metadata chỉ có một snapshot; không thể khôi phục description tại cutoff lịch
sử. D1/D2 dùng cùng snapshot này và phải công bố giới hạn khi diễn giải kết quả.

Để chỉ xuất lại G2, dùng `-TextOnlyFromRun <run G0–G2 gốc>` với runner trên.
Run mới có `phase=G2_text_refresh`, `parent_gold_path`, refs text mới và manifest;
không sao chép hay ghi lại validation/test. Reader lấy các fold ở parent,
item_text_refs/review_text_refs ở run mới. Phải dùng refs mới với ZIP mới, vì
item text_id có thể đổi khi thêm description. Bản title-only cũ vẫn giữ để truy vết.

## Dùng notebook Kaggle T4 × 2

Mở [05_kaggle_text_embeddings.ipynb](../notebooks/05_kaggle_text_embeddings.ipynb).
Notebook tự chứa toàn bộ mã encoder; không cần upload file Python hỗ trợ.

1. Tạo Kaggle Dataset riêng tư từ ZIP hoặc thư mục `kaggle_texts` đã complete.
   Chọn một `CATEGORY`, bắt đầu All Beauty. Notebook chỉ giải nén ngành đã chọn;
   `PACKAGE_RUN` phân biệt nhiều bản export. Chỉ chọn gói title + description mới.
2. Thêm Dataset vào notebook, chọn GPU T4 × 2 và bật Internet để cài các phiên
   bản thư viện đã pin, tải đúng model revision. Không cài lại Torch/CUDA thủ công.
3. Chọn `RUN_MODE='pilot'` để thử, hoặc `'encode'` để chạy pilot rồi encode.
   `MAX_SHARDS_PER_RUN=16` mặc định giới hạn shard mới mỗi lượt trên cả hai GPU;
   đặt `None` để chạy hết ngành đã chọn. Run All chỉ encode một lượt. Chạy lại
   ô encode để tiếp tục; ô kiểm tra kết quả và ô lưu cần chạy lại sau đó.
4. Xem pilot_tokens.json và pilot_throughput.json. MiniLM 384 chiều, mỗi đoạn
   tối đa 256 token; tokenizer overflow không chồng lặp, không decode/re-tokenize.
   Mean pooling theo model cho từng đoạn, normalize, trung bình có trọng số số
   token, normalize lại. Không bỏ phần cuối review. Compute float16, output
   float32; mỗi GPU giữ một model eval/inference độc lập và xử lý shard riêng.
5. `partial` là chạy thành công nhưng còn shard thiếu, được lưu để resume;
   chỉ import Gold khi embedding_manifest.json complete. Output Parquet gồm
   text_id/vector và token/chunk counts. Các shard có checksum, row counts,
   hash thứ tự text_ids, model/config/code/environment signatures.
6. Lưu notebook Output và tải toàn bộ thư mục `gold_embeddings`. Hoặc bật
   `SAVE_DATASET=True`, điền `DATASET_HANDLE=username/slug` ở ô cuối. KaggleHub
   tạo Dataset mới riêng tư, cùng handle tạo version mới; dùng Dataset riêng tư
   riêng từng ngành. Snapshot chỉ chứa các shard đã xác minh cùng checkpoints,
   không gồm text input/pilot/tệp tmp. Không ghi token vào notebook.
   Khi resume, thêm Output/Dataset cũ làm input, đặt `RESUME_ROOT` đến thư mục
   chứa bundle (gold_embeddings với Output; root Dataset với ô upload).
   Shard chỉ copy/tái sử dụng khi signature và checksums khớp; tệp tmp chưa có
   checkpoint complete sẽ được encode lại. Lưu Output là thao tác riêng; ổ
   working của phiên không tự là bản lưu bền vững.

Nguồn thao tác Dataset: [KaggleHub](https://github.com/Kaggle/kagglehub#upload-dataset).
Upload chỉ chạy khi bật tùy chọn; chưa gọi tài khoản Kaggle hoặc chạy GPU tại local.

Batch size/io_rows, compute dtype, encoder revision, cleaning, source manifest,
phiên bản thư viện và mã encoder đều nằm trong signature. Đổi các giá trị ảnh
hưởng phép tính sẽ tạo bundle mới, không âm thầm ghép vectors khác cấu hình.

Không có GPU T4 tại máy local để xác nhận throughput hoặc chạy inference thật.
Các kiểm tra local bao phủ Parquet, khóa, checksum, resume, hình dạng/normalization
và notebook nhúng đúng script đã kiểm tra. Notebook chạy pilot trên GPU trước
full để kiểm chứng phần inference tại môi trường Kaggle thực tế.

Nguồn API: [model card](https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2),
[Sentence Transformers](https://sbert.net/docs/package_reference/sentence_transformer/model.html).

## G2 title + description ngày 03/11/2026

Ba run complete, verifier độc lập passed cho cả ba: lineage fold, bins với EDA,
Silver manifest, query/label separation và mọi checksum/shard text. Không OOM.
[Handoff local](../outputs/validation/gold-text-20261103/handoff.json) giữ paths và hashes.

Ba gói mới dùng contract v2 và cùng producer, giữ các fold ở parent_gold_path.
Description được đánh giá trên toàn bộ candidates refit; tỷ lệ thêm nội dung
loại trường hợp mô tả chỉ lặp đúng title. Các ZIP ngày 30/10 là bản title-only lịch sử.

| Ngành | Item candidates | Có description sạch | Thêm nội dung ngoài title | Text duy nhất trong gói | ZIP mới, MB |
|---|---:|---:|---:|---:|---:|
| All_Beauty | 102,003 | 16.69% | 15.73% | 697,678 | [135.4](../outputs/processing/gold/run_20261103T025512333964Z/all_beauty_kaggle_texts.zip) |
| Video_Games | 118,531 | 65.16% | 64.20% | 3,788,734 | [1125.5](../outputs/processing/gold/run_20261103T030034694677Z/video_games_kaggle_texts.zip) |
| Industrial_and_Scientific | 364,229 | 68.39% | 66.65% | 4,118,493 | [882.7](../outputs/processing/gold/run_20261103T031106166302Z/industrial_and_scientific_kaggle_texts.zip) |

MB dùng 10^6 bytes. Text counts là distinct trong từng ngành. Gói mới có cả item
title + description và review title/body trước 2022; refs mới nằm ở run mới,
còn train/validation/test dùng parent. Không trộn refs item của gói cũ với ZIP mới.

[Báo cáo description local](../outputs/validation/gold-text-20261103/BAO_CAO_DESCRIPTION.md) có coverage,
độ dài, nhận xét mẫu và quy tắc join. Item text dài hơn title-only về tổng ký tự
1,76× / 8,34× / 4,86×, không suy ra thời gian encode toàn corpus tăng tương ứng.
Description bổ sung công dụng/thành phần, chế độ chơi/cốt truyện, thông số/vật liệu;
vẫn có quảng cáo và dữ liệu lệch ngành trong nguồn. Chưa có kết quả recommend để
khẳng định tăng metric. Metadata snapshot chưa có lịch sử phiên bản theo cutoff.

Notebook mới chọn một ngành, chia lượt bằng MAX_SHARDS_PER_RUN, giữ checkpoint
và tùy chọn upload Dataset partial/complete. Chữ ký vector không phụ thuộc giới
hạn shard mỗi lượt, nên có thể đổi giới hạn để tiếp tục cùng bundle. Checkpoint
theo shard chỉ dùng lại khi export manifest và chữ ký khớp; ZIP mới có item text
và partition/checksum khác, nên checkpoint shard của ZIP title-only cũ không tự
được dùng lại. Vector review cũ có cùng text_id và encoder vẫn có thể được đối
soát riêng khi import; luồng ghép hai phiên bản export chưa triển khai.

## Kết quả G0–G2 ngày 30/10/2026

Cả ba runs production complete, cùng producer/contract, nguồn Silver đúng
protocol và manifest giữ nguyên trước/sau. Counts và tất cả history bins trên
population/warm ở hai fold khớp EDA độc lập; verifier từng run passed. Không có
OOM hoặc lỗi exit ở ba lượt hoàn tất. Data bytes ở bảng chỉ tính Parquet Gold,
chưa cộng JSON manifests; GB là 10^9 bytes. Text counts là distinct trong từng
ngành, chưa là global distinct trên cả ba.

| Category | Gold run | Parquet Gold, GB | Text duy nhất | ZIP text, GB |
|---|---|---:|---:|---:|
| All_Beauty | run_20261030T040703698079Z | 0,528 | 697.576 | 0,130 |
| Video_Games | run_20261030T041305392174Z | 2,989 | 3.786.568 | 1,083 |
| Industrial_and_Scientific | run_20261030T043005297574Z | 3,231 | 4.117.319 | 0,794 |

Gói Kaggle local, ignored Git:

- [All Beauty](../outputs/processing/gold/run_20261030T040703698079Z/all_beauty_kaggle_texts.zip)
- [Video Games](../outputs/processing/gold/run_20261030T041305392174Z/video_games_kaggle_texts.zip)
- [Industrial and Scientific](../outputs/processing/gold/run_20261030T043005297574Z/industrial_and_scientific_kaggle_texts.zip)

Mỗi run có `run.json`, `verification.json`, export manifest, archive SHA256 và
resource logs. [Handoff local](../outputs/validation/gold-20261030/handoff.json)
giữ final paths và measurements. Source/code/contract hash nhất quán; ZIP và
mọi shard đã đối soát checksum. Clone Git không kèm các data artifacts này.

Peak scratch quan sát: 0,715 / 6,880 / 4,881 GiB. Peak cgroup sát 2 GiB,
không suy ra encoder/G4 cũng vừa cấu hình này. Lượt đầu
`run_20261030T040533529840Z` dừng trước khi đọc Silver vì runner đã tạo thư mục
report nhưng Python yêu cầu chưa tồn tại; đã sửa và thêm regression fixture.
Không có final Gold từ lượt lỗi này, không xóa dữ liệu cũ để chạy lại.

## Tải và đọc embedding về máy local

[Notebook 06](../notebooks/06_download_read_embeddings.ipynb) chạy bằng Python kernel
VS Code/Jupyter local, không cần GPU/Spark. Mặc định Dataset
`thoandanh/gold-emb-all-beauty-v2`; chọn `CATEGORY`, `DATASET_HANDLE` và có thể pin
`DATASET_VERSION`. `DOWNLOAD_ROOT` cấu hình cache KaggleHub dưới outputs/processing,
giữ dữ liệu tải ngoài Git. Dataset private cần tài khoản có quyền; bật
`AUTHENTICATE` để gọi login tương tác, không ghi token vào notebook.

SDK trả đường dẫn local sau tải. Điền đường dẫn đó vào `LOCAL_DATASET_DIR` để
mở lại offline. Notebook tìm đúng một bundle của category, kiểm tra manifest,
sidecar, checksums, schema/counts, đọc vector theo batch để kiểm tra finite/L2
norm và key/token/chunk digests. Chỉ các shard manifest liệt kê được đọc; không
đọc pilot hoặc gom toàn bộ vector vào pandas. Nếu còn manifest export G2 local,
đối soát thêm nguồn/hash/counts; thiếu thì report ghi gold_export_matched=false.

`ALLOW_PARTIAL=False` mặc định yêu cầu complete; có thể bật để xem checkpoint,
nhưng không dùng partial cho Gold/model. Report nằm dưới outputs/processing/
embeddings/validation. Đây chưa phải G3 import, chưa kiểm tra global duplicate
text_ids giữa các shard hoặc join refs lên HDFS. Preview và ví dụ batch giữ
text_id làm khóa. Kiểm tra offline bằng `python tests/test_download_read_embeddings.py`.

API download/auth theo [KaggleHub](https://github.com/Kaggle/kagglehub#download-dataset).

## Xuất Gold training thành một Kaggle Dataset

Đã viết [exporter Python](../processing/gold_kaggle.py) và
[runner PowerShell](../tools/gold/Publish-GoldDataset.ps1). Theo lựa chọn của
người dùng, chỉ có **một Dataset riêng tư** `thoandanh/gold-training-v2`, ba thư
mục All_Beauty / Video_Games / Industrial_and_Scientific. Ba Dataset embedding
vẫn độc lập. Chưa thực hiện production export/upload trong lượt triển khai code này.

Kế hoạch thực thi:

1. Pin ba run text ngày 03/11 và fold parent ngày 30/10 từ handoff đã verify;
   so lại protocol/Silver/HDFS manifests, kiểm tra fsck nguồn trước mỗi ngành.
2. Xuất tuần tự từng ngành bằng Spark 2 CPU, container 2 GiB, driver 1 GiB.
   Không chia lại dữ liệu hoặc ghi vào Gold/Silver trên HDFS.
3. Whitelist cột, kiểm tra khóa/counts/schema/read-back và cửa sổ thời gian;
   tạo file manifest/checksum cho từng shard. Mỗi category publish local khi
   đã đủ; lượt sau chỉ bỏ qua category hoàn tất nếu verify vẫn đạt.
4. Chỉ finalize Dataset khi đủ ba ngành; snapshot upload chỉ chứa các file
   manifest liệt kê, không kèm CRC/log/tệp tmp/raw text/vectors.
5. Upload bằng KaggleHub. Dataset mới tạo riêng tư; Dataset đã tồn tại phải
   qua kiểm tra metadata is_private trước upload. Lỗi quyền hoặc không xác
   minh được privacy sẽ dừng. Cùng handle tạo version mới theo KaggleHub.

Cấu trúc gói:

```text
outputs/processing/gold_training/gold_training_export_v1_<fingerprint>/
  export_plan.json    dataset_manifest.json    training_contract.json    README.md
  All_Beauty/   Video_Games/   Industrial_and_Scientific/
    validation/ và test/
      history_events/ users/ items/ candidates/ train_pairs/ train_examples/
      queries/ labels/ cohorts/
    item_text_refs/ review_text_refs/ history_ratings/
    category_manifest.json    text_export_manifest.json
```

`validation/train_pairs` là train trước 2021; `test/train_pairs` là refit trước
2022. Query/label windows giữ nguyên protocol. `event_count` bị loại khỏi
train_pairs/train_examples vì là audit toàn kỳ. Labels/cohorts chỉ phục vụ
đánh giá; mappings riêng category/fold. Không gộp Int indices giữa các ngành.

G0–G2 không chứa rating trong các bảng train, nên exporter thêm lookup hẹp
`history_ratings(record_hash,rating)` từ Silver trước 2022, khớp các record_hash
của review_text_refs mới. Rating thiếu vẫn giữ null, không đổi positive policy.
Đây chưa phải profile D2: lọc history_events theo event_time < prediction_time
trước khi join rating/review vector và tổng hợp. Không dùng cả cache trước 2022
để làm validation history trước 2021 hoặc lịch sử train anchor.

`text_export_manifest.json` là đúng manifest gói title+description đã tạo
vectors. So SHA256 tệp này với signature.export_manifest_sha256 trong bundle
embedding trước khi join text_id. Gói training chứa native user/item IDs để
join, vì thế chỉ dùng Dataset riêng tư. Ràng buộc không upload IDs ở G2 áp
dụng gói text; gói training riêng này được xuất theo yêu cầu mới của người dùng.

PowerShell host tại project, cài thư viện host trước (Spark chạy trong image sẵn có):

```powershell
python -m pip install "kagglehub>=1.0.3,<2" "pyarrow>=20,<21" python-dotenv

# Mặc định chỉ in kế hoạch, không chạy Docker/export/upload:
& ./tools/gold/Publish-GoldDataset.ps1

# Xuất thật ba ngành và finalize local; cần HDFS ready và không có Spark job/notebook chạy:
& ./tools/gold/Publish-GoldDataset.ps1 -Export

# Upload gói đã finalize; mở login tương tác nếu chưa có xác thực:
& ./tools/gold/Publish-GoldDataset.ps1 -Upload -Login

# Hoặc xuất và upload tuần tự trong một lệnh:
& ./tools/gold/Publish-GoldDataset.ps1 -Export -Upload -Login
```

Nếu Jupyter đang chạy nhưng không có kernel/job, dừng container notebook đó
trước export, rồi start lại sau. Runner không tự dừng notebook của người dùng.
`-Python` chọn Python/venv host khác; `-DatasetHandle` đổi owner/slug. Không ghi
token trong code, tham số CLI hoặc Git. Không thay image Torch/Spark để cài KaggleHub.

Python CLI tương ứng: `python -m processing.gold_kaggle plan`, `export`,
`finalize`, `verify`, `upload`; xem `--help` cho từng lệnh. `upload` mặc định
dry run; thêm `--execute` mới gửi dữ liệu. Host verification không cần Spark.

Nếu đã export xong rồi gặp lỗi đăng nhập/upload, dùng `-Upload -Login
-PackagePath <thư mục gói đã hoàn tất>` để dùng lại đúng dữ liệu cũ, kể cả
sau khi mã exporter được sửa (fingerprint mặc định có thể đổi). Không dùng
`-Export` khi retry gói cũ. KaggleHub 1.0.3 đặt token login trong bộ nhớ tiến
trình; credentials set chưa phải xác nhận token hợp lệ. HTTP 401/403 dừng
upload và yêu cầu kiểm tra token/quyền, không coi là Dataset chưa tồn tại.

Uploader đọc `.env` ở project root bằng python-dotenv, không nội suy biến,
không in hoặc ghi lại giá trị. Ưu tiên `KAGGLE_API_TOKEN`; nếu `KAGGLE_KEY`
bắt đầu bằng `KGAT_`, cũng nhận diện là token mới và dùng token auth, không
Basic username/key. Các giá trị key khác dùng cặp legacy KAGGLE_USERNAME /
KAGGLE_KEY. Có cấu hình `.env` hợp lệ thì kể cả `-Login` cũng không hỏi lại.
Nếu thiếu cả token và cặp legacy mới dùng cấu hình có sẵn hoặc login khi được
 yêu cầu; cặp legacy thiếu một biến sẽ dừng. Không copy `.env` vào payload.

Nếu đã xác thực đúng tài khoản và đọc được Dataset embedding nhưng metadata
Dataset Gold vẫn 403, kiểm tra đích đã tồn tại và thuộc tài khoản có quyền.
Có thể tạo trước Dataset riêng tư đúng slug `gold-training-v2` trên Kaggle
với README.md của gói local, chờ xử lý xong rồi upload gói đầy đủ như version
kế tiếp qua `-Upload -PackagePath ...`. Không suy ra Dataset mới từ riêng mã
403 và không bỏ qua kiểm tra private.

Tệp upload receipt ở cạnh package, ngoài payload. Sau API thành công, trạng
thái là submitted_processing, chưa khẳng định Kaggle đã xử lý xong. Nếu đã có
receipt, script chặn gửi lặp cho cùng package; kiểm tra trên Kaggle trước khi
chủ động dùng `-NewVersion`/`--new-version`. Lỗi mạng để upload_uncertain vì
không thể bảo đảm phía server chưa nhận. Không xóa version cũ.

Trước upload dự phòng thêm khoảng hai lần payload + 5 GiB cho snapshot copy
và ZIP tạm SDK. Export kiểm tra reserve trước mỗi bảng và host theo chu kỳ;
đây không phải hard filesystem quota. Lỗi export giữ work folder để chẩn đoán,
không dùng category thiếu manifest complete. Không có GPU/model training ở đây.

Kiểm tra đạt: fixture Spark trên dữ liệu nhỏ (fold/time/rating lookup/export),
checksum/whitelist upload giả lập, private guard, ba export tuần tự và runner
mocks trên PS7/5.1. Chưa kiểm chứng upload thật hoặc metadata API bằng tài khoản
Kaggle; host hiện chưa cài KaggleHub. API theo
[KaggleHub upload](https://github.com/Kaggle/kagglehub#upload-dataset) và
[Kaggle Dataset metadata](https://www.kaggle.com/docs/mcp).

## Kiểm tra

PowerShell host tại `project/`; HDFS fixture chỉ ở namespace testing:

```powershell
& ./tests/test_gold_runtime.ps1
docker compose -f compose.yaml -f compose.datanodes.yaml -f compose.gold-light.yaml run --rm --pull never --no-deps -e GOLD_TEST_HDFS=1 spark-notebook python -W ignore::ResourceWarning /workspace/tests/test_gold.py
docker compose -f compose.yaml -f compose.datanodes.yaml -f compose.gold-light.yaml run --rm --pull never --no-deps spark-notebook python /workspace/tests/test_kaggle_text_embeddings.py
# Sau khi run production complete, đối chiếu bins với EDA (mỗi lần một run):
docker compose -f compose.yaml -f compose.datanodes.yaml -f compose.gold-light.yaml run --rm --pull never --no-deps spark-notebook python /workspace/tests/integration/verify_gold_outputs.py --run-id <run_id>
```

Fixture kiểm tra ranh giới cutoff/end, ties, repeats, cold users/items, train
anchor có/không history, lexical mappings, blank/dedup text, refs trước 2022,
Parquet export không chứa user IDs, atomic publish và failure trước publish.
Runner mocks kiểm tra lock, job đang chạy, fsck, reserve, OOM/exit, manifest và
log/cleanup failures trên PowerShell. Chưa train hoặc đánh giá model mới.

Verifier production chỉ đọc Gold/Silver, so toàn bộ bins events/items/timestamps
ở population và warm với EDA cutoff độc lập, kiểm tra cửa sổ nhãn, separation
queries/labels và mọi checksum/shard trong gói text. Kết quả ghi local
`outputs/processing/gold/<run>/verification.json`; không sửa snapshot HDFS.
