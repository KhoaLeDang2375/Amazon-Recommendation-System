# Lịch sử triển khai và bằng chứng kiểm chứng

Các mục có ngày là **kết quả lịch sử**, không xác nhận mọi service/dataset đang ở cùng trạng thái hôm nay. Hướng dẫn vận hành hiện hành nằm trong [operations.md](operations.md), phạm vi trong [data-management.md](data-management.md).

## 08/11/2026 — tối ưu train/scoring notebook neural

Notebook 09/10 dùng sampler vector hóa, prefetch một CPU batch/pinned transfer
trên CUDA, Adam foreach và giảm đồng bộ kiểm tra loss/gradients. Scoring tăng
query/item/pair blocks lên 32/8192/65536, gộp exact Top-20 trên device và cache
projection D1/D2 theo checkpoint. Giữ train batch4096, dim8, early stopping,
population/candidates và FP32; xem mục 12 của kế hoạch Kaggle để điều chỉnh.
Sampler thưa đổi chuỗi random có version và signature mới; không trộn resume
với checkpoints trước tối ưu. Builder backup rồi tái tạo hai notebook tự chứa.

Neural suite: 19 tests, 18 đạt và một CUDA parity bỏ qua vì host PyTorch CPU.
Shared helper regression: 12 tests, 11 đạt và một ALS opt-in bỏ qua. Bằng chứng
tại outputs/validation/kaggle-neural-performance-tests.log và
kaggle-recommendation-performance-regression.log. Chưa train/scoring full trên
Kaggle, chưa đo speedup hoặc GPU utilization, không upload Dataset thật.

## 11/10/2026 — volume và smoke ban đầu

Demo cũ persist NameNode nhưng DataNode dùng mặc định /tmp/hadoop-hadoop/dfs/data, khác volume mount /tmp/hadoop-root/dfs/data. Cấu hình mới đặt rõ dfs.datanode.data.dir khớp mount, tái sử dụng project identity và named volumes; không format để migrate.

HDFS test cũ /data/test.txt có 19 bytes và một block thiếu; container cũ không còn để phục hồi filesystem đó. Entry sau chuyển sang /project/testing/legacy/test.txt; namespace move không khôi phục block. Fixture local được giữ tại [tests/fixtures/legacy/test.txt](../tests/fixtures/legacy/test.txt).

Kiểm chứng tiny smoke cùng ngày:

- Compose hợp lệ; build Flume/Spark đạt, pip check đạt.
- Publish UTF-8 JSONL ba object với tên batch/SHA256; Flume đóng output.
- Spark đọc đúng IDs 1/2/3 và values 10/20/30; notebook format/cú pháp đạt, Jupyter login endpoint HTTP 200.
- Recreate Flume với channel rỗng không nhập lại sample; restart DataNode vẫn đọc được block sample.
- fsck smoke HEALTHY, không missing/corrupt blocks. Path lúc kiểm chứng là /project/raw/smoke/v1; sau đó chuyển về /project/testing/raw/smoke/v1 để tách dữ liệu thử.

Fixture này không chứng minh throughput lớn, peak RAM, exactly-once hoặc Colab connectivity. Demo layout cũ đã chuyển vào project root; mẫu test local không được tự động mount/ingest bởi Compose cũ.

## 11/10/2026 — migration và Video Games metadata

Sau channel drain/file đóng, di chuyển /project/{raw,processed,analytics}/dataset sang video_games_metadata; smoke sang /project/testing/{raw,processed,analytics}/smoke. Giữ project/volumes và bốn file metadata HDFS, không publish lại.

| Bằng chứng Video Games metadata | Giá trị |
| --- | --- |
| Source | meta_Video_Games.jsonl |
| Bytes | 437.375.785 |
| Records nguồn | 137.269 |
| File HDFS / block | 4 file / 7 block |
| fsck dataset thật | HEALTHY, không missing/corrupt blocks |
| SHA256 nguồn và nội dung HDFS nối đã đối chiếu | 3ad94dfaa8dfed23c5f420aa9ddec415d00f2b8ec9f2d796f487b4b2c4d256de |

Manifest gốc ở [video_games_metadata.v1.json](../ingestion/manifests/video_games_metadata.v1.json), ngày/URL tải ban đầu còn null. Không suy ra SHA256 của file HDFS ghép tùy thứ tự: kết quả trên là phép đối chiếu đã thực hiện khi migrate, không lệnh cat tùy tiện để chạy lại.

Smoke staging/spool cũ chuyển archive local; fixture/code test tách vào tests. Notebook đã backup tại data/archive/notebooks/01_eda.before-layout.ipynb trong migration ngày đó. Các thay đổi notebook do người dùng làm sau migration không bị reset ở đợt dọn hiện tại.

Ngày 11/10 từng ghi NameNode/DataNode/Flume/Spark notebook đang chạy, init exit 0, Graphify refresh/query đạt và các vùng secret/data được ignore. Đây không phải danh sách service đang chạy ngày 14/10. Legacy thiếu block vẫn riêng, không được sửa bởi kiểm chứng dataset thật.

## 13/10/2026 — EDA cũ và áp lực tài nguyên

Lượt EDA sáu category bị lỗi/ngắt giữa chừng; run sau ghi completed_categories gồm All Beauty và Video Games. Không coi hai run failed là EDA đủ ba category mới; Industrial and Scientific chưa có xác nhận full report hiện hành.

Đã dừng job sở hữu và dọn runtime/scratch được xác định theo run, giữ raw và report aggregate. Số read/write nhiều GB là I/O tích lũy; áp lực ổ C còn liên quan VHDX không tự thu nhỏ khi xóa file Linux.

File kết quả compact local ghi VHDX 131.088.777.216 → 41.879.076.864 bytes, thu hồi 83,08 GiB; free C 116,15 → 200,91 GiB và Docker/WSL dừng. File không có timestamp nên không gán ngày thực hiện chính xác; đây không phải số đo dung lượng hiện tại.

[Ảnh kiến trúc đề xuất ngày 13/10](architecture-proposed-20261013.png) được giữ làm bản lịch sử, không đại diện dịch vụ đã triển khai. Sơ đồ hiện hành nằm trong [architecture.md](architecture.md).

## 14/10/2026 — phạm vi ba ngành hàng và vòng đời EDA

Đã xác nhận bytes local theo manifest của All Beauty/Video Games/Industrial and Scientific: tổng 7.136.947.646 bytes JSONL không nén. Scope chung, downloader một worker, selector một category và preview đúng namespace đã được kiểm thử offline.

Pha 2: CLI/notebook dùng chung run_eda, driver 1 GiB; batch container 2 GiB/CPU 1. Cache được quản lý khi load lỗi/5-core; SIGTERM đi qua finally. Cell xem report không tạo Spark, cross-category scratch và helper run ID cố định được bỏ sau backup.

- Scope: 3 test đạt; downloader: 6 test offline đạt ở pha 1.
- Runtime: 7 test mock đạt, không JVM.
- Spark: fixture adversarial 1 integration test đạt trong 75,257 giây, no persistent RDD sau job và context dừng.
- PowerShell mock và Docker thật xác nhận exit/log/host samples, ngắt và cleanup đúng container sở hữu.
- Compose config, cú pháp, preservation notebook và graph refresh đạt; sau test chỉ các Hadoop container ban đầu còn chạy.
- Không chạy EDA raw, xóa dataset hoặc compact trong pha 1–2. Fixture chưa chứng minh dataset lớn vừa tài nguyên.

Nguồn bằng chứng và tiến độ chi tiết nằm trong [kế hoạch dọn repo](KE_HOACH_DON_DEP_REPO.md). Backup đợt dọn ở data/archive; không commit payload backup/log.

## 14/10/2026 — kiểm thử bàn giao pha 4

Count/fsck trên sáu namespace selected: HEALTHY, 9 file, tổng 7.136.947.646 bytes khớp manifest. Cluster có 3 Live DataNode; missing block duy nhất được định vị tại /project/testing/legacy/test.txt (19 bytes), giữ nguyên. Không gọi toàn cluster HEALTHY và không dùng fsck/size như SHA256 payload mới.

Đã tăng guard Flume skip (bytes/finalized suffix), source manifest, cleanup log-error/lock và giới hạn smoke chỉ trong testing với limit(4). PowerShell fault cases, 8 runtime mock test và 3 scope test đạt. Spark smoke đọc đúng 3 record/111 bytes, không raw; wrapper Docker thật chạy job mock và dọn container đạt. Full EDA fixture pha 2 không lặp vì code phân tích không đổi.

Log/summary kiểm chứng tại outputs/validation/phase4-20261014/ được ignore Git. Chưa xóa data, sửa legacy block, train hoặc compact. Những con số này là snapshot pha 4, phải kiểm chứng lại trước cleanup có thay đổi dữ liệu.

## 14/10/2026 — cleanup data pha 5

Inventory và fsck trước/sau xác nhận sáu selected datasets HEALTHY, 9 file và 7.136.947.646 bytes không đổi; vẫn giữ sáu JSONL local phục hồi. Xóa sáu file staging và sáu namespace HDFS Sports/Toys/Health (23.688.120.187 bytes mỗi bộ), sau đó xác minh target vắng mặt và dọn parent rỗng. Trash interval 0, xóa vĩnh viễn theo từng target đã kiểm kê; không xóa block/volumes hoặc đổi số DataNode.

Dọn bản spool Video metadata COMPLETED sau checksum khớp nguồn, log channel rỗng và bốn file HDFS finalized; giữ tracking/channel/checkpoint. Dọn report cũ trùng và Sports dang dở, giữ bộ All Beauty/Video Games cùng trạng thái run failed và phần khác biệt của report cũ trong diagnostics. Cache metadata/log ~1,7 MB giữ nguyên. Manifest active còn sáu entry đúng path; bản 12 entry gốc giữ trong biên bản, revision/checksum không đổi.

Free ổ C đo được 182,84 → 205,30 GiB (+22,46 GiB). Linux df Used giảm ~22,23 GiB; VHDX giữ ~39,11 GiB, chưa compact. Không cộng hai số thành dung lượng Windows đã thu hồi. Selected fsck/size không chứng minh SHA256 payload mới; không chạy raw EDA. Legacy missing block 19 bytes vẫn giữ nguyên, cluster vẫn có 1 missing block.

Bằng chứng ở outputs/validation/phase5-20261014/ (ignored), số đo là snapshot có thể chịu hoạt động nền. Bàn giao pha 6 được ghi riêng bên dưới.

## 14/10/2026 — bàn giao pha 6, đóng đợt cleanup

Hoàn thành pha 0–6: phạm vi ba ngành hàng thống nhất, EDA một category/process với guard tài nguyên, docs được gộp, fixture và kiểm chứng selected HDFS có bằng chứng, dữ liệu ngoài scope đã dọn. README/project-map là entrypoint hiện hành; kế hoạch cleanup giữ như hồ sơ lịch sử, không còn là backlog đang mở.

Rà file tracked/untracked có thể chia sẻ: không có dataset/output/cache hoặc credential theo các mẫu kiểm tra; index chưa staged. Đã sao lưu notebook 01 tại data/archive/before-phase6-20261014/, bỏ output/execution_count của hai cell và xác nhận source/cell metadata không đổi. Notebook 02 sạch output. Local links và cú pháp PowerShell đạt; graph được refresh sau cập nhật docs. Không commit/push hoặc chạy lại raw EDA. Biên bản outputs/validation/phase6-20261014/ được ignore Git.

Việc tiếp theo thuộc phát triển pipeline: EDA thật tuần tự để đo tài nguyên và xác nhận diagnostic gates trước khi chốt processing; ETL/model/evaluation/API/web chưa triển khai. Reports hiện còn là lịch sử, fixture không chứng minh raw vừa RAM hay kiến trúc mở rộng nhiều máy. Selected raw healthy theo snapshot pha 5; legacy testing vẫn có một missing block. Compact VHDX vẫn là bước bảo trì thủ công.

## 14/10/2026 — All Beauty hoàn tất, sửa thu log Windows PowerShell

Người dùng chạy batch All_Beauty: outputs/eda/run_20261014T055611280385Z/run.json có status complete, completed_categories chứa All_Beauty; summary ghi 701.528 reviews, 112.590 metadata, 631.986 users, 112.565 items và 693.929 distinct pairs. Container batch đã bị xóa, chỉ bốn Hadoop container còn chạy khi kiểm tra. Wrapper báo lỗi ở bước lưu log, không phải lỗi job/cache cleanup. Log batch gốc không được lưu thành công và container đã xóa nên không thể lấy lại bằng docker logs; báo cáo/run/resources vẫn còn.

Đã tái hiện trước sửa bằng native process ghi stdout/stderr trên Windows PowerShell 5.1: ErrorActionPreference Stop coi native stderr là lỗi chặn pipeline. Run-Eda chỉ đặt Continue trong phạm vi thu docker logs, chuyển bản ghi thành text; ghi file vẫn ErrorAction Stop và exit code Docker vẫn được kiểm tra. Lỗi thật vẫn báo failed và không bỏ qua rm/nhả lock. Regression native stderr exit 0, logs exit 7 và lỗi thu log đạt trên Windows PowerShell 5.1/PowerShell 7; không chạy lại EDA raw để kiểm thử sửa này.

## 14/10/2026 — tăng CPU cho các batch tiếp theo

Khi Video Games đang ở reviews_profile, batch dùng gần 100% CPU nhưng bị quota một CPU, Spark local[1]; RAM chưa đầy. Đã tăng quota overlay lên hai CPU và mặc định Run-Eda -Cores 2 để đồng bộ local[2]. Giữ container RAM 2 GiB, driver 1 GiB, shuffle 64 và DISK_ONLY; không sửa logic phân tích. Direct CLI/notebook vẫn mặc định một luồng, có thể chọn hai luồng. Regression wrapper kiểm tra mặc định hai luồng và override -Cores 1. Không dừng/restart Video Games đang chạy; chưa đo tăng tốc raw, không cam kết nhanh gấp đôi.

## 14/10/2026 — ba full EDA hoàn tất và kế hoạch processing

Đối chiếu ba run mới: All Beauty `run_20261014T055611280385Z`, Video Games `run_20261014T060728726293Z`, Industrial `run_20261014T064207289175Z`, đều status complete và completed_categories đúng một category. Cùng scope/code SHA256, raw v1, UTC/cutoff 2022-01-01; Spark lần lượt local[1]/local[1]/local[2]. Hoàn tất 13:02:38 / 13:41:05 / 13:56:32 giờ Việt Nam. Không suy tăng tốc từ thời gian của các category khác nhau.

Tổng dòng theo nguồn 10.509.148 reviews và 677.423 metadata, chín file / 7.136.947.646 bytes khớp quy mô documented. Input=parsed, parse errors=0, keys/rating/time và safe metadata coverage đạt; join không nhân dòng. Đối soát summary/overview, degree bins, rating bins và train/test/cold-start aggregates đạt. Không đọc lại raw payload hoặc hash lại toàn dataset; global distinct/overlap vẫn not_measured.

Phân tích trong [processing.md](processing.md): exact copies excess 113.909, reviews sau exact dedup dự kiến 10.395.239; chưa phải processed count. One-item user rất cao, nhất là All Beauty 93,22%; không dùng ≥5/5-core mặc định. Metadata price/content/taxonomy có vai trò riêng; giữ source_category khác main_category. Diagnostic warm-user/warm-item chỉ 4,20% / 17,91% / 20,68% test events ở cutoff 2022. Kế hoạch gồm clean/catalog/quarantine/audit, split trước pair latest, train-only history/mapping/candidates, cohort/coverage và read-back Parquet.

Biên bản local `outputs/validation/processing-plan-20261014/evidence.json` lưu aggregate checks và SHA256 bảng nguồn, ignored Git. Chỉ phân tích/lập kế hoạch, chưa chạy processing/Spark EDA lại, chưa tạo processed/model hoặc sửa raw/volumes. Các mục lịch sử “chưa đủ ba EDA” phía trên được giữ đúng thời điểm.

## 16/10/2026 — Silver triển khai và hoàn tất ba category

Đã thêm processing/silver.py, compose.silver-light.yaml, tools/silver/Run-Silver.ps1, parser/HDFS fixtures và host fault tests. Chạy tuần tự All Beauty, Video Games, Industrial với image có sẵn, local[2], driver 1 GiB, container 2 GiB/2 CPU; không đổi raw/volumes/Flume. Notebook idle đã kiểm tra không có JVM/kernel trước khi tạm dừng để dành tài nguyên.

Read-back xác nhận 10.395.239 reviews + 677.423 catalog rows theo category, loại 113.909 exact copies, quarantine=0; coverage metadata 100%, join/count/key/hash/time/schema đạt. Ba final Silver paths fsck HEALTHY. Parquet dataset tổng 4.446.160.227 bytes (~4,14 GiB). Inventory chín raw files giữ nguyên bytes/mtime; chưa SHA256 payload lại. Có một missing block testing legacy trước đợt này; không tuyên bố toàn HDFS khỏe.

Run IDs/path và lưu ý producer/current code ở [Silver mục 10](silver.md). Các full runs dùng producer hash 0f08a715; code hiện hành cd43f250 bổ sung optional integer string precision, finite reserve, schema gate và hashed conflict diagnostics. Đã lưu producer source, đối chiếu native counters/schema output và chạy fixture HDFS cuối đạt 2 tests, host mocks đạt PowerShell 7/5.1. Video Games có khoảng trống timestamp ~10.981 giây chưa xác minh nguyên nhân, không dùng wall time đó làm throughput benchmark.

Bằng chứng local: outputs/validation/silver-20261016/handoff.json và outputs/processing/silver/<run_id>/, ignored Git. Gold/split/features/model/Hive chưa triển khai. Không stage/commit/push.

## 18/10/2026 — EDA bổ sung trên Silver hoàn tất

Thêm module/notebook 03, runner đọc Silver và bộ tổng hợp reports; giữ nguyên EDA raw và notebook 01/02. Fixture Spark cuối đạt 3 tests; host fault mocks đạt PowerShell 7/5.1. Guard schema chấp nhận reorder cột nhưng từ chối sai fields/types; cache hẹp được materialize tuần tự sau chẩn đoán một attempt chờ AQE. Hai attempt không complete được giữ log/report, không đưa vào kết quả tổng hợp.

Ba complete readiness runs: All Beauty run_20261018T083227913697Z, Video Games run_20261018T083634881633Z, Industrial run_20261018T084441932113Z. Đọc đúng Silver ngày 16/10, đối soát input contracts; cùng code/protocol, local[2], driver 1 GiB, container 2 GiB/2 CPU, không OOM. Source manifest không đổi trước/sau, không ghi raw/Silver và không cài encoder/model.

Đã đo text item, rà 576 excerpts theo category, first-next/history/candidates/content eligibility, verification A/B, train support và negative feasibility. Kết quả và giới hạn ở [EDA mục 8.7](eda.md#87-kết-quả-full-silver-ngày-18102026). Test A warm+full queries lần lượt 2.877/70.794/144.444; không phải toàn bộ query population hoặc model metric. Không train/encode/tạo Gold. Host snapshots và checks local ở outputs/validation/model-readiness-20261018/, báo cáo tổng hợp ở outputs/eda_model_readiness/comparison_20261018_final/; ignored Git.

## 30/10/2026 — khóa cutoff bằng thống kê toàn Silver

Bổ sung `eda_cutoff.py` và route `-CutoffStudy` trong runner readiness hiện có.
So sánh 2019/2020/2021 × 3/6/12 tháng trên ba ngành, mỗi process một ngành.
Quy tắc khai báo trước: cutoff gần nhất đủ ≥2.401 warm-full queries cho mỗi
ngành, rồi horizon ngắn nhất. Chọn validation nửa đầu 2021; refit trước 2022,
test nửa đầu 2022. Đã đo riêng test sau selection; không chọn bằng model score.
Chi tiết counts, giả định cỡ mẫu, hai biểu đồ và giới hạn ở
[EDA mục 8.8](eda.md#88-so-sánh-cutoff-và-cửa-sổ-trước-gold).

Sáu runs complete, code/hash và source manifest nhất quán; 2021 full-year
query/warm/pair counts khớp EDA 18/10. Fixture Spark đạt 2 tests; runner success
và fault mocks đạt PowerShell 7/5.1. All Beauty test có 1.751 warm queries và
377 history≥2; không tuyên bố ngưỡng validation bảo đảm mọi nhóm test đủ mẫu.
Không train model, tạo embeddings/Gold hoặc sửa Silver. Protocol khóa lưu ở
`processing/gold_protocol.json`; báo cáo aggregate local ở
`outputs/eda_model_readiness/cutoff_20261030_final/`, verification ở
`outputs/validation/cutoff-20261030/verification.json`.

Lượt `run_20261030T025009518376Z` được dừng có chủ đích để sửa phép tính target
lag; exit 130, OOM=false, không được đưa vào kết quả. Phép tính đã có fixture
kiểm tra trước khi chạy lại. Đầu phiên chỉ một DataNode hoạt động, NameNode
safemode ON; đã khởi động đúng hai container DataNode cũ để HDFS tự rời safemode.
Jupyter không có kernel/job được tạm dừng khi chạy batch và khởi động lại sau đó.
Không format, đổi volume hoặc republish dữ liệu.

## 30/10/2026 — cập nhật kế hoạch Gold theo ba phiên bản DeepFM

Thay kế hoạch D0 đến D5 bằng đúng ba bản: D0 chỉ ID, D1 thêm item text embedding,
D2 thêm rating-history và review profile. NCF giữ thuần ID. Kế hoạch Gold ở
[processing mục 6.3](processing.md#63-kế-hoạch-triển-khai-tầng-gold) chia Gold nền,
export/Kaggle/import embedding và feature theo thời gian thành các pha nghiệm
thu riêng. Chốt grain đề xuất một positive/distinct train pair, feature as-of
first_pair_time; cùng positive/sampler cho NCF và ba bản DeepFM, giữ mẫu thiếu
history/text/rating. Đây là cập nhật tài liệu kế hoạch, chưa chạy hoặc tạo Gold,
embeddings hay model mới.

## 30/10/2026 — Gold G0–G2 và notebook Kaggle T4 × 2

Triển khai producer/runner, hợp đồng Gold, read-back/publish và gói text-only.
Ba category complete từ Silver đã pin, cùng code/contract: All Beauty
`run_20261030T040703698079Z`, Video Games `run_20261030T041305392174Z`, Industrial
`run_20261030T043005297574Z`. Tất cả query/warm/pairs và bins history hai fold
khớp EDA cutoff; source manifests bất biến, selected inputs HEALTHY. Gold giữ
cold-start/outside labels, ID mappings riêng fold và positive anchors; chưa tạo
negative rows hoặc rating/review profiles D2.

Parquet Gold tổng 6.748.134.272 bytes; ZIP text tổng 2.007.134.679 bytes. Chi tiết
run paths, text counts, checksums và measured budgets ở [Gold](gold.md).
Kaggle packages không có user IDs/labels; refs giữ ở HDFS. Notebook 05 tự chứa
encoder code, MiniLM revision đã pin, hai GPU workers, chunk-mean review dài,
pilot, bounded IO, signature/checksum resume và disk preflight.

Fixture Spark/HDFS kiểm tra read-back, fail-before-publish và report directory
do host tạo; mocks runner đạt PowerShell 7/5.1. CPU tests encoder bao phủ key
alignment, checksum, resume, vectors và notebook sync/schema. Không có GPU
local để test inference thật; chưa upload hoặc chạy Kaggle, chưa tạo embedding
bundle hoặc train model mới. Lượt report collision đầu tiên giữ batch failed;
không ghi Silver hay tạo final Gold ở lượt đó. Handoff local:
`outputs/validation/gold-20261030/handoff.json`.

## 03/11/2026 — description và notebook Kaggle theo từng lượt

Đánh giá toàn bộ candidates refit ba ngành, dùng title + description cho G2
contract v2, bỏ đoạn trùng chính xác, thiếu description dùng title. Ba run mới
complete: run_20261103T025512333964Z (All Beauty), run_20261103T030034694677Z
(Video Games), run_20261103T031106166302Z (Industrial). ZIP tương ứng
135.447.953 / 1.125.511.518 / 882.728.272 bytes; refs mới, folds giữ ở
parent_gold_path. Verifier độc lập passed cả ba; Silver manifests giữ nguyên.

Notebook 05 chọn một ngành trước giải nén, giới hạn shard mới mỗi lượt, resume
checkpoint và lưu Dataset tùy chọn qua KaggleHub. Fixture Spark, encoder IO,
notebook schema/script sync và runner mocks PS7/5.1 đạt. Chưa chạy T4 inference
hoặc upload tài khoản Kaggle. Metadata chưa có phiên bản description tại cutoff.
Bằng chứng/ZIP paths ở [Gold](gold.md#g2-title--description-ngày-03112026) và
[handoff local](../outputs/validation/gold-text-20261103/handoff.json).

## 03/11/2026 — code xuất Gold training lên một Kaggle Dataset

Theo yêu cầu người dùng, chuẩn bị một Dataset `thoandanh/gold-training-v2`,
ba ngành riêng thư mục. Exporter giữ fold parent đã khóa và refs text mới,
thêm rating lookup trước 2022 từ Silver, chưa tổng hợp D2 profiles. Không
copy raw text/vectors; mappings và labels giữ riêng đúng category/fold.

Đã kiểm tra fixture Spark (3 cases), host checksum/whitelist/private guard,
runner mocks PS7/5.1 và plan trên manifest thật. Chưa export production hoặc
upload qua tài khoản Kaggle; chưa cài KaggleHub vào host. Jupyter idle được
khôi phục sau fixture. Kết quả local: outputs/validation/gold-training-code/
verification.json. Hướng dẫn và lệnh ở [Gold](gold.md#xuất-gold-training-thành-một-kaggle-dataset).

## 04/11/2026 — notebook ALS full và content-based trên Kaggle

Thêm notebook 07/08 tự chứa runtime, đọc Gold đã export và embedding item
MiniLM đã có. Giữ nguyên temporal split, đánh giá all-query/warm-full, native
và Popularity fallback; ALS chọn cấu hình bằng validation rồi refit cho test.
Content dùng trung bình vector các item khác nhau trong history. Exact ranking
chia block/query shard, hỗ trợ hai GPU khi scoring và checkpoint qua phiên.

Bộ 5 kiểm tra đạt trong Linux Python 3.11, gồm fit ALS CPU thật, refit test,
metric nhiều target, content, resume và đồng bộ source notebook. Loader đọc
được toàn validation All Beauty thật. Chưa chạy full train hoặc kiểm tra CUDA
trên T4; notebook có GPU preflight. Bằng chứng local nằm trong
outputs/validation/recommendation-notebooks; cách chạy ở
[hướng dẫn](kaggle-recommendation-plan.md#8-cách-dùng-notebook-đã-triển-khai).

Sửa tiếp môi trường Kaggle Python 3.13: bỏ guard chặn 3.13, dùng implicit 0.7.3
và scientific stack có wheel tương thích. Mặc định CPU fit ở nhánh này, scoring
vẫn hai GPU; GPU fit 0.7.3 cần CUDA 13/RMM và bật rõ trong ô cài đặt. Thêm gate
không cho train sau khi cài dependency thất bại. Bộ 6 kiểm tra đạt trên host
Python 3.13.12 với implicit 0.7.3 CPU thật; log py313_implicit073.log trong cùng
thư mục bằng chứng. Chưa xác minh CUDA 13 trên Kaggle.

## 07/11/2026 — notebook NCF/NeuMF và DeepFM D0/D1/D2

Thêm notebook 09/10 tự chứa source, đọc cùng Gold protocol với 07/08. NeuMF
dùng GMF/MLP embeddings riêng; DeepFM dùng ID FM/Deep chung embedding, D1/D2
bổ sung item text và rating/review lịch sử vào nhánh Deep. Sampler chung theo
example_id/epoch; D2 strict-before cả timestamp group, scaler chỉ fit train.
Validation chọn config/epoch rồi refit/test từ đầu; không đổi candidates/split.

Checkpoint last có optimizer/RNG, best chỉ chứa trọng số; scoring exact chia
query shard và pair microbatch. Trial/refit complete thu gọn last về weights/
epoch, còn checkpoint của train chưa xong giữ optimizer/RNG. Kết quả giữ private, upload sau từng variant/
ngành qua helper của 07/08; bổ sung whitelist .pt cho snapshot và restore.
Notebook 07/08 chỉ đồng bộ hai dòng whitelist trong source nhúng, giữ các
cell/cấu hình/outputs khác, có backup trước khi sửa. Trial thua giữ metrics
và bỏ model payload; D2 thu gọn cache train sau phase hoàn tất.

Kiểm tra host Python 3.13/PyTorch 2.13 CPU dùng Parquet thật và optimizer thật,
gồm bốn variants, refit/test, timestamp/missing/multiplicity, scoring parity,
resume và upload giả lập. Logs tại outputs/validation/kaggle-neural-tests.log
và kaggle-recommendation-regression.log. Chưa train full hoặc xác minh CUDA
trên T4, chưa upload thật. Graph refresh đã thử nhưng Docker Desktop Linux
daemon đang tắt; graph cũ chưa phản ánh các symbol mới.

## 08/11/2026 — compact và early stopping cho notebook 09/10

Áp dụng compact_full_users: một trial C8, ID dim 8, MLP [32,16], projection
item/review 16; giữ mọi user/item/train pairs, cùng sampler và protocol Gold.
Tăng max_epochs lên 20, full warm_full validation ở [1,3,6,9,12,15,18,20].
Early stopping bắt đầu cộng bad checks từ epoch 6, patience hai lần validation,
min_delta=0,00001 NDCG@10. Best checkpoint thực tế được lưu độc lập min_delta;
refit từ đầu đúng selected_epoch, không dùng test để dừng hoặc chọn lại.

Last checkpoint lưu cả loss/validation history, significant-best và patience
counter cùng model/optimizer/RNG. Checkpoint là bản commit, JSON trial là
mirror; phục hồi từ checkpoint khi ngắt giữa hai lần ghi. Stop metadata tách
stopped_epoch/selected_epoch, lưu epoch thật thay max_epochs, phân biệt
ranking_plateau/max_epochs/refit_selected_epoch. Model hoàn tất mới thu gọn
optimizer. Thêm validation.csv và biểu đồ ranking bên cạnh loss curve.

Builder backup notebook cũ trước khi nhúng source mới. Source/config mới có
signature mới, không dùng lẫn checkpoints bản dim16/32 cũ. Giảm hàng user
embedding theo ngưỡng lịch sử vẫn chưa triển khai; xem kế hoạch mục 11.
Kiểm tra CPU và log tại outputs/validation/kaggle-neural-compact-tests.log;
hồi quy helper 07/08 tại kaggle-recommendation-compact-regression.log. Chưa
train full hoặc xác minh CUDA/T4, chưa upload Dataset thật trong đợt này.

Kiểm chứng cuối: 14 kiểm tra neural đạt, gồm compact parameter counts và bốn
điểm ngắt/resume; 11 kiểm tra hồi quy helper đạt, một test ALS full opt-in bỏ
qua. Graphify đã refresh thành công qua container tools (không khởi động HDFS
hoặc đổi volumes), graph gồm 858 nodes. Notebook/source sync và các controls
thực thi trong cell cấu hình đều khớp mặc định compact.
