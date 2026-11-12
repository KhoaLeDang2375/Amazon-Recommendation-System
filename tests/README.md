# Kiểm thử nhỏ

NCF/NeuMF và DeepFM D0/D1/D2 — PowerShell host tại project/:

Bao gồm sampler vector hóa (uniform, không trùng/seen, độc lập batch/order),
Top-K FP32/ties và merge, projection cache theo checkpoint, prefetch so với
đồng bộ, chặn non-finite trước optimizer update. CUDA parity tự bỏ qua khi
máy không có CUDA; CPU tests không chứng minh throughput trên T4.

```powershell
python -X utf8 tools/gold/Build-NeuralNotebooks.py
python -X utf8 -m unittest discover -s tests -p test_kaggle_neural_recommendation.py -v
```

Dùng PyTorch CPU và Parquet thật, không cần Docker hoặc mạng. Bao phủ temporal
features/missing/ties, sampler, FM/MLP và scoring parity, train→val→refit/test,
checkpoint resume qua phiên, upload Dataset giả lập và notebook/source sync.
Compact defaults có kiểm tra số tham số; early stopping bao phủ warm-up,
patience theo validation checks, tiny gains, best khác last, actual stopped
epoch, max cap và resume trước/sau commit validation hoặc completion. Refit
chỉ dùng selected_epoch và không đọc validation/test để tự dừng.
Không phải full-data hoặc CUDA benchmark.

`fixtures/ingestion/smoke.jsonl` gồm ba record; `fixtures/legacy/test.txt` lưu ví dụ cũ. Smoke HDFS nằm riêng dưới `/project/testing/raw/smoke/v1`.

## Scope và ingestion offline — PowerShell host

Không cần Docker hoặc Spark, không tải mạng hay đọc payload dataset:

```powershell
python tests/test_experiment_scope.py
python tests/test_download_amazon_hf.py
& ./tests/test_scope_scripts.ps1
```

Test Python kiểm tra scope/selector/notebook và tải giả lập/checksum/hardlink. Test PowerShell thay Docker và manifest tải bằng fixture trong bộ nhớ: xác nhận chỉ sáu dataset được preview, không ghi HDFS, giữ metadata Flume đã finalized, wrapper chọn một category và từ chối category ngoài scope trước gọi Docker. Test hardlink cần filesystem NTFS cho phép tạo link; môi trường hạn chế quyền có thể chặn thao tác này.

## Smoke HDFS chỉ đọc

HDFS phải sẵn sàng và smoke đã có trong vùng testing. Chỉ chạy một job Spark; không bật notebook đồng thời. Dùng container batch nhẹ:

```powershell
docker compose -f compose.yaml -f compose.datanodes.yaml -f compose.eda-light.yaml run --rm --pull never --no-deps spark-notebook python /workspace/tests/integration/verify-smoke.py
```

Script kiểm tra notebook 02 format/cú pháp và đúng ba record smoke. Từ chối path ngoài hdfs://namenode:8020/project/testing/ trước tạo JVM; collect tối đa bốn dòng để phát hiện dư input. Không quét/collect raw thật. Trên clone mới **chưa có smoke**, nhập fixture một lần vào vùng testing, không publish vào routing dataset thật:

```powershell
docker compose cp .\tests\fixtures\ingestion\smoke.jsonl namenode:/tmp/smoke.jsonl
docker compose exec namenode hdfs dfs -mkdir -p /project/testing/raw/smoke/v1
docker compose exec namenode hdfs dfs -put /tmp/smoke.jsonl /project/testing/raw/smoke/v1/
```

Chỉ nhập fixture một lần; script mong đúng ba record.

## EDA Recommendation

```powershell
docker compose -f compose.yaml -f compose.datanodes.yaml -f compose.eda-light.yaml run --rm --pull never --no-deps spark-notebook python /workspace/tests/test_eda_runtime.py
docker compose -f compose.yaml -f compose.datanodes.yaml -f compose.eda-light.yaml run --rm --pull never --no-deps spark-notebook python -W ignore /workspace/tests/test_eda_recommendation.py
```

Fixture JSONL đối nghịch được tạo tạm trong output local rồi xóa sau kiểm thử; không put vào HDFS. Kiểm tra notebook mới, missing/native type, timestamp trộn đơn vị, lỗi JSON, metadata copies/complement/conflicts, distinct-pair density, coverage, left join không nhân dòng, cold-start và 5-core rỗng. Xem [hướng dẫn EDA](../docs/eda.md).

Runtime test dùng mock, không khởi tạo JVM: guard trước scan, chỉ một category, Spark.stop khi thành công/lỗi/SIGTERM, cache được đăng ký trước action, giải phóng cache khi load lỗi, status failed/interrupted và guard ở ranh giới pha. Spark fixture thật còn xác nhận không còn persistent RDD, không tạo scratch/global overlap và dừng context. PowerShell fixture kiểm tra thêm cleanup đúng container khi batch exit khác 0 và host thiếu dung lượng trước gọi Docker.

## Kiểm thử bổ sung pha 4

Runtime có 8 test, thêm từ chối raw/path traversal và smoke dư record với collection giới hạn. PowerShell mô phỏng cả -Apply nhưng Docker/manifest/source/hash là fixture: missing/failed manifest, size/checksum sai, target size/extra file, Flume byte mismatch/.tmp, safemode, nguồn ngoài staging và checksum nguồn sai đều bị chặn trước put/mkdir. Wrapper được kiểm tra lỗi lưu log vẫn rm/nhả lock và guard Spark đang chạy.

Bằng chứng pha 1–3 vẫn dùng khi code tương ứng không đổi; không cần chạy lại full EDA fixture hoặc tải dataset chỉ vì cập nhật docs. Kiểm chứng chỉ đọc HDFS count/fsck theo sáu namespace có log tại outputs/validation/phase4-20261014/ (không commit). Fsck healthy/size khớp chưa là SHA256 payload mới; lỗi legacy tách riêng trong history.

Wrapper log regression dùng native process thật ghi stdout/stderr nhưng Docker/job vẫn là mock: stderr với exit 0 phải được lưu và báo thành công; logs exit khác 0 hoặc exception vẫn báo failed, rm container và nhả lock. Đã chạy trên Windows PowerShell 5.1 và PowerShell 7; không khởi tạo Spark/đọc raw trong test này.

## Silver

PowerShell host tại project/, không chạy Spark fixture cùng batch raw:

```powershell
& ./tests/test_silver_runtime.ps1
docker compose -f compose.yaml -f compose.datanodes.yaml -f compose.silver-light.yaml run --rm --pull never --no-deps -e SILVER_TEST_HDFS=1 spark-notebook python -W ignore::ResourceWarning /workspace/tests/test_silver.py
```

Host test tạo TestRepo riêng dưới outputs/validation, mock toàn bộ Docker; kiểm tra native stderr, shared EDA lock, active notebook, fsck, host reserve, exit/OOM, incomplete manifest và log/rm cleanup failures. Chạy được trên Windows PowerShell 5.1 và PowerShell 7; không đọc raw.

Python test có pure parser/path checks và HDFS fixture trong /project/testing/{raw,processed}/silver/fixture_<id>. Kiểm tra exact copies khác dict order, low/invalid rating vẫn giữ interaction, text/phủ định, ms 12/13 digits, round-trip timestamp, case-sensitive nested JSON, quarantine/audit, unmatched review và metadata conflict. Failure-before-publish/budget vượt không tạo final run; duplicate local run ID bị từ chối; SparkContext dừng. Fixture/report được giữ để truy vết, không tự xóa dữ liệu.

Read-back/count gates trên raw là bước nghiệm thu riêng qua [Run-Silver.ps1](../tools/silver/Run-Silver.ps1), không phải test chạy lại mặc định. Xem [Silver](../docs/silver.md) cho contract, namespace và báo cáo từng run.

## EDA bổ sung trên Silver

PowerShell host tại project/; Docker/Spark image hiện có, không có Spark batch/notebook khác đang chạy:

```powershell
& ./tests/test_model_readiness_runtime.ps1
docker compose -f compose.yaml -f compose.datanodes.yaml -f compose.eda-light.yaml run --rm --pull never --no-deps spark-notebook python -W ignore::ResourceWarning /workspace/tests/test_model_readiness.py
```

Host mocks không truy cập Docker thật; kiểm tra shared lock, active job, selected fsck, reserve, OOM/exit, manifest và cleanup/native stderr. Spark fixture là DataFrames nhỏ, không đọc raw/Silver production; kiểm tra first-next, ties, repeat, partial/outside candidates, verified-only, target rating leakage, text flags/excerpts, empty population và negative feasibility khi đã thấy hết catalog. Xem [EDA mục 8](../docs/eda.md#8-kế-hoạch-eda-bổ-sung-trên-silver-trước-gold). Không chạy encoder/model hay tạo Gold.

## Kiểm tra Gold G0–G2 và encoder notebook

Hai notebook training 07/08 dùng source chung và kiểm tra bằng
`python tests/test_kaggle_recommendation.py` (numpy, scipy, pandas, pyarrow,
psutil, nbformat; Torch tùy chọn để kiểm tra nhánh tie-break bằng tensor CPU).
Chạy thêm fit ALS CPU thật bằng RECOMMENDATION_ALS_TEST=1 trong môi trường Linux
Python 3.11 có implicit==0.7.2, numpy==1.26.4, scipy==1.14.1, pandas==2.2.3 và
libgomp1. Fixture chỉ dùng Parquet nhỏ local, không chạy Spark/HDFS hoặc Kaggle.
Kết quả/checksum notebook sync và load Gold All Beauty thật được ghi local
outputs/validation/recommendation-notebooks. Hướng dẫn [training Kaggle](../docs/kaggle-recommendation-plan.md#8-cách-dùng-notebook-đã-triển-khai).

Gold training export/upload: `python tests/test_gold_kaggle.py` kiểm tra Parquet,
manifest/checksums, whitelist snapshot, private guard và upload giả lập.
`tests/test_gold_publish_runtime.ps1` mock runner (PS7/5.1), ba export tuần tự,
finalize trước upload, chặn active job/fsck lỗi/OOM và cleanup.
Fixture Spark đầy đủ (chỉ một category nhỏ, không production/Kaggle):

```powershell
docker compose -f compose.yaml -f compose.datanodes.yaml -f compose.gold-light.yaml run --rm --pull never --no-deps -e GOLD_KAGGLE_SPARK_TEST=1 spark-notebook python -W ignore::ResourceWarning /workspace/tests/test_gold_kaggle.py
```

Notebook tải/đọc local 06 có fixture Parquet/checkpoints và chạy luồng offline:
`python tests/test_download_read_embeddings.py` (cần numpy, pyarrow, pandas).
Không đăng nhập, tải Dataset private hay gọi GPU trong fixture.

`test_gold.py` bao phủ split/anchors, labels/cohorts, text cleaning/dedup/export
và HDFS publish/fail-before-publish (bật `GOLD_TEST_HDFS=1`).
`test_gold_runtime.ps1` mock các fault của runner trên host.
`test_kaggle_text_embeddings.py` kiểm tra Parquet/vector keys, shape/norm,
checksum/resume, giới hạn shard mới mỗi lượt, snapshot Dataset partial chỉ gồm
shard hợp lệ và notebook nhúng đúng script đã kiểm tra, không cần GPU.
`integration/verify_gold_outputs.py --run-id ...` đối chiếu production với EDA
và gói text, gồm lineage parent khi chỉ refresh G2. Lệnh cụ thể ở [Gold](../docs/gold.md#kiểm-tra); không chạy hai Spark
jobs đồng thời. Kiểm tra inference T4 thực tế bằng pilot trong notebook Kaggle.

## Kiểm tra EDA cutoff

Kiểm tra EDA cutoff (không chạy đồng thời với batch/Spark notebook):

```powershell
docker compose -f compose.yaml -f compose.datanodes.yaml -f compose.eda-light.yaml run --rm --pull never --no-deps spark-notebook python -W ignore::ResourceWarning /workspace/tests/test_eda_cutoff.py
```

Fixture kiểm tra ranh giới nửa mở, ties nhiều targets, cold-item theo target pairs,
history events khác distinct items, target lag, empty population và quy tắc chọn
latest-feasible/shortest-horizon. `test_model_readiness_runtime.ps1` kiểm tra thêm
route `-CutoffStudy -TestStart ... -Months ...` và giữ các fault checks cũ.

## Kiểm tra baseline ALS

Two-Tower và hồi quy neural chạy bằng Python host, không cần Spark/GPU:

```powershell
python -m unittest discover -s tests -p test_kaggle_two_tower.py
python -m unittest discover -s tests -p test_kaggle_neural_recommendation.py
```

Two-Tower kiểm tra hai encoder/cached dot-product, missing text, temporal features,
training/refit, resume, validation-before-test và notebook 11 tự chứa. Fixtures ở
outputs/validation, không đọc production data. GPU benchmark chạy riêng trên Kaggle.

Với notebook container đang idle, chạy trên host PowerShell:

```powershell
docker compose exec -T spark-notebook python -W ignore::ResourceWarning /workspace/tests/test_als_baseline.py
```

Dữ liệu giả: temporal cutoff/ties/repeat, binary pairs, cold user/target ngoài
candidates, Recall/NDCG đối chiếu tay, sample ổn định và fit/save/load ALS thật.
Không đọc production Silver. Xem [hướng dẫn pilot](../docs/als-baseline.md).
