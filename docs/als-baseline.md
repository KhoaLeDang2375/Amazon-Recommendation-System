# Thử baseline implicit ALS trên Silver

Kế hoạch thực nghiệm full Gold mới: [ALS và content-based trên Kaggle](kaggle-recommendation-plan.md).
Các số liệu bên dưới là pilot trên mẫu và cửa sổ validation cũ.

Notebook `notebooks/04_als_baseline.ipynb` dùng cùng implementation với
`notebooks/als_baseline.py`. Không sửa Silver hoặc notebook EDA cũ. Đây là snapshot
thí nghiệm nhỏ, chưa triển khai pipeline Gold tổng quát.

Mặc định Video Games, Silver run `run_20261016T022209697481Z`, 5.000 user có
SHA256(seed:user_id) nhỏ nhất, seed 42. Giữ mọi event của user được chọn; không
lọc history/k-core/rating/verified. Mẫu có thể giảm support item và không đại diện
mặc định cho toàn category. Hash sampling vẫn phải scan cột user toàn Silver.

Train trước 2021; validation là timestamp đầu tiên/user trong [2021,2022), ties
giữ toàn bộ distinct parents. Test mặc định tắt; bật sau khi khóa cấu hình sẽ
refit trước 2022 rồi đánh giá first-next trong [2022,2023-10-01). Không random split,
không dùng target rating/text làm feature. Training pair binary distinct, mappings
chỉ fit train. Candidates là toàn bộ items train của mẫu, giữ repeat. Không sinh
negative rows hay mở rộng toàn catalog metadata.

Recall/NDCG@10/20 báo hai nhóm: tất cả query có nhãn (target ngoài candidates vẫn
ở mẫu số) và warm user với toàn bộ targets thuộc candidates. Có ba hàng model:
Popularity train-only theo distinct users/item; ALS thuần (cold user nhận danh sách
rỗng, tính miss); ALS có Popularity fallback cho cold user. Không dùng drop để che
query không dự đoán được. Query không có target không nằm trong population metric.

## Chạy notebook

PowerShell host tại `project/`, Docker Desktop/HDFS đã sẵn sàng:

```powershell
docker compose -f compose.yaml -f compose.datanodes.yaml start namenode datanode1 datanode2 datanode3
docker compose exec -T namenode hdfs dfsadmin -safemode get
docker compose exec -T namenode hdfs fsck /project/processed/silver/video_games/v1/run_20261016T022209697481Z
docker compose -f compose.yaml -f compose.datanodes.yaml --profile eda up -d --no-deps --pull never spark-notebook
```

Giữ đúng named volumes; chỉ chạy khi safemode OFF, ba DataNode live và selected
Silver HEALTHY. Module khóa `.als.lock` để tránh hai kernel chạy ALS cùng lúc;
chỉ một kernel chạy Spark, không đồng thời EDA/Silver/batch. Dùng
Jupyter hiện có với authentication; không ghi token vào Git. Mở notebook 04, sửa
CONFIG nếu cần và Run All. Notebook sở hữu và dừng Spark khi hoàn tất. Session
dùng helper hiện có: local[2], driver 1 GiB, shuffle 16; container dùng Compose
hiện có. Không cần cài thêm package. Kiểm tra host free trên ổ chứa Docker VHDX
và outputs >=20 GiB; guard trong Python chỉ đo filesystem container theo pha,
không thay phép đo host hoặc giám sát liên tục.

Kiểm tra logic với dữ liệu giả, rồi chạy pilot (PowerShell host; notebook/kernel
khác phải idle):

```powershell
docker compose exec -T spark-notebook python -W ignore::ResourceWarning /workspace/tests/test_als_baseline.py
docker compose exec -T spark-notebook python /workspace/notebooks/als_baseline.py --sample-size 5000
```

Khi không có notebook container, dùng `docker compose ... run --rm --pull never
--no-deps spark-notebook python /workspace/notebooks/als_baseline.py` sau preflight
và bảo đảm không có job khác. Không chạy batch với notebook đang có Spark.

## Output và cách đọc

`outputs/als_baseline/run_<UTC>/` được ignore Git. `run.json` ghi nguồn/manifest
hash, code hash, cấu hình, counts, thời gian, folds và complete/failed;
`resources.json` là số đo theo pha. `sample_events/` giữ ba cột user/item/time.
Mỗi fold có model Spark ML, mappings users/items, train_pairs, targets, queries,
recommendations và metrics.json. Spark ghi file:// dưới mount outputs, không ghi
nhầm vào HDFS Silver. Không overwrite runs cũ; lỗi giữ manifest failed.

Notebook nguồn không giữ output. Có thể lưu bản executed riêng vào thư mục ignored
để xem bằng chứng. Chạy pilot 5.000 trước, rồi thử 50.000 nếu số query warm đủ và
tài nguyên phù hợp. Chỉ tuning trên validation; không đổi cutoff/sample bằng test
score. ALS không vượt Popularity vẫn là một kết quả thực nghiệm hợp lệ.

Kết quả chạy ngày 22/10/2026 (giờ Việt Nam):

| Mẫu | Run ID | Events | Train pairs | Queries validation | Warm, toàn bộ targets trong candidates |
| --- | --- | ---: | ---: | ---: | ---: |
| 5.000 user | run_20261022T154338576319Z | 8.341 | 6.438 | 703 | 50 |
| 50.000 user | run_20261022T154634893236Z | 83.155 | 63.936 | 6.850 | 812 |

Cả hai notebooks chạy toàn bộ 6 code cells, không có error output, run complete.
Mẫu 50.000 được tăng vì lượt smoke chỉ có 50 warm queries, không chọn bằng test
score. Cùng seed/rank=16/maxIter=5/regParam=0.1/alpha=1.0; chưa tuning, test tắt.

| Model, mẫu 50.000 | Nhóm | Recall@10 | NDCG@10 | Recall@20 | NDCG@20 |
| --- | --- | ---: | ---: | ---: | ---: |
| Popularity | 812 warm queries | 0,019704 | 0,010610 | 0,032020 | 0,013621 |
| ALS | 812 warm queries | 0,008621 | 0,003261 | 0,017241 | 0,005429 |
| Popularity | 6.850 all queries | 0,022190 | 0,010731 | 0,029781 | 0,012583 |
| ALS thuần | 6.850 all queries | 0,001022 | 0,000387 | 0,002044 | 0,000644 |
| ALS + Popularity fallback | 6.850 all queries | 0,020876 | 0,009859 | 0,028029 | 0,011612 |

Mẫu 50.000 có 38.326 training users, 19.330 training items, 5.459 cold-user queries;
4.117/6.850 targets thuộc candidates. ALS thuần có gợi ý cho 1.391 queries;
fallback cho 5.459 cold users giúp hệ thống có gợi ý cho toàn bộ 6.850 queries.
ALS chưa vượt Popularity ở cấu hình này; chưa đủ cơ sở kết luận cho full category.
Thời gian pha fit=5,604 giây, recommendation=6,795 giây, evaluation=15,659 giây;
đây không phải benchmark toàn pipeline hoặc cluster phân tán. Filesystem reserve
đạt tại các pha; không dùng tổng capacity ảo ba DataNode làm dung lượng host.

Bằng chứng local (ignored Git):
[notebook 5k đã chạy](../outputs/als_baseline/04_als_baseline.executed.ipynb),
[notebook 50k đã chạy](../outputs/als_baseline/04_als_baseline.50k.executed.ipynb),
[manifest 50k](../outputs/als_baseline/run_20261022T154634893236Z/run.json),
[metrics 50k](../outputs/als_baseline/run_20261022T154634893236Z/validation/metrics.json).
Code hash của run 50k khớp source triển khai. Kiểm tra dữ liệu giả đã đạt cả
temporal ties/repeat, missing recommendations, metric tính tay và fit/save/load
model. Container notebook cũ đã được tạo lại bằng Compose hiện hành để bổ sung
mount manifests bị thiếu; kernel cũ không hoạt động khi thao tác. Không sửa Silver.
