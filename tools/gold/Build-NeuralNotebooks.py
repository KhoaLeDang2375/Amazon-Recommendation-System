"""Build standalone Kaggle NCF and DeepFM notebooks from the tested Python sources."""
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'notebooks'))
from kaggle_neural_recommendation import default_config


def build(algorithm, filename):
    cells = []
    def markdown(text):
        cells.append(dict(cell_type='markdown', metadata={}, source=text.splitlines(keepends=True)))
    def code(text, hidden=False):
        cells.append(dict(cell_type='code', metadata={'jupyter': {'source_hidden': True}} if hidden else {},
                          execution_count=None, outputs=[], source=text.splitlines(keepends=True)))
    name = 'NCF / NeuMF' if algorithm == 'ncf' else 'DeepFM D0 / D1 / D2'
    markdown(f'''# {name} trên Gold — Kaggle

Chạy Run All trên kernel mới. Chọn GPU T4 × 2 và bật Internet để cài dependencies/lưu Dataset.
Thêm Dataset private `thoandanh/gold-training-v2` vào Input. Mặc định chạy ba ngành tuần tự,
validation → khóa config và epoch → refit/test → upload Dataset private sau mỗi model/ngành.

NCF dùng NeuMF thuần ID, GMF/MLP có embeddings riêng. DeepFM dùng Linear + FM + Deep chung ID
embeddings. D1 thêm item text vào nhánh Deep, D2 thêm rating/review lịch sử; đây là phần mở rộng
của đồ án. Encoder MiniLM frozen, không encode hoặc fine-tune lại. Mỗi variant train từ đầu.
Profile mặc định compact_full_users: giữ đủ user/item/positives, ID embedding 8 chiều,
MLP [32,16], projection 16 chiều, một trial. Trần 20 epoch và có early stopping theo ranking validation.
''')
    if algorithm == 'deepfm':
        markdown('''## Input embedding

| Category | Dataset private |
|---|---|
| All_Beauty | thoandanh/gold-emb-all-beauty-v2 |
| Video_Games | thoandanh/gold-emb-video-games-v2 |
| Industrial_and_Scientific | thoandanh/gold-emb-industrial-and-scientific-v2 |

D0 chỉ cần Gold. D1/D2 cần bundle complete đúng export hash/encoder/revision; D2 cần cả review
embeddings, `review_text_refs` và `history_ratings`. D2 dùng events **trước** từng anchor train,
loại cả group cùng timestamp; giữ positive đầu tiên và missing flags. Metadata sản phẩm là
snapshot tĩnh, chưa chứng minh đúng phiên bản lịch sử. Không nạp mọi review vectors vào RAM.
''')
    code('''import os, sys, json, subprocess
from pathlib import Path
ENVIRONMENT_READY = False
os.environ['OPENBLAS_NUM_THREADS'] = '1'
os.environ['MKL_NUM_THREADS'] = '1'
os.environ['OMP_NUM_THREADS'] = '1'
os.environ['CUBLAS_WORKSPACE_CONFIG'] = ':4096:8'
if sys.version_info < (3, 10):
    raise RuntimeError('Cần Python >= 3.10')
modern = sys.version_info >= (3, 13)
packages = (['numpy>=2.1,<3', 'scipy>=1.14.1,<2', 'pandas>=2.2.3,<4', 'pyarrow>=20,<24']
            if modern else ['numpy==1.26.4', 'scipy==1.14.1', 'pandas==2.2.3', 'pyarrow==20.0.0'])
packages += ['psutil', 'matplotlib', 'kagglehub>=1.0.3,<2']
# Giữ Torch/CUDA sẵn của Kaggle; không cài implicit cho các notebook neural.
subprocess.run([sys.executable, '-m', 'pip', 'install', '-q', '--only-binary=:all:', *packages], check=True)
subprocess.run([sys.executable, '-c', 'import torch,numpy,pandas,scipy,pyarrow,psutil; print("Torch:",torch.__version__,"CUDA:",torch.version.cuda,"GPUs:",torch.cuda.device_count())'], check=True)
ENVIRONMENT_READY = True
''')
    markdown('''## Source tự chứa

Hai cell thu gọn dưới đây được tạo từ Python source trong repo. Notebook không cần upload file `.py` rời.
''')
    for module, source in [('gold_recommendation.py', 'kaggle_recommendation.py'),
                           ('gold_neural_recommendation.py', 'kaggle_neural_recommendation.py')]:
        code('%%writefile /kaggle/working/' + module + '\n' + (ROOT / 'notebooks' / source).read_text(encoding='utf-8'), True)
    config = default_config(algorithm)
    markdown('''## Cấu hình chạy và lưu trữ

Giữ Dataset private. Sửa RESULT_OWNER thành tài khoản Kaggle của bạn; mỗi variant/ngành dùng Dataset
riêng. Auth dùng KaggleHub hoặc Kaggle Secrets `KAGGLE_API_TOKEN` / `KAGGLE_USERNAME` + `KAGGLE_KEY`
như notebook 07–08; bật quyền Secret cho notebook, không paste token vào code.

Một GPU train; scorer chia query shards cho các GPU được chỉ định. Hai GPU không gộp VRAM.
Nếu chỉ có một GPU, đặt SCORE_DEVICES=['cuda:0']. CPU phải chọn rõ cả TRAIN_DEVICE và SCORE_DEVICES.
Validation-only: PHASES=['validation'], PUBLISH_RESULTS=False rồi Save Version/Output để giữ checkpoint.
Muốn hạ MAX_EPOCHS, sửa EVAL_EPOCHS để mốc cuối bằng trần mới và điều chỉnh EARLY_STOP_START_EPOCH;
nếu chỉ smoke test ngắn có thể tắt EARLY_STOPPING rõ ràng. Giảm chiều không làm nhỏ input MiniLM/cache D2.
''')
    code('''CATEGORIES_TO_RUN = ['All_Beauty', 'Video_Games', 'Industrial_and_Scientific']
VARIANTS_TO_RUN = ''' + repr(config['variants']) + '''
PHASES = ['validation', 'test']
TRAIN_DEVICE = 'cuda:0'
SCORE_DEVICES = ['cuda:0', 'cuda:1']
GOLD_ROOT = None
EMBEDDING_ROOT = None
BUNDLE_NAME = None  # chạy nhiều ngành: để None, tự chọn theo hash của từng ngành
RESUME_ROOT = None  # thư mục neural_results/recommendation_results chứa các run từ Output cũ
PUBLISH_RESULTS = True
AUTO_RESUME = True
RESULT_OWNER = 'thoandanh'
INPUT_VERSIONS = {'gold': None, 'embedding': None}  # điền versions đang gắn vào Input
TRAIN_BATCH = 4096  # số pairs gần đúng sau sampling, tối thiểu 1 positive + negatives của anchor
ADAM_FOREACH = True
PREFETCH_TRAIN = True
SCORE_FEATURE_CACHE = True
QUERY_BATCH = 32
ITEM_BLOCK = 8192
PAIR_BATCH = 65536
SCORE_SHARD_QUERIES = 128
ID_EMBED_DIM = 8
MLP_HIDDEN = [32, 16]
PROJECTION_DIM = 16
MAX_EPOCHS = 20
EVAL_EPOCHS = [1, 3, 6, 9, 12, 15, 18, 20]
EARLY_STOPPING = True
EARLY_STOP_START_EPOCH = 6
EARLY_STOP_PATIENCE = 2  # số lần full validation, không phải số epochs
EARLY_STOP_MIN_DELTA = 0.00001
NEGATIVES_PER_POSITIVE = 4
TRIALS = [dict(name='C' + str(ID_EMBED_DIM), dim=ID_EMBED_DIM, hidden=MLP_HIDDEN,
               lr=0.001, weight_decay=0.00001)]
RESULT_DATASETS = {
    variant + '/' + category: RESULT_OWNER + '/gold-' + variant.lower().replace('_','-') + '-results-' + category.lower().replace('_','-')
    for variant in VARIANTS_TO_RUN for category in CATEGORIES_TO_RUN
}
CONFIG = ''' + repr(config) + '''
CONFIG.update(categories=CATEGORIES_TO_RUN, variants=VARIANTS_TO_RUN, phases=PHASES,
    train_device=TRAIN_DEVICE, score_devices=SCORE_DEVICES, gold_root=GOLD_ROOT,
    embedding_root=EMBEDDING_ROOT, embedding_bundle=BUNDLE_NAME, resume_root=RESUME_ROOT,
    publish_results=PUBLISH_RESULTS, auto_resume=AUTO_RESUME, result_datasets=RESULT_DATASETS,
    dataset_versions=INPUT_VERSIONS, train_batch=TRAIN_BATCH, query_batch=QUERY_BATCH,
    item_block=ITEM_BLOCK, pair_batch=PAIR_BATCH, score_shard_queries=SCORE_SHARD_QUERIES,
    max_epochs=MAX_EPOCHS, eval_epochs=EVAL_EPOCHS, negatives=NEGATIVES_PER_POSITIVE, trials=TRIALS,
    projection_dim=PROJECTION_DIM, early_stopping=EARLY_STOPPING, early_stop_start_epoch=EARLY_STOP_START_EPOCH,
    early_stop_patience=EARLY_STOP_PATIENCE, early_stop_min_delta=EARLY_STOP_MIN_DELTA,
    adam_foreach=ADAM_FOREACH, prefetch_train=PREFETCH_TRAIN, score_feature_cache=SCORE_FEATURE_CACHE)
CONFIG_PATH = Path('/kaggle/working/''' + algorithm + '''_config.json')
''')
    markdown('''## Train, validation và refit/test

Train dùng BCEWithLogitsLoss/Adam FP32. Positives/negatives giống nhau giữa các variants, sampler
uniform loại observed train items, không xem future labels. Không sigmoid trước loss; logits
dùng trực tiếp để xếp hạng. Sigmoid không phải xác suất mua hàng đã hiệu chỉnh.

Val train trước 2021, labels nửa đầu 2021; test refit trước 2022, labels nửa đầu 2022 (UTC).
Chọn config/epoch bằng exact warm-full NDCG@10, rồi Recall@10, số tham số ít hơn và epoch sớm hơn.
Đánh giá best trên all_queries và subgroup; cold users dùng Popularity. Không lọc seen items,
giữ target ngoài candidates trong mẫu số. Test chỉ dùng config/epoch đã khóa, không early-stop theo test.

Early stopping theo warm-full NDCG@10: trước epoch 6 chỉ theo dõi; từ epoch 6, hai lần full validation
liên tiếp không tăng ít nhất min_delta sẽ dừng. Với lịch mặc định, có thể dừng sớm nhất ở epoch 9.
Mọi best checkpoint thực tế vẫn được giữ dù mức tăng nhỏ hơn min_delta. Train loss là tín hiệu chẩn đoán,
không phải tiêu chí dừng độc lập. Nếu dừng ở 12 nhưng best ở 6, refit/test chỉ train 6 epochs.
MAX_EPOCHS là trần ngân sách, không phải cam kết hội tụ; log phân biệt ranking_plateau và max_epochs.

Scoring neural có thể lâu hơn train. Đo pairs/giây của shard đầu để dự toán; mỗi shard được lưu
ngay. Giảm query/item/pair batch chỉ đổi cách tính, không giảm candidates hoặc population.
D2 dùng cache memmap, vẫn cần đủ disk cho prefix profiles; notebook kiểm tra disk trước khi tạo.

Tối ưu runtime: sampler uniform theo batch với seed/example/epoch ổn định, một CPU batch chuẩn bị
trước (pinned memory khi train CUDA), Adam foreach và một lần kiểm tra finite trước mỗi update.
Train batch mặc định vẫn 4096 để giữ số optimizer steps; có thể thử 8192/16384 nhưng phải đánh giá
lại validation vì batch lớn thay đổi quá trình học. Log mỗi epoch có pairs/giây và thời gian chờ batch.
Scoring giữ logits/Top-K trên device, chỉ chuyển Top-20 sau khi duyệt đủ catalog của query batch.
D1/D2 cache projection theo từng model checkpoint, không cache projection giữa các optimizer steps.
Cache rebuilt cho checkpoint mới, không làm thay đổi weights hoặc candidates. FP32 giữ nguyên.
Nếu scoring hết VRAM: giảm PAIR_BATCH rồi QUERY_BATCH/ITEM_BLOCK; nếu cache quá lớn, đặt
SCORE_FEATURE_CACHE=False. ADAM_FOREACH=False giảm bộ nhớ tạm optimizer. Hai T4 vẫn không gộp VRAM.
''')
    code('''if not ENVIRONMENT_READY:
    raise RuntimeError('Chạy thành công cell cài dependency trước khi train')
CONFIG_PATH.write_text(json.dumps(CONFIG, indent=2), encoding='utf-8')
subprocess.run([sys.executable, '/kaggle/working/gold_neural_recommendation.py', '--config', str(CONFIG_PATH)], check=True)
''')
    markdown('''## Đọc kết quả

Mỗi run có best_config.json, trials.csv, loss.csv theo trial, model best/last, mappings, metrics,
validation.csv, stop_reason/stopped_epoch/selected_epoch, Top-20, per-query metrics, coverage/fallback,
CI và resource logs. Trial không thắng giữ metrics/loss,
bỏ model payload; best chứa trọng số. Last giữ optimizer/RNG khi train còn dở; trial/refit đã complete
được thu gọn còn weights/epoch để giảm dung lượng upload. D2 giữ query features và
scaler sau khi phase complete, thu gọn prefix cache train để giảm dung lượng. Giải mã item_indices bằng
mappings của đúng fold. So sánh ALS/Content chỉ khi cùng Gold/category/fold/query population.
''')
    code('''import pandas as pd
import matplotlib.pyplot as plt
SUMMARY = json.loads((Path(CONFIG['output_root']) / (CONFIG['algorithm'] + '_suite_results.json')).read_text())
tables = []
for result in SUMMARY:
    root = Path(result['run_root'])
    print(result['category'], result['variant'], 'Dataset:', result['dataset'], 'Upload:', result['upload'])
    display(pd.read_csv(root / 'trials.csv'))
    for loss in sorted((root / 'trials').glob('*/loss.csv')):
        trial_state = json.loads((loss.parent / 'trial.json').read_text())
        print(loss.parent.name, 'stop:', trial_state['stop_reason'], 'trained:', trial_state['stopped_epoch'],
              'selected:', trial_state['selected_epoch'])
        frame = pd.read_csv(loss)
        ax = frame.plot(x='epoch', y='loss', marker='o', title=result['category'] + ' / ' + result['variant'] + ' / ' + loss.parent.name)
        ax.set_ylabel('Train BCE'); plt.tight_layout(); plt.show()
        validation = pd.read_csv(loss.parent / 'validation.csv')
        ax = validation.plot(x='epoch', y='ndcg_at10', marker='o', title=result['category'] + ' / ' + result['variant'] + ' / validation')
        ax.axvline(trial_state['selected_epoch'], color='green', linestyle='--', label='selected epoch')
        ax.set_ylabel('Warm-full NDCG@10'); ax.legend(); plt.tight_layout(); plt.show()
    for phase in CONFIG['phases']:
        frame = pd.read_csv(root / phase / 'evaluation/metrics.csv')
        frame.insert(0, 'category', result['category']); frame.insert(1, 'variant', result['variant']); frame.insert(2, 'phase', phase)
        tables.append(frame)
metrics = pd.concat(tables, ignore_index=True)
display(metrics[metrics.cohort.isin(['all_queries', 'warm_full'])])
display(metrics[~metrics.cohort.isin(['all_queries', 'warm_full'])])
''')
    markdown('''## Tiếp tục khi bị ngắt

Chạy lại cùng config/source. Khi train chưa xong, last chứa model, optimizer, epoch và RNG; epoch đang dở
sẽ chạy lại từ cuối epoch đã lưu. Scoring bỏ qua shards hợp lệ; checkpoint sai hash/source bị từ chối.
Checkpoint lưu loss history, best epoch và bộ đếm early stopping; không đếm lại cùng một mốc validation.
Kiến trúc/budget mới có signature mới; weights/checkpoints của bản ba trials cũ không được trộn vào run này.
Sampler mới vẫn uniform/train-only nhưng đổi chuỗi random so với bản PCG từng anchor; source signature
ngăn trộn model/ranking trước và sau tối ưu. Việc tối ưu runtime chưa được đo trên GPU tại máy local.
Auto-resume lấy Dataset kết quả đã hoàn tất. Với run đang dở, Save Version/Output rồi gắn lại Output
và đặt RESUME_ROOT (ưu tiên hơn auto-resume từ Dataset); checkpoint local không tự trở thành bản lưu bền vững khi phiên mất.

Upload sau mỗi variant/ngành đã đủ validation/test, không đợi hết ba ngành. submitted_processing
chỉ xác nhận API đã nhận upload; nếu upload_uncertain cần kiểm tra version remote trước khi retry.
Không đổi siêu tham số sau khi xem test. Không chạy song song category hoặc nhiều trial trên cùng GPU.

Nguồn: [NCF](https://arxiv.org/abs/1708.05031), [DeepFM](https://arxiv.org/abs/1703.04247),
[PyTorch BCEWithLogitsLoss](https://docs.pytorch.org/docs/stable/generated/torch.nn.BCEWithLogitsLoss.html).
''')
    for index, cell in enumerate(cells):
        cell['id'] = f'{algorithm}-{index:02d}'
    notebook = dict(nbformat=4, nbformat_minor=5, cells=cells, metadata=dict(
        kernelspec=dict(display_name='Python 3', language='python', name='python3'),
        language_info=dict(name='python', version='3.11')))
    target = ROOT / 'notebooks' / filename
    if target.exists():
        backups = ROOT / 'outputs/validation/recommendation-notebook-backups'; backups.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(target, backups / (target.stem + '_' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ') + '.ipynb'))
    target.write_text(json.dumps(notebook, ensure_ascii=False, indent=1) + '\n', encoding='utf-8')
    print(target.name)


def build_two_tower():
    """Keep the Two-Tower notebook focused; embed the shared tested runtime."""
    cells = []
    def md(text):
        cells.append(dict(cell_type='markdown', metadata={}, source=text.splitlines(keepends=True)))
    def code(text, hidden=False):
        cells.append(dict(cell_type='code', execution_count=None, outputs=[],
            metadata={'jupyter': {'source_hidden': True}} if hidden else {}, source=text.splitlines(keepends=True)))
    md('''# Hybrid Two-Tower TT0 / TT1 / TT2 — Kaggle

Notebook tự chứa, không cần upload `.py` hoặc clone repo. Chọn kernel mới Python >=3.10,
GPU T4 × 2 và Internet để cài dependencies. Giữ Torch/CUDA của image Kaggle.
Chạy một category trước để đo tài nguyên; mặc định All_Beauty và đủ ba variants.

| Input private | Dùng cho |
|---|---|
| thoandanh/gold-training-v2 | Tất cả variants |
| thoandanh/gold-emb-all-beauty-v2 | TT1/TT2 All_Beauty |
| thoandanh/gold-emb-video-games-v2 | TT1/TT2 Video_Games |
| thoandanh/gold-emb-industrial-and-scientific-v2 | TT1/TT2 Industrial_and_Scientific |

Thêm Dataset đúng ngành vào Input, pin version trong Kaggle và ghi INPUT_VERSIONS bên dưới.
Runtime verify complete, checksums, source export hash, encoder/revision và mappings riêng fold.
Không encode/fine-tune MiniLM. Đây là mở rộng theo Two-Tower retrieval của Google,
không phải tái lập loss Mixed Negative Sampling của paper.
''')
    code('''import os, sys, json, subprocess
from pathlib import Path
ENVIRONMENT_READY = False
for variable in ['OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS', 'OMP_NUM_THREADS']:
    os.environ[variable] = '1'
os.environ['CUBLAS_WORKSPACE_CONFIG'] = ':4096:8'
if sys.version_info < (3, 10):
    raise RuntimeError('Cần Python >=3.10')
packages = (['numpy>=2.1,<3', 'scipy>=1.14.1,<2', 'pandas>=2.2.3,<4', 'pyarrow>=20,<24']
    if sys.version_info >= (3, 13) else ['numpy==1.26.4', 'scipy==1.14.1', 'pandas==2.2.3', 'pyarrow==20.0.0'])
subprocess.run([sys.executable, '-m', 'pip', 'install', '-q', '--only-binary=:all:',
    *packages, 'psutil', 'matplotlib', 'kagglehub>=1.0.3,<2'], check=True)
subprocess.run([sys.executable, '-c', 'import torch,numpy,pandas,scipy,pyarrow,psutil; print(torch.__version__,torch.version.cuda,torch.cuda.device_count())'], check=True)
ENVIRONMENT_READY = True
''')
    md('''## Hai encoder độc lập

TT0: User ID → MLP → vector; Item ID → MLP → vector.
TT1 thêm item text MiniLM 384 → Linear 16 và has_text vào item input.
TT2 thêm review profile 384 → Linear 16 và 7 thống kê rating/review vào user input.
TT2: user concat 31 → Linear 64 → ReLU → Linear 32;
item concat 25 → Linear 64 → ReLU → Linear 32. ID dim=8.
Output không L2 normalize, score = dot product, không thêm MLP tương tác sau hai tower.
Thiếu item text dùng zero + flag; item vẫn có ID và vẫn được xếp hạng.

Review profile là mean event-weighted rồi L2 normalize. Mỗi anchor chỉ lấy events
**strictly trước prediction_time**, loại cả group cùng timestamp; không dùng target review/rating.
Scaler fit positive train anchors riêng fold. Giữ mọi positive kể cả history rỗng.
Metadata sản phẩm là snapshot tĩnh, chưa chứng minh đúng phiên bản tại thời điểm lịch sử.
''')
    for module, source in [('gold_recommendation.py', 'kaggle_recommendation.py'),
                           ('gold_neural_recommendation.py', 'kaggle_neural_recommendation.py')]:
        code('%%writefile /kaggle/working/' + module + '\n' + (ROOT / 'notebooks' / source).read_text(encoding='utf-8'), True)
    md('''## Cấu hình và thứ tự chạy

TRAIN_BATCH là ngân sách scored pairs, không phải số positives: 20480 ≈4096 positives với 4 negatives.
Một GPU train; hai GPU chia query shards khi scoring, không gộp VRAM. Nếu một GPU, dùng
SCORE_DEVICES=['cuda:0']; chạy CPU phải đổi cả TRAIN_DEVICE và SCORE_DEVICES=['cpu'].
Không đủ VRAM thì giảm TRAIN_BATCH/QUERY_BATCH/ITEM_BLOCK; đổi train batch cần chọn lại epoch.
TT2 dùng memmap nhưng vẫn cần nhiều GB disk cho profiles, nhất là hai ngành lớn.

Chạy hai lượt để khóa model trước khi xem test. Lượt 1 dùng RUN_MODE='validation': validation
cho đủ category/variant, không upload; nếu đã có validation run, gắn Output cũ và đặt RESUME_ROOT
để resume/reuse, sau đó Save Version/Output để lưu đủ best checkpoints.
Lượt 2 dùng RUN_MODE='category_test', chỉ một TEST_CATEGORY mỗi lần; gắn Output validation,
đặt RESUME_ROOT đến thư mục `two_tower_results` trong Input, rồi bật upload.
Suite reuse validation, chạy test cho ba T của category đó và upload riêng từng T/category.
Mỗi Dataset upload cần chỗ trống tạm khoảng 2× bundle + 5 GiB. Receipt `submitted_processing`
chỉ xác nhận đã gửi upload; giữ bản local cho tới khi xác minh Dataset đã xử lý và đọc được.
Trước khi dọn run lớn, cell phân tích lưu metrics, per-query và CI thành gói gọn; Save Version/Output
gói này riêng. Không xóa thư mục khi remote chưa được xác minh.
''')
    code('''RUN_MODE = 'validation'  # lượt 1: validation; lượt 2: category_test
VALIDATION_CATEGORIES = ['All_Beauty', 'Video_Games', 'Industrial_and_Scientific']
TEST_CATEGORY = 'All_Beauty'  # lượt test chỉ chọn một category
CATEGORIES_TO_RUN = VALIDATION_CATEGORIES if RUN_MODE == 'validation' else [TEST_CATEGORY]
VARIANTS_TO_RUN = ['TT0', 'TT1', 'TT2']
PHASES = ['validation'] if RUN_MODE == 'validation' else ['validation', 'test']
TRAIN_DEVICE = 'cuda:0'
SCORE_DEVICES = ['cuda:0', 'cuda:1']
GOLD_ROOT = None
EMBEDDING_ROOT = None
BUNDLE_NAME = None
RESUME_ROOT = None  # gắn Output cũ ở cả hai lượt để reuse; lượt test cần Output validation
PUBLISH_RESULTS = RUN_MODE == 'category_test'
RESULT_OWNER = 'thoandanh'
INPUT_VERSIONS = {'gold': None, 'embedding': None}
TRAIN_BATCH = 20480  # scored pairs; ~4096 positive anchors với 4 negatives
QUERY_BATCH = 512
ITEM_BLOCK = 16384
SCORE_SHARD_QUERIES = 2048
ID_EMBED_DIM = 8
MLP_HIDDEN = [64]
PROJECTION_DIM = 16
TOWER_OUTPUT_DIM = 32
NEGATIVES_PER_POSITIVE = 4
MAX_EPOCHS = 20
EVAL_EPOCHS = [1, 3, 6, 9, 12, 15, 18, 20]
EARLY_STOPPING = True
EARLY_STOP_START_EPOCH = 6
EARLY_STOP_PATIENCE = 2
EARLY_STOP_MIN_DELTA = 0.00001
CONFIG = ''' + repr(default_config('two_tower')) + '''
CONFIG.update(categories=CATEGORIES_TO_RUN, variants=VARIANTS_TO_RUN, phases=PHASES,
    train_device=TRAIN_DEVICE, score_devices=SCORE_DEVICES, gold_root=GOLD_ROOT,
    embedding_root=EMBEDDING_ROOT, embedding_bundle=BUNDLE_NAME, resume_root=RESUME_ROOT,
    publish_results=PUBLISH_RESULTS, dataset_versions=INPUT_VERSIONS,
    train_batch=TRAIN_BATCH, query_batch=QUERY_BATCH, item_block=ITEM_BLOCK,
    score_shard_queries=SCORE_SHARD_QUERIES, projection_dim=PROJECTION_DIM,
    tower_output_dim=TOWER_OUTPUT_DIM, negatives=NEGATIVES_PER_POSITIVE,
    max_epochs=MAX_EPOCHS, eval_epochs=EVAL_EPOCHS, early_stopping=EARLY_STOPPING,
    early_stop_start_epoch=EARLY_STOP_START_EPOCH, early_stop_patience=EARLY_STOP_PATIENCE,
    early_stop_min_delta=EARLY_STOP_MIN_DELTA,
    trials=[dict(name='T' + str(ID_EMBED_DIM), dim=ID_EMBED_DIM, hidden=MLP_HIDDEN, lr=.001, weight_decay=.00001)],
    result_datasets={variant + '/' + category: RESULT_OWNER + '/gold-' + variant.lower()
        + '-results-' + category.lower().replace('_', '-') for variant in VARIANTS_TO_RUN for category in CATEGORIES_TO_RUN})
CONFIG_PATH = Path('/kaggle/working/two_tower_config.json')
if RUN_MODE not in ('validation', 'category_test'):
    raise ValueError("RUN_MODE phải là 'validation' hoặc 'category_test'")
if 'validation' not in PHASES and not RESUME_ROOT:
    raise ValueError('Gắn Output validation và đặt RESUME_ROOT trước khi chạy riêng phase test')
if TEST_CATEGORY not in ['All_Beauty', 'Video_Games', 'Industrial_and_Scientific']:
    raise ValueError('TEST_CATEGORY không hợp lệ')
''')
    md('''## Huấn luyện và đánh giá

BCEWithLogitsLoss + Adam FP32, uniform negatives loại observed train items, không dùng future labels.
Các negatives là unobserved giả định, không phải dislike đã xác nhận. Sampler dùng toàn train fold
theo contract hiện có, không phải pool causal theo từng anchor. Giữ cùng sampler với NCF/DeepFM.

Validation train trước 2021-01-01, labels nửa đầu 2021; test refit từ đầu trước 2022-01-01,
labels nửa đầu 2022 (UTC). Chọn epoch bằng exact warm_full NDCG@10, rồi Recall@10,
ít tham số và epoch sớm hơn. Test dùng đúng selected_epoch, không early-stop theo test.
Suite hoàn tất validation của mọi job đã chọn trước khi chạy test.
Early stopping bắt đầu tại epoch 6, patience=2 validation checks, min_delta=0.00001.
Giữ best weights thực tế kể cả tiny gain; log phân biệt selected/stopped epoch và stop reason.

Scoring encode item một lần/checkpoint/device rồi dùng matmul theo blocks, giữ Top-20 trên device.
Quét đủ candidates, không ANN/sampled evaluation; không lọc seen items, giữ targets ngoài catalog
trong mẫu số. Unknown users dùng Popularity; known users thiếu text/history vẫn có ID prediction.
All_queries và nguyên warm_full Gold, history bins, repeat/novel, coverage và fallback được báo riêng.
CI 95% bootstrap theo query không đo biến động training giữa nhiều seeds.
''')
    code('''if not ENVIRONMENT_READY:
    raise RuntimeError('Chạy cell dependency thành công trước')
CONFIG_PATH.write_text(json.dumps(CONFIG, indent=2), encoding='utf-8')
subprocess.run([sys.executable, '/kaggle/working/gold_neural_recommendation.py', '--config', str(CONFIG_PATH)], check=True)
''')
    md('''## Kết quả và ablation

Output có mappings đúng fold, best_config, checkpoints, loss/validation.csv, stop state,
item_output_vectors.npy, Top-20, per-query/group metrics, CI với Popularity và resource logs.
TT2 giữ query profiles/scaler sau phase complete; prefix train cache được compact để tiết kiệm disk.
Kết quả mới là full benchmark; không so trực tiếp với pilot ALS khác cutoff/population.
''')
    code('''import pandas as pd
import matplotlib.pyplot as plt
sys.path.insert(0, '/kaggle/working')
import gold_recommendation as rec
results = json.loads((Path(CONFIG['output_root']) / 'two_tower_suite_results.json').read_text(encoding='utf-8'))
tables = []
for result in results:
    root = Path(result['run_root'])
    print(result['category'], result['variant'], root)
    display(pd.read_csv(root / 'trials.csv'))
    for path in sorted((root / 'trials').glob('*/loss.csv')):
        frame = pd.read_csv(path)
        frame.plot(x='epoch', y='loss', marker='o', title=result['category'] + ' / ' + result['variant'])
        plt.tight_layout(); plt.show()
        display(pd.read_csv(path.parent / 'validation.csv'))
    for phase in CONFIG['phases']:
        frame = pd.read_csv(root / phase / 'evaluation/metrics.csv')
        frame.insert(0, 'category', result['category']); frame.insert(1, 'variant', result['variant'])
        frame.insert(2, 'phase', phase); tables.append(frame)
metrics = pd.concat(tables, ignore_index=True)
display(metrics[metrics.cohort.isin(['all_queries', 'warm_full'])])
analysis_root = Path(CONFIG['output_root']) / 'analysis' / (CATEGORIES_TO_RUN[0] if len(CATEGORIES_TO_RUN) == 1 else 'validation-all')
analysis_root.mkdir(parents=True, exist_ok=True)
metrics.to_csv(analysis_root / 'metrics.csv', index=False)
for result in results:
    run_root = Path(result['run_root'])
    variant_dir = analysis_root / result['variant']
    variant_dir.mkdir(parents=True, exist_ok=True)
    for name in ('best_config.json', 'trials.csv'):
        source_file = run_root / name
        if source_file.is_file():
            (variant_dir / name).write_bytes(source_file.read_bytes())
    for phase in CONFIG['phases']:
        source_dir = run_root / phase / 'evaluation'
        target_dir = variant_dir / phase
        target_dir.mkdir(parents=True, exist_ok=True)
        (target_dir / 'metrics.csv').write_bytes((source_dir / 'metrics.csv').read_bytes())
with (analysis_root / 'analysis_manifest.json').open('w', encoding='utf-8') as stream:
    json.dump(dict(category=CATEGORIES_TO_RUN, variants=CONFIG['variants'], phases=CONFIG['phases'],
        dataset_versions=CONFIG.get('dataset_versions'), result_datasets=[r.get('dataset') for r in results],
        local_runs=[r['run_root'] for r in results]), stream, ensure_ascii=False, indent=2)
for category in CATEGORIES_TO_RUN:
    roots = {r['variant']: Path(r['run_root']) for r in results if r['category'] == category}
    for phase in PHASES:
        for first, second in [('TT0', 'TT1'), ('TT1', 'TT2')]:
            if first not in roots or second not in roots:
                continue
            a = roots[first] / phase / 'evaluation'
            b = roots[second] / phase / 'evaluation'
            output = Path(CONFIG['output_root']) / 'comparisons' / category / phase / (second + '-vs-' + first + '.json')
            rec.compare_results(a, b, output, samples=CONFIG['bootstrap_samples'], seed=CONFIG['seed'])
            print(category, phase, second, 'vs', first, 'system + Popularity:', json.loads(output.read_text(encoding='utf-8')))
            af = pd.read_parquet(a / (first.lower() + '_native_per_query.parquet'))
            bf = pd.read_parquet(b / (second.lower() + '_native_per_query.parquet'))
            merged = af.merge(bf, on='query_id', suffixes=('_a', '_b'), how='outer', validate='one_to_one', indicator=True)
            if not (merged['_merge'] == 'both').all() or not (merged.warm_full_a == merged.warm_full_b).all():
                raise ValueError('Native comparison populations differ')
            delta = merged['ndcg@10_b'].to_numpy() - merged['ndcg@10_a'].to_numpy()
            native_ci = {group: rec.paired_ci(delta[mask], CONFIG['bootstrap_samples'], CONFIG['seed'])
                for group, mask in {'all_queries': merged.query_id.notna().to_numpy(), 'warm_full': merged.warm_full_a.to_numpy(bool)}.items()}
            rec.save_json(output.with_name(output.stem + '-native.json'), native_ci)
            compact_comparisons = analysis_root / 'comparisons' / phase
            compact_comparisons.mkdir(parents=True, exist_ok=True)
            for comparison in (output, output.with_name(output.stem + '-native.json')):
                (compact_comparisons / comparison.name).write_bytes(comparison.read_bytes())
            print('native:', native_ci)
''')
    md('''## Dọn run local sau khi xác minh Dataset

Cell xóa không tự chạy. Trước hết mở từng Kaggle Dataset trong `analysis_manifest.json`,
xác nhận version mới đã xử lý xong và có `result_bundle.json` cùng `recommendation_results/`.
Sau đó mới đổi cả hai cờ bên dưới thành True và chạy cell dọn. Nếu upload còn `submitted_processing`,
Dataset chưa kiểm tra được hoặc gói analysis chưa đầy đủ, để cờ False và giữ nguyên run local.
Cell chỉ xóa thư mục run của đúng category đang chọn; gói analysis nhỏ và upload receipts được giữ lại.
''')
    code('''REMOTE_DATASETS_VERIFIED = False  # xác nhận thủ công từng Dataset đã xử lý và đọc được
DELETE_LOCAL_CATEGORY_RUNS = False  # chủ động bật sau khi kiểm tra xong
if REMOTE_DATASETS_VERIFIED and DELETE_LOCAL_CATEGORY_RUNS:
    if RUN_MODE != 'category_test' or len(CATEGORIES_TO_RUN) != 1:
        raise ValueError('Dọn chỉ hỗ trợ một category ở chế độ category_test')
    category = CATEGORIES_TO_RUN[0]
    analysis_root = Path(CONFIG['output_root']) / 'analysis' / category
    required = [analysis_root / 'metrics.csv', analysis_root / 'analysis_manifest.json']
    if not all(path.is_file() for path in required):
        raise ValueError('Gói analysis category chưa hoàn tất; giữ run local')
    base = Path(CONFIG['output_root']).resolve()
    for result in results:
        root = Path(result['run_root']).resolve()
        if result['category'] != category or root.parent != base:
            raise ValueError('Run path/category không an toàn cho cleanup')
        receipt_path = base / (root.name + '_upload_receipt.json')
        receipt = rec.read_json(receipt_path)
        if receipt.get('status') != 'submitted_processing' or receipt.get('handle') != result['dataset']:
            raise ValueError('Upload receipt chưa khớp; giữ run local')
        __import__('shutil').rmtree(root)
    suite_path = base / 'two_tower_suite_results.json'
    if suite_path.is_file():
        summary = rec.read_json(suite_path)
        for row in summary:
            if row.get('category') == category:
                row['run_root'] = None
                row['analysis_root'] = str(analysis_root)
        rec.save_json(suite_path, summary)
    print('Đã dọn run local của', category, '; analysis và receipts vẫn được giữ.')
else:
    print('Cleanup đang tắt; run local được giữ nguyên.')
''')
    md('''## Resume

Last checkpoint lưu model/optimizer/RNG và early-stop state ở ranh giới epoch; epoch dở sẽ chạy lại.
Ranking shards đã verify được dùng lại, signature khác thì từ chối. Thay kiến trúc/config/source
tạo run mới. Output working mất khi phiên mất nếu chưa Save Version/Output.
Upload chỉ xảy ra khi bật PUBLISH_RESULTS; Dataset private và remote checks như 09/10.

Tham khảo: [RecSys 2019](https://research.google/pubs/sampling-bias-corrected-neural-modeling-for-large-corpus-item-recommendations/),
[WWW 2020](https://yangli-cs-ucsb.github.io/paper/www20_yang.pdf).
Kích thước mạng, review/rating features và BCE là lựa chọn của đồ án.
''')
    for index, cell in enumerate(cells):
        cell['id'] = f'two-tower-{index:02d}'
    target = ROOT / 'notebooks/11_kaggle_two_tower.ipynb'
    if target.exists():
        backups = ROOT / 'outputs/validation/recommendation-notebook-backups'
        backups.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(target, backups / ('11_kaggle_two_tower_' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ') + '.ipynb'))
    target.write_text(json.dumps(dict(nbformat=4, nbformat_minor=5, cells=cells,
        metadata=dict(kernelspec=dict(display_name='Python 3', language='python', name='python3'),
                      language_info=dict(name='python', version='3.11'))), ensure_ascii=False, indent=1) + '\n', encoding='utf-8')
    print(target.name)


if __name__ == '__main__':
    build('ncf', '09_kaggle_ncf.ipynb')
    build('deepfm', '10_kaggle_deepfm.ipynb')
    build_two_tower()
