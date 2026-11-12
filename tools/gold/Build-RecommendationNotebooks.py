"""Embed the tested shared source into two standalone Kaggle notebooks."""
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]
source=(ROOT/'notebooks/kaggle_recommendation.py').read_text(encoding='utf-8')


def build(algorithm, filename):
    cells=[]
    def markdown(text): cells.append(dict(cell_type='markdown',metadata={},source=text.splitlines(keepends=True)))
    def code(text,hidden=False): cells.append(dict(cell_type='code',metadata={'jupyter':{'source_hidden':True}} if hidden else {},execution_count=None,outputs=[],source=text.splitlines(keepends=True)))
    name='Implicit ALS full Gold' if algorithm=='als' else 'Content-based mean-history'
    markdown(f'''# {name} trên Kaggle

Chọn GPU T4 × 2, bật Internet để cài dependencies, chạy trên kernel mới. Thêm Dataset riêng tư
`thoandanh/gold-training-v2` vào Input. Mặc định chạy tuần tự cả ba category.
Notebook tự chứa toàn bộ source, không cần upload file Python hoặc chạy Spark/HDFS.

Tự chạy validation → khóa cấu hình → test → upload Dataset private cho mỗi ngành.
Dùng toàn bộ train pairs, không random split lại. Test refit trước 2022;
không chỉnh tham số theo test score. Candidates giữ item đã từng tương tác và nhãn ngoài
candidates vẫn nằm trong mẫu số. Report có all_queries và warm_full, native và Popularity fallback.
''')
    if algorithm=='content':
        markdown('''Thêm Dataset embedding đúng ngành:

| CATEGORY | Dataset |
|---|---|
| All_Beauty | thoandanh/gold-emb-all-beauty-v2 |
| Video_Games | thoandanh/gold-emb-video-games-v2 |
| Industrial_and_Scientific | thoandanh/gold-emb-industrial-and-scientific-v2 |

Bundle phải complete và signature khớp text export Gold. Chỉ lấy item vectors từ title +
description; đọc shards theo batch, không nạp toàn bộ review vectors. User profile là mean
của distinct history items rồi L2-normalize. Metadata là static snapshot, chưa có phiên bản
description tại cutoff lịch sử. Baseline chưa dùng rating/review hoặc fine-tune encoder.
''')
    code('''import os, sys
ENVIRONMENT_READY = False
os.environ['OPENBLAS_NUM_THREADS']='1'
os.environ['MKL_NUM_THREADS']='1'
os.environ['OMP_NUM_THREADS']='1'
if sys.version_info < (3,10):
    raise RuntimeError('Notebook cần Python >= 3.10.')
print('Python:',sys.version)
''')
    code('''import subprocess
# Python >=3.13 giữ các bản có wheel tương thích, không ép numpy 1.x.
modern_python = sys.version_info >= (3,13)
packages = (['numpy>=2.1,<3','scipy>=1.14.1,<2','pandas>=2.2.3,<4','pyarrow>=20,<24']
            if modern_python else ['numpy==1.26.4','scipy==1.14.1','pandas==2.2.3','pyarrow==20.0.0'])
packages += ['psutil','matplotlib','kagglehub>=1.0.3,<2']
DEFAULT_ALS_BACKEND = 'cpu' if modern_python else 'gpu'
# Chỉ bật khi đã xác nhận driver/runtime hỗ trợ CUDA 13; 0.7.3 GPU cần RMM.
INSTALL_ALS_GPU_073 = False
'''+('''packages += [('implicit[gpu]==0.7.3' if INSTALL_ALS_GPU_073 else 'implicit==0.7.3')
             if modern_python else 'implicit==0.7.2']
if modern_python and INSTALL_ALS_GPU_073:
    DEFAULT_ALS_BACKEND = 'gpu'
''' if algorithm=='als' else '')+'''subprocess.run([sys.executable,'-m','pip','install','-q','--only-binary=:all:',*packages],check=True)
# Kiểm tra bằng tiến trình mới, cùng interpreter với tiến trình train.
subprocess.run([sys.executable,'-c',"import numpy, scipy, pandas, pyarrow, psutil, matplotlib'''+(', implicit' if algorithm=='als' else '')+'''; print('Dependency imports OK')"],check=True)
ENVIRONMENT_READY = True
'''+("print('Environment ready. ALS fit backend:',DEFAULT_ALS_BACKEND,'; scoring: cấu hình riêng bên dưới.')" if algorithm=='als' else "print('Environment ready. Content-based: không fit ALS; scoring cấu hình riêng bên dưới.')")+'''
''')
    markdown('''Python 3.13+ dùng implicit 0.7.3 cho ALS, mặc định fit CPU; scoring vẫn dùng hai GPU.
Muốn fit GPU bằng 0.7.3, cần CUDA 13/RMM: xác nhận môi trường rồi bật INSTALL_ALS_GPU_073
trong ô cài đặt. Python 3.10–3.12 giữ implicit 0.7.2 và mặc định GPU fit.
Content-based không cài hoặc dùng implicit. Nếu ô cài đặt lỗi, dừng và sửa lỗi đó trước.
Nếu đã import numpy/scipy trước ô cài package và thấy version không khớp, restart kernel rồi Run All.
Giữ Torch/CUDA của image Kaggle. Source dùng chung nằm trong ô thu gọn dưới đây.
''')
    code('%%writefile /kaggle/working/gold_recommendation.py'+chr(10)+source,True)
    markdown('''## Chọn ngành và Dataset kết quả

Chọn CATEGORIES_TO_RUN; validation và test chạy tự động. Các đường dẫn None tự tìm trong /kaggle/input;
nếu có nhiều bản Dataset/bundle, điền GOLD_ROOT hoặc EMBEDDING_ROOT/BUNDLE_NAME rõ ràng.
Ghi version Dataset đang gắn vào INPUT_VERSIONS để audit; manifest SHA kiểm tra đúng nội dung.

RESULT_DATASETS dùng Dataset riêng từng model/ngành; sửa RESULT_OWNER nếu tài khoản không phải thoandanh.
Bật Internet. API dùng auth sẵn của KaggleHub hoặc Secrets KAGGLE_API_TOKEN; cũng hỗ trợ
Secrets KAGGLE_USERNAME + KAGGLE_KEY. Bật quyền Secret cho notebook, không paste token vào code.
AUTO_RESUME tự tải Dataset kết quả đã có; checkpoint phải cùng source/config/version.
RESUME_ROOT trỏ recommendation_results của Output cũ nếu chưa upload Dataset.
ALGORITHM/seed/backend giữ nguyên khi resume; giảm QUERY_BATCH/ITEM_BLOCK nếu thiếu VRAM.
''')
    if algorithm=='als':
        markdown('''ALS fit trên một GPU, scoring chia query cho cả hai GPU. Preflight dừng nếu implicit không có
CUDA extension; có thể chủ động đổi ALS_BACKEND='cpu' để chạy CPU, không có fallback ngầm.
Validation thử A0–A4 trên đủ warm-full queries và giữ model tốt nhất. Mỗi trial có checkpoint;
nếu dừng giữa fit phải fit lại trial đó, còn scoring tiếp tục theo shard. Không cần Dataset embedding.
''')
    else:
        markdown('''Content-based không có optimizer train: thời gian fit là join vectors và dựng profile. Nó vẫn
cần lịch sử user; user không có lịch sử/content dùng Popularity fallback. Hai GPU chia query
scoring; mỗi GPU giữ item matrix riêng, không gộp VRAM.
''')
    config=dict(algorithm=algorithm,category='All_Beauty',phase='validation',gold_root=None,
        embedding_root=None,embedding_bundle=None,als_backend='gpu',score_devices=['cuda:0','cuda:1'],
        seed=42,cpu_threads=4,query_batch=512,item_block=16384,score_shard_queries=4096,
        bootstrap_samples=1000,min_free_gib=5,output_root='/kaggle/working/recommendation_results',resume_root=None,
        dataset_versions={'gold':None,'embedding':None})
    code('''from pathlib import Path
import json, subprocess, sys

CATEGORIES_TO_RUN = ['All_Beauty', 'Video_Games', 'Industrial_and_Scientific']
PUBLISH_RESULTS = True
AUTO_RESUME = True
RESULT_OWNER = 'thoandanh'
RESULT_DATASETS = {
    category: RESULT_OWNER + '/gold-'''+algorithm+'''-results-' + category.lower().replace('_', '-') + '-v1'
    for category in CATEGORIES_TO_RUN
}
GOLD_ROOT = None
EMBEDDING_ROOT = None
BUNDLE_NAME = None
RESUME_ROOT = None      # thư mục recommendation_results từ Output cũ
ALS_BACKEND = DEFAULT_ALS_BACKEND  # Python 3.13+: CPU fit mặc định, GPU scoring bên dưới
SCORE_DEVICES = ['cuda:0','cuda:1']  # một GPU: ['cuda:0']; CPU: ['cpu']; không tự fallback
QUERY_BATCH = 512
ITEM_BLOCK = 16384
INPUT_VERSIONS = {'gold': None, 'embedding': None}  # điền số version đang gắn

CONFIG = '''+repr(config)+'''
CONFIG.update(categories=CATEGORIES_TO_RUN, publish_results=PUBLISH_RESULTS, auto_resume=AUTO_RESUME,
    result_datasets=RESULT_DATASETS, gold_root=GOLD_ROOT, embedding_root=EMBEDDING_ROOT,
    embedding_bundle=BUNDLE_NAME, resume_root=RESUME_ROOT, als_backend=ALS_BACKEND,
    score_devices=SCORE_DEVICES, query_batch=QUERY_BATCH, item_block=ITEM_BLOCK, dataset_versions=INPUT_VERSIONS)
config_path=Path('/kaggle/working/'''+algorithm+'''_config.json')
''')
    markdown('''## Chạy

Run All tự chạy validation/test rồi upload từng category. Test dùng cấu hình đã khóa.
Nếu một bước lỗi, dừng và giữ kết quả local. Phase complete đúng hash sẽ được bỏ qua khi chạy lại.
Validation/test xếp hạng exact toàn candidates;
không negative sampling để đánh giá. Seed=42, NDCG@10 chọn cấu hình, Recall/NDCG@10/20 báo cáo.
Checkpoint scoring được kiểm tra SHA/source; không trộn một phần output từ phiên bản source khác.
''')
    code('''if not globals().get('ENVIRONMENT_READY',False):
    raise RuntimeError('Ô cài dependency chưa thành công. Chạy lại từ ô môi trường; không bỏ qua lỗi cài đặt.')
'''+("print('ALS fit:',ALS_BACKEND,'; scoring:',SCORE_DEVICES)" if algorithm=='als' else "print('Content-based: dựng profile từ history, không fit ALS; scoring:',SCORE_DEVICES)")+'''
config_path.write_text(json.dumps(CONFIG,indent=2))
subprocess.run([sys.executable,'/kaggle/working/gold_recommendation.py','--config',str(config_path),'--suite'],check=True)
''')
    markdown('''## Đọc kết quả

Mỗi Dataset chứa cả validation/test, model/vectors, mappings, metrics, Top-20 và checkpoints.
submitted_processing nghĩa là API đã nhận upload; Kaggle có thể còn xử lý phía server.
Giữ Output nếu phiên dừng trước upload hoặc cần kiểm tra upload receipt.
''')
    code('''import pandas as pd
SUITE_RESULTS=json.loads((Path(CONFIG['output_root'])/(CONFIG['algorithm']+'_suite_results.json')).read_text())
tables=[]
for result in SUITE_RESULTS:
    run_root=Path(result['run_root'])
    print(result['category'],'Dataset:',result['dataset'],'Upload:',result['upload'])
    if (run_root/'trials.csv').is_file(): display(pd.read_csv(run_root/'trials.csv'))
    for phase in ('validation','test'):
        frame=pd.read_csv(run_root/phase/'evaluation/metrics.csv')
        frame.insert(0,'category',result['category']); frame.insert(1,'phase',phase)
        tables.append(frame)
metrics=pd.concat(tables,ignore_index=True)
display(metrics[metrics.cohort.isin(['all_queries','warm_full'])])
display(metrics[~metrics.cohort.isin(['all_queries','warm_full'])])
''')
    code('''import matplotlib.pyplot as plt
for (category,phase),frame in metrics.groupby(['category','phase'],sort=False):
    chart=frame[frame.cohort.isin(['all_queries','warm_full'])].pivot(index='model',columns='cohort',values='ndcg@10')
    ax=chart.plot.bar(figsize=(9,4),rot=15)
    ax.set_ylabel('NDCG@10'); ax.set_title(category+' / '+phase)
    plt.tight_layout(); plt.show()
''')
    if algorithm=='content':
        markdown('''## Tùy chọn so sánh trực tiếp với ALS

Gắn Output ALS từ cùng Gold/category/fold vào Input. Điền đường dẫn thư mục evaluation của ALS.
Mã kiểm tra query IDs, source hashes và cohort trước khi tính paired bootstrap cho system có fallback.
''')
        code('''ALS_RESULT_ROOTS = {}  # category: thư mục run ALS có validation/ và test/ cùng Gold
if ALS_RESULT_ROOTS:
    import importlib.util
    spec=importlib.util.spec_from_file_location('gold_eval','/kaggle/working/gold_recommendation.py')
    helper=importlib.util.module_from_spec(spec); spec.loader.exec_module(helper)
    for result in SUITE_RESULTS:
        if result['category'] not in ALS_RESULT_ROOTS: continue
        for phase in ('validation','test'):
            run_root=Path(result['run_root'])
            comparison=helper.compare_results(Path(ALS_RESULT_ROOTS[result['category']])/phase/'evaluation',
                run_root/phase/'evaluation',run_root/(phase+'_vs_als.json'))
            print(result['category'],phase,json.dumps(comparison,indent=2))
''')
    markdown('''## Lưu và tiếp tục

Dataset tự tạo mới hoặc thêm version sau validation/test của từng ngành; luôn giữ private.
Save Version/Output nếu phiên dừng trước upload. Chỉ trạng thái complete là đã đủ scoring/metrics;
run failed vẫn có checkpoint model/trial/query hợp lệ để tiếp tục sau khi sửa lỗi.

Chạy lại để tiếp tục; scoring chưa xong tiếp tục theo shard. Nếu upload_uncertain, kiểm tra
version remote trước khi xóa receipt để retry, tránh gửi version trùng.
Content cần embedding của tất cả ngành đã chọn; giảm CATEGORIES_TO_RUN nếu chỉ muốn một ngành.
Không copy lại Gold hoặc toàn bộ embedding vào Output. Hệ số/vector và compact item_indices được giải mã
bằng mappings của chính fold nằm trong Output; không dùng mappings validation để giải mã test.

Nguồn: [implicit ALS](https://benfred.github.io/implicit/api/models/gpu/als.html),
[MiniLM](https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2).
''')
    for index,cell in enumerate(cells): cell['id']=f'{algorithm}-{index:02d}'
    notebook=dict(cells=cells,nbformat=4,nbformat_minor=5,metadata=dict(
        kernelspec=dict(display_name='Python 3',language='python',name='python3'),language_info=dict(name='python',version='3.11')))
    target=ROOT/'notebooks'/filename
    if target.exists():
        backups=ROOT/'outputs/validation/recommendation-notebook-backups'; backups.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(target,backups/(target.stem+'_'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')+'.ipynb'))
    target.write_text(json.dumps(notebook,ensure_ascii=False,indent=1)+chr(10),encoding='utf-8')
    print(target.name)


if __name__=='__main__':
    build('als','07_kaggle_als_full.ipynb')
    build('content','08_kaggle_content_based.ipynb')
