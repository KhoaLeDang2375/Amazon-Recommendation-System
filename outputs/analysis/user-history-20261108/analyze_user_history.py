"""Count train-only distinct-item support from the pinned Gold export (aggregate outputs only)."""
import hashlib
import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[3]
EXPORT = ROOT / 'outputs/processing/gold_training/gold_training_export_v1_566ed959afa207d6'
OUTPUT = Path(__file__).resolve().parent
CATEGORIES = ['All_Beauty', 'Video_Games', 'Industrial_and_Scientific']


def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(8 * 1024**2), b''):
            digest.update(block)
    return digest.hexdigest()


def read_json(path):
    return json.loads(path.read_text(encoding='utf-8-sig'))


def parameter_counts(users, items, dim=32):
    shared = (2 * dim + 1) * 64 + 65 * 32 + 33 * 16
    ncf = 2 * (users + items) * dim + shared + 16 + dim + 1
    d0 = (users + items) * (dim + 1) + shared + 17 + 1
    d1 = d0 + 385 * 32 + 33 * 64
    return dict(NCF_ID=ncf, D0=d0, D1=d1, D2=d1 + 385 * 32 + 39 * 64)


dataset_path = EXPORT / 'dataset_manifest.json'
dataset = read_json(dataset_path)
assert dataset['status'] == 'complete'
records = []
for category in CATEGORIES:
    directory = EXPORT / category
    manifest_path = directory / 'category_manifest.json'
    assert sha256(manifest_path) == dataset['categories'][category]
    manifest = read_json(manifest_path)
    assert manifest['status'] == 'complete'
    for fold, cutoff in [('validation', '2021-01-01'), ('test', '2022-01-01')]:
        tables = manifest['tables']
        nusers = tables[fold + '/users']['rows']
        nitems = tables[fold + '/items']['rows']
        specification = tables[fold + '/train_pairs']
        counts = np.zeros(nusers, np.int64)
        rows = 0
        for shard in specification['files']:
            path = (directory / shard['path']).resolve()
            assert path.is_relative_to(directory.resolve()) and path.suffix == '.parquet'
            assert path.stat().st_size == shard['bytes'] and sha256(path) == shard['sha256']
            parquet = pq.ParquetFile(path)
            assert parquet.metadata.num_rows == shard['rows']
            for batch in parquet.iter_batches(batch_size=131072, columns=['user_idx'], use_threads=False):
                column = batch.column(0)
                assert pa.types.is_integer(column.type) and column.null_count == 0
                users = column.to_numpy(zero_copy_only=False).astype(np.int64, copy=False)
                assert len(users) and users.min() >= 0 and users.max() < nusers
                unique, frequencies = np.unique(users, return_counts=True)
                counts[unique] += frequencies
                rows += len(users)
        assert rows == specification['rows'] == int(counts.sum())
        assert (counts > 0).all(), 'Every train mapping user must have at least one observed pair'
        bins = {str(k): int((counts == k).sum()) for k in range(1, 6)}
        bins['>5'] = int((counts > 5).sum())
        assert sum(bins.values()) == nusers
        original = parameter_counts(nusers, nitems)
        thresholds = []
        for threshold in range(2, 7):
            mask = counts >= threshold
            kept = int(mask.sum())
            shared_rows = kept + int(kept < nusers)
            smaller = parameter_counts(shared_rows, nitems)
            thresholds.append(dict(min_items=threshold, users_at_or_above=kept, users_pct=100 * kept / nusers,
                pairs_from_these_users=int(counts[mask].sum()), pair_pct=100 * int(counts[mask].sum()) / rows,
                hypothetical_user_table_rows=shared_rows, dim32_parameters=smaller,
                ncf_parameter_reduction_pct=100 * (1 - smaller['NCF_ID'] / original['NCF_ID']),
                deepfm_d2_parameter_reduction_pct=100 * (1 - smaller['D2'] / original['D2'])))
        record = dict(category=category, fold=fold, train_end_exclusive_utc=cutoff, users=nusers, items=nitems,
            train_pairs=rows, bins=bins, percentages={key: 100 * value / nusers for key, value in bins.items()},
            thresholds=thresholds, original_dim32_parameters=original,
            category_manifest_sha256=sha256(manifest_path))
        records.append(record)
        print(json.dumps(record, ensure_ascii=False), flush=True)

result = dict(dataset_manifest_sha256=sha256(dataset_path), source=str(EXPORT),
    definition='All mapped train users; count distinct user-item pairs, not raw review events; no evaluation-cohort filter',
    checks='Dataset/category/shard hashes and sizes; Parquet row counts; integer IDs/range; sum of degrees and all-positive user degrees; exhaustive bins',
    hypothetical_reduction='Keep full item mapping and all examples; users below threshold share one user embedding/bias row. Analysis only; no notebook/data changes.',
    records=records)
(OUTPUT / 'user_history_counts.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')

lines = ['# Phân bố lịch sử user và kích thước embedding', '',
    'Ngày tính: 08/11/2026. Đếm trên Gold training đã pin, toàn bộ user train của từng ngành/fold.',
    'Một tương tác là một distinct user–item pair; nhiều review cùng item chỉ tính một lần.', '',
    'Validation: train trước 01/01/2021 UTC. Refit/test: train trước 01/01/2022 UTC.', '']
for fold in ('validation', 'test'):
    subset = [r for r in records if r['fold'] == fold]
    lines += ['## ' + ('Train dùng validation' if fold == 'validation' else 'Train refit dùng test'), '',
        '| Số item/user | All Beauty | Video Games | Industrial and Scientific |', '|---|---:|---:|---:|']
    for label in ['1', '2', '3', '4', '5', '>5']:
        lines.append('| ' + label + ' | ' + ' | '.join(f"{r['bins'][label]:,} ({r['percentages'][label]:.2f}%)" for r in subset) + ' |')
    lines += ['| Tổng user | ' + ' | '.join(f"{r['users']:,}" for r in subset) + ' |', '']

lines += ['## Giả định giảm bảng user embedding', '',
    'Chưa sửa notebook. Mô phỏng chỉ cấp embedding riêng cho user có ít nhất k items; user còn lại dùng một hàng chung.',
    'Giữ toàn bộ item mapping, số chiều 32 và kiến trúc Dense hiện tại. Tỷ lệ dưới đây là giảm tham số, không phải đo tăng chất lượng.', '',
    '| Fold | Ngành | k | User đủ lịch sử | % user | % train pairs của nhóm đó | Giảm tham số NCF | Giảm tham số D2 |',
    '|---|---|---:|---:|---:|---:|---:|---:|']
for r in records:
    for t in r['thresholds']:
        lines.append(f"| {r['fold']} | {r['category']} | {t['min_items']} | {t['users_at_or_above']:,} | {t['users_pct']:.2f}% | {t['pair_pct']:.2f}% | {t['ncf_parameter_reduction_pct']:.2f}% | {t['deepfm_d2_parameter_reduction_pct']:.2f}% |")
lines += ['', 'Không chọn ngưỡng bằng lịch sử sau cutoff hoặc kết quả test. Dùng chung embedding làm mất phân biệt ID của nhóm ít lịch sử;',
    'có thể cần nhánh content/history encoder hoặc chính sách fallback. Giữ all-query evaluation nếu đổi cách biểu diễn user.', '',
    'Giảm dim 32 → 16 làm gần một nửa phần tham số embeddings; FP16/quantization giảm byte lưu, không làm giảm số tham số.', '',
    'Chi tiết số nguyên, provenance và checks nằm trong user_history_counts.json cùng thư mục.']
(OUTPUT / 'USER_HISTORY_REPORT.md').write_text('\n'.join(lines) + '\n', encoding='utf-8')
