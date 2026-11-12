"""Export pinned temporal Gold, then publish one private KaggleHub training Dataset."""
import argparse
import hashlib
import json
import math
import re
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VERSION = 'gold_training_export_v1'
HANDLE = 'thoandanh/gold-training-v2'
RUNS = {
    'All_Beauty': 'run_20261103T025512333964Z',
    'Video_Games': 'run_20261103T030034694677Z',
    'Industrial_and_Scientific': 'run_20261103T031106166302Z',
}
# Only these columns enter the training Dataset. Labels/cohorts stay separate.
TABLES = {
    'history_events': ('record_hash user_id parent_asin event_time', 'record_hash'),
    'users': ('user_id user_idx', 'user_id'),
    'items': ('parent_asin item_idx', 'parent_asin'),
    'candidates': ('parent_asin item_idx', 'parent_asin'),
    'train_pairs': ('user_id parent_asin user_idx item_idx first_pair_time strength', 'user_id parent_asin'),
    'train_examples': ('example_id user_id parent_asin user_idx item_idx first_pair_time prediction_time history_empty strength', 'example_id'),
    'queries': ('query_id user_id user_idx prediction_time history_events history_items history_times unknown_user', 'query_id'),
    'labels': ('query_id parent_asin item_idx target_time target_record_hash target_event_versions in_candidates is_repeat', 'query_id parent_asin'),
    'cohorts': ('query_id user_id user_idx prediction_time history_events history_items history_times unknown_user target_count targets_in repeat_targets has_history warm_full candidate_group history_events_bin history_items_bin history_times_bin', 'query_id'),
}
REFS = {
    'item_text_refs': ('parent_asin source_record_hash text_id has_text', 'parent_asin'),
    'review_text_refs': ('record_hash user_id parent_asin event_time text_id has_text', 'record_hash'),
}


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(8 * 1024**2), b''):
            digest.update(block)
    return digest.hexdigest()


def json_hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8-sig'))


def write_json(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    temporary.replace(path)


def inside(root, relative):
    root = Path(root).resolve()
    path = (root / relative).resolve()
    if path == root or not path.is_relative_to(root):
        raise ValueError('Path escapes training package')
    return path


def make_plan():
    protocol = read_json(ROOT / 'processing/gold_protocol.json')
    contract = read_json(ROOT / 'processing/gold_contract.json')
    if protocol.get('status') != 'locked' or protocol['protocol_id'] != contract['protocol_id']:
        raise ValueError('Need locked Gold protocol')
    categories = {}
    for category, run in RUNS.items():
        directory = ROOT / 'outputs/processing/gold' / run
        info = read_json(directory / 'run.json')
        export_path = directory / 'kaggle_texts/export_manifest.json'
        export = read_json(export_path)
        evidence = next(x for x in protocol['evidence'] if x['category'] == category)
        expected_path = f"hdfs://namenode:8020/project/processed/gold/{category.lower()}/v1/{run}/{protocol['protocol_id']}"
        if (info.get('status') != 'complete' or info['category'] != category or info['final_path'] != expected_path
                or info.get('protocol') != protocol or info['source_manifest_sha256'] != evidence['silver_manifest_sha256']
                or info['contract_sha256'] != sha256(ROOT/'processing/gold_contract.json')
                or read_json(directory/'verification.json')['status'] != 'passed'
                or read_json(directory/'batch.json')['status'] != 'complete'):
            raise ValueError('Gold handoff mismatch: ' + category)
        parent = info.get('parent_gold_path')
        if not parent or not re.fullmatch(re.escape(expected_path.split('/v1/')[0]) + r'/v1/run_[A-Za-z0-9_]+/' + re.escape(protocol['protocol_id']), parent):
            raise ValueError('Need pinned original fold parent')
        if (export.get('status') != 'complete' or export['gold_path'] != expected_path
                or export.get('item_text_policy') != 'title_description_exact_paragraph_dedup_v1'):
            raise ValueError('Need matching title + description text export')
        categories[category] = dict(gold_path=expected_path, fold_path=parent, silver_path=info['source'],
            source_manifest_sha256=info['source_manifest_sha256'], local_run_sha256=sha256(directory/'run.json'),
            ref_tables=info['tables'], folds=info['folds'],
            text_export_sha256=sha256(export_path), text_export=export)
    plan = dict(export_version=VERSION, protocol=protocol, contract=contract, categories=categories,
        exporter_sha256=sha256(__file__), dataset_handle=HANDLE,
        includes='temporal Gold tables, text refs, narrow historical rating lookup; no raw text or vectors',
        native_ids=True, privacy='private', split_rebuilt=False)
    plan['bundle_id'] = VERSION + '_' + json_hash(plan)[:16]
    return plan


def reserve(path, minimum_gib):
    if not math.isfinite(minimum_gib) or minimum_gib < 0 or shutil.disk_usage(path).free < minimum_gib * 1024**3:
        raise RuntimeError('Insufficient free disk reserve')


def export_tables(spark, fold_root, refs_root, silver_root, destination, parent, refreshed, protocol, min_free_gib=20):
    """Core export; fixture tests use file:// sources, production uses read-only HDFS."""
    import pyarrow.parquet as pq
    from pyspark.sql import functions as F
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=False)
    tables = {}

    def emit(relative, frame, keys, expected_count=None):
        reserve(destination, min_free_gib)
        count = frame.count()
        if expected_count is not None and count != expected_count:
            raise ValueError('Source count mismatch: ' + relative)
        key_names = keys.split()
        if frame.filter(' OR '.join(f'`{k}` IS NULL' for k in key_names)).limit(1).count() or frame.groupBy(*key_names).count().filter('count > 1').limit(1).count():
            raise ValueError('Null/duplicate key: ' + relative)
        output = destination / relative
        frame.write.mode('error').option('maxRecordsPerFile', 100000).parquet(output.as_uri())
        back = spark.read.parquet(output.as_uri())
        if back.count() != count or back.schema.simpleString() != frame.schema.simpleString():
            raise ValueError('Parquet read-back mismatch: ' + relative)
        files = [dict(path=p.relative_to(destination).as_posix(), rows=pq.ParquetFile(p).metadata.num_rows,
            bytes=p.stat().st_size, sha256=sha256(p)) for p in sorted(output.glob('*.parquet'))]
        if sum(f['rows'] for f in files) != count:
            raise ValueError('Shard count mismatch')
        tables[relative] = dict(rows=count, schema=back.schema.jsonValue(), files=files)
        print(f'[GOLD EXPORT] {relative}: {count:,} rows', flush=True)

    for fold in ('validation', 'test'):
        start, end = protocol[fold+'_start'], protocol[fold+'_end']
        for table, (columns, keys) in TABLES.items():
            relative = fold + '/' + table
            frame = spark.read.parquet(fold_root + '/' + relative)
            if columns:
                frame = frame.select(*columns.split())
            if table == 'history_events' and frame.filter(F.col('event_time') >= F.lit(start).cast('timestamp')).limit(1).count():
                raise ValueError('History crosses cutoff')
            if table == 'queries' and frame.filter(F.col('prediction_time') != F.lit(start).cast('timestamp')).limit(1).count():
                raise ValueError('Query cutoff mismatch')
            if table == 'labels' and frame.filter((F.col('target_time') < F.lit(start).cast('timestamp')) | (F.col('target_time') >= F.lit(end).cast('timestamp'))).limit(1).count():
                raise ValueError('Label outside fold window')
            emit(relative, frame, keys, parent['tables'][relative]['rows'])
    for table, (columns, keys) in REFS.items():
        frame = spark.read.parquet(refs_root + '/' + table).select(*columns.split())
        if table == 'review_text_refs' and frame.filter(F.col('event_time') >= F.lit(protocol['test_start']).cast('timestamp')).limit(1).count():
            raise ValueError('Review cache crosses test cutoff')
        emit(table, frame, keys, refreshed['tables']['/'+table]['rows'])
    # G0-G2 omitted rating. Add only a keyed lookup before refit cutoff, never target ratings/features.
    reviews = spark.read.parquet(silver_root+'/reviews_clean').filter(F.col('event_time') < F.lit(protocol['test_start']).cast('timestamp'))
    history_ratings = reviews.select('record_hash', 'rating')
    refs = spark.read.parquet((destination/'review_text_refs').as_uri())
    if refs.select('record_hash').join(history_ratings.select('record_hash'), 'record_hash', 'left_anti').limit(1).count():
        raise ValueError('Missing historical rating lookup')
    emit('history_ratings', history_ratings.join(refs.select('record_hash'), 'record_hash'), 'record_hash', tables['review_text_refs']['rows'])
    return tables


def export_category(category, package, min_free_gib=20):
    from processing.silver import create_spark
    from notebooks.eda_model_readiness import read_manifest
    plan = make_plan()
    entry = plan['categories'][category]
    package = Path(package).resolve()
    package.mkdir(parents=True, exist_ok=True)
    plan_path = package/'export_plan.json'
    if plan_path.exists() and read_json(plan_path) != plan:
        raise ValueError('Package belongs to another code/source plan; choose a new folder')
    write_json(plan_path, plan)
    final = package/category
    if final.exists():
        verify_category(package, category, plan)
        print('[GOLD EXPORT] Existing complete category verified; skipping:', category)
        return
    work_root = package.parent/'_work'
    work_root.mkdir(exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=category+'_', dir=work_root))
    scratch = Path('/tmp/gold-training')/staging.name
    scratch.mkdir(parents=True, exist_ok=False)
    spark = None
    try:
        spark = create_spark(scratch, 2, 32)
        spark.sparkContext.setLogLevel('WARN')
        refreshed = read_manifest(spark, entry['gold_path'])
        parent = read_manifest(spark, entry['fold_path'])
        silver = read_manifest(spark, entry['silver_path'])
        if (refreshed.get('status') != 'complete' or parent.get('status') != 'complete'
                or refreshed.get('category') != category or parent.get('category') != category
                or refreshed.get('parent_gold_path') != entry['fold_path']
                or refreshed.get('protocol') != plan['protocol'] or parent.get('protocol') != plan['protocol']
                or refreshed.get('tables') != entry['ref_tables'] or parent.get('folds') != entry['folds']
                or refreshed['source_manifest_sha256'] != entry['source_manifest_sha256']
                or parent['source_manifest_sha256'] != entry['source_manifest_sha256']
                or json_hash(silver) != entry['source_manifest_sha256']):
            raise ValueError('Published HDFS lineage differs from pinned handoff')
        data = staging/'data'
        tables = export_tables(spark, entry['fold_path'], entry['gold_path'], entry['silver_path'], data,
            parent, refreshed, plan['protocol'], min_free_gib)
        if read_manifest(spark, entry['silver_path']) != silver or read_manifest(spark, entry['gold_path']) != refreshed or read_manifest(spark, entry['fold_path']) != parent:
            raise ValueError('Source manifests changed during export')
        write_json(data/'text_export_manifest.json', entry['text_export'])
        write_json(data/'category_manifest.json', dict(status='complete', category=category, bundle_id=plan['bundle_id'],
            sources={k: v for k, v in entry.items() if k != 'text_export'}, folds=parent['folds'], tables=tables,
            parent_manifest_sha256=json_hash(parent), refreshed_manifest_sha256=json_hash(refreshed)))
        data.rename(final)
        verify_category(package, category, plan)
    except BaseException as exc:
        write_json(staging/'failure.json', dict(status='failed', error_type=type(exc).__name__))
        raise
    finally:
        if spark is not None:
            spark.stop()


def verify_category(package, category, plan):
    import pyarrow.parquet as pq
    directory = Path(package)/category
    info = read_json(directory/'category_manifest.json')
    entry = plan['categories'][category]
    if info.get('status') != 'complete' or info.get('category') != category or info.get('bundle_id') != plan['bundle_id']:
        raise ValueError('Incomplete/wrong category export')
    if info['sources'] != {k: v for k, v in entry.items() if k != 'text_export'}:
        raise ValueError('Category lineage differs from plan')
    expected_tables = {f'{fold}/{table}' for fold in ('validation','test') for table in TABLES} | set(REFS) | {'history_ratings'}
    if set(info['tables']) != expected_tables or json_hash(read_json(directory/'text_export_manifest.json')) != json_hash(entry['text_export']):
        raise ValueError('Training tables/text manifest differ from plan')
    files = [directory/'category_manifest.json', directory/'text_export_manifest.json']
    for relative, table in info['tables'].items():
        columns = (TABLES.get(relative.split('/')[-1]) or REFS.get(relative) or ('record_hash rating','record_hash'))[0]
        paths = [f['path'] for f in table['files']]
        if not paths or len(set(paths)) != len(paths) or sum(f['rows'] for f in table['files']) != table['rows']:
            raise ValueError('Duplicate/empty shards or row mismatch')
        for file in table['files']:
            path = inside(directory, file['path'])
            if path.parent != inside(directory, relative) or path.suffix != '.parquet':
                raise ValueError('Unexpected shard folder')
            if path.stat().st_size != file['bytes'] or sha256(path) != file['sha256']:
                raise ValueError('Training shard checksum/rows mismatch')
            parquet = pq.ParquetFile(path)
            if columns and set(parquet.schema_arrow.names) != set(columns.split()):
                raise ValueError('Unexpected columns in training package')
            if parquet.metadata.num_rows != file['rows']:
                raise ValueError('Training shard checksum/rows mismatch')
            files.append(path)
    return info, files


README = '''# Gold training trên Kaggle

Một Dataset, ba thư mục ngành hàng. Embeddings là ba Dataset riêng, không copy vào đây.
Mỗi category có validation/ và test/: train trước 2021 / refit trước 2022, query/label theo protocol.
Các bảng nằm cùng thư mục với category_manifest.json. Không nối index giữa category/fold.
item_text_refs và review_text_refs dùng đúng bản title+description; join text_id với embeddings.
text_export_manifest.json giữ SHA nguồn để kiểm tra signature.export_manifest_sha256 của vectors.

history_ratings là lookup record_hash → rating của review trước 2022, KHÔNG phải feature D2 đã tổng hợp.
Lọc history_events theo event_time < prediction_time, rồi join review_text_refs/history_ratings;
validation query chỉ dùng history trước 2021, test trước 2022. Loại cả nhóm cùng timestamp tại train anchor.
Labels/cohorts chỉ phục vụ evaluation. Không lấy rating/review của target làm input.
Sampler chưa materialize negative rows. Mọi model dùng cùng positives/cohorts đã khóa.
event_count bị bỏ khỏi train_pairs/train_examples vì là audit toàn kỳ, không phải feature as-of.
Metadata sản phẩm là static snapshot; chưa có phiên bản description tại cutoff.

Gói chứa native user/item IDs cần cho join: giữ Dataset PRIVATE. Không có raw text, token, raw files hoặc vectors.
Không dùng dataset này để chọn lại cutoff theo kết quả test. Chưa train model/G4 feature profiles trong exporter.
'''


def finalize(package):
    package = Path(package).resolve()
    plan = read_json(package/'export_plan.json')
    if plan.get('export_version') != VERSION or set(plan['categories']) != set(RUNS):
        raise ValueError('Need all three planned categories')
    category_manifests = {}
    for category in RUNS:
        verify_category(package, category, plan)
        category_manifests[category] = sha256(package/category/'category_manifest.json')
    write_json(package/'training_contract.json', dict(export_version=VERSION, protocol=plan['protocol'],
        encoder=plan['contract']['encoder'], history_policy=plan['contract']['history_policy'],
        sampler=plan['contract']['sampler'], tables={k: v[0] for k, v in TABLES.items()},
        privacy='private; native IDs retained for joins', d2_profiles_materialized=False,
        evaluation_only=['labels','cohorts'], excluded_features=['event_count']))
    (package/'README.md').write_text(README, encoding='utf-8')
    write_json(package/'dataset_manifest.json', dict(status='complete', export_version=VERSION,
        bundle_id=plan['bundle_id'], categories=category_manifests, plan_sha256=sha256(package/'export_plan.json'),
        contract_sha256=sha256(package/'training_contract.json'), readme_sha256=sha256(package/'README.md')))
    return verify_package(package)


def verify_package(package):
    package = Path(package).resolve()
    plan = read_json(package/'export_plan.json')
    manifest = read_json(package/'dataset_manifest.json')
    if manifest.get('status') != 'complete' or manifest.get('export_version') != VERSION or plan.get('export_version') != VERSION or manifest.get('bundle_id') != plan.get('bundle_id') or set(plan['categories']) != set(RUNS) or set(manifest['categories']) != set(RUNS):
        raise ValueError('Incomplete Dataset; require all three categories')
    for name, key in (('export_plan.json','plan_sha256'),('training_contract.json','contract_sha256'),('README.md','readme_sha256')):
        if sha256(package/name) != manifest[key]:
            raise ValueError('Dataset metadata checksum mismatch')
    files = [package/n for n in ('dataset_manifest.json','export_plan.json','training_contract.json','README.md')]
    for category in RUNS:
        _, category_files = verify_category(package, category, plan)
        if sha256(package/category/'category_manifest.json') != manifest['categories'][category]:
            raise ValueError('Category manifest checksum mismatch')
        files.extend(category_files)
    if len(files) != len(set(files)):
        raise ValueError('Duplicate upload files')
    return dict(status='passed', bundle_id=plan['bundle_id'], categories=list(RUNS), files=len(files),
        bytes=sum(p.stat().st_size for p in files), dataset_manifest_sha256=sha256(package/'dataset_manifest.json')), files


def remote_visibility(handle):
    """Fail closed on a public/unknown existing Dataset; never print credentials."""
    from kagglehub.clients import BackendError, build_kaggle_client
    from requests.exceptions import HTTPError
    from kagglesdk.datasets.types.dataset_api_service import ApiGetDatasetMetadataRequest
    owner, slug = handle.split('/')
    request = ApiGetDatasetMetadataRequest()
    request.owner_slug, request.dataset_slug = owner, slug
    try:
        with build_kaggle_client() as api:
            response = api.datasets.dataset_api_client.get_dataset_metadata(request)
    except (BackendError, HTTPError) as exc:
        response = getattr(exc, 'response', None)
        status = response.status_code if response is not None else getattr(exc, 'error_code', None)
        if status == 404:
            return 'new_private'
        if status in (401, 403):
            raise RuntimeError(
                f'Kaggle HTTP {status}: credentials or Dataset permissions were rejected. '
                '"Kaggle credentials set" only means credentials were set in this Python process. '
                'Check KAGGLE_USERNAME/KAGGLE_KEY in project .env. For interactive login, '
                'generate a current API token at https://www.kaggle.com/settings/api, '
                'paste the complete token without quotes/spaces, and retry upload with --login. '
                'Keep the existing local package; do not export again. '
                'A 403 is not evidence that a Dataset is absent; upload was not attempted.'
            ) from None
        raise RuntimeError('Cannot verify remote Dataset privacy/authentication') from None
    if response.info.is_private is not True:
        raise ValueError('Existing Dataset must be private')
    return 'existing_private'


def configure_auth(login=False, env_path=None):
    """Recognize modern tokens and legacy pairs without exposing or persisting secrets."""
    path = Path(env_path) if env_path is not None else ROOT/'.env'
    if path.is_file():
        from dotenv import dotenv_values
        values = dotenv_values(path, encoding='utf-8-sig', interpolate=False)
        username = (values.get('KAGGLE_USERNAME') or '').strip()
        key = (values.get('KAGGLE_KEY') or '').strip()
        token = (values.get('KAGGLE_API_TOKEN') or '').strip()
        if token or key.startswith('KGAT_'):
            from kagglehub.config import set_kaggle_api_token
            set_kaggle_api_token(token or key)
            print('Kaggle API token loaded from .env (values hidden).')
            return 'dotenv_api_token'
        if username or key:
            if not username or not key:
                raise ValueError('.env must define both KAGGLE_USERNAME and KAGGLE_KEY')
            from kagglehub.config import set_kaggle_credentials
            set_kaggle_credentials(username, key)
            print('Kaggle authentication loaded from .env (values hidden).')
            return 'dotenv_legacy'
    if login:
        import kagglehub
        kagglehub.login()
        return 'interactive'
    return 'existing_configuration'


def upload(package, handle=HANDLE, execute=False, new_version=False, login=False):
    if not re.fullmatch(r'[A-Za-z0-9_-]+/[A-Za-z0-9_-]+', handle):
        raise ValueError('Expected owner/dataset-slug, without version')
    package = Path(package).resolve()
    summary, files = verify_package(package)
    print(json.dumps(dict(summary, handle=handle, action='UPLOAD' if execute else 'DRY RUN'), indent=2))
    if not execute:
        return summary
    import kagglehub
    configure_auth(login=login)
    remote_visibility(handle)
    receipt_path = package.parent/(package.name+'_upload_receipt.json')
    if receipt_path.exists() and not new_version:
        raise FileExistsError('Upload receipt exists. Check Kaggle; --new-version explicitly submits another version.')
    reserve(package.parent, 2*summary['bytes']/1024**3 + 5)  # Snapshot copy plus SDK upload archive.
    # A whitelist-only temporary snapshot prevents accidental upload of CRC/log/tmp files.
    with tempfile.TemporaryDirectory(prefix='gold-upload-', dir=package.parent) as staging:
        staging = Path(staging).resolve()
        if not staging.is_relative_to(package.parent):
            raise ValueError('Invalid snapshot root')
        for path in files:
            target = staging/path.relative_to(package)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, target)
        # Verify the exact tree sent to Kaggle; no unlisted payload is included.
        verify_package(staging)
        receipt = dict(summary, status='upload_started', handle=handle,
            started_at=datetime.now(timezone.utc).isoformat())
        receipt['status'] = 'upload_started'
        write_json(receipt_path, receipt)
        try:
            kagglehub.dataset_upload(handle, str(staging), version_notes='Pinned temporal Gold; '+summary['bundle_id'])
        except BaseException as exc:
            write_json(receipt_path, dict(receipt, status='upload_uncertain', error_type=type(exc).__name__))
            raise
        write_json(receipt_path, dict(receipt, status='submitted_processing', finished_at=datetime.now(timezone.utc).isoformat()))
    print('Upload submitted; wait for Kaggle processing: https://www.kaggle.com/datasets/'+handle)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('plan')
    export = sub.add_parser('export')
    export.add_argument('--category', choices=list(RUNS), required=True)
    export.add_argument('--package', required=True)
    export.add_argument('--min-free-gib', type=float, default=20)
    for name in ('finalize','verify','upload'):
        p = sub.add_parser(name)
        p.add_argument('--package', required=True)
        if name == 'upload':
            p.add_argument('--handle', default=HANDLE)
            p.add_argument('--execute', action='store_true')
            p.add_argument('--new-version', action='store_true')
            p.add_argument('--login', action='store_true')
    args = vars(parser.parse_args())
    command = args.pop('command')
    if command == 'plan':
        plan = make_plan()
        print(json.dumps(dict(bundle_id=plan['bundle_id'], dataset_handle=plan['dataset_handle'],
            categories={c: {k: v for k, v in e.items() if k in ('gold_path','fold_path','silver_path')} for c,e in plan['categories'].items()},
            privacy=plan['privacy'], native_ids=plan['native_ids'], includes=plan['includes']), ensure_ascii=False, indent=2))
    elif command == 'export':
        export_category(**args)
    elif command == 'finalize':
        print(json.dumps(finalize(**args)[0], indent=2))
    elif command == 'verify':
        print(json.dumps(verify_package(**args)[0], indent=2))
    else:
        upload(**args)


if __name__ == '__main__':
    main()
