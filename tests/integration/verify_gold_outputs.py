"""Read-only production handoff: compare full Gold cohort bins to independent EDA."""
import argparse
import hashlib
import json
import re
import sys
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from processing import gold
from processing.silver import create_spark, local_json
from notebooks.eda_model_readiness import read_manifest
from pyspark.sql import functions as F


def verify(run_id):
    if not re.fullmatch(r"run_[A-Za-z0-9_]{8,64}", run_id):
        raise ValueError("Invalid run ID")
    directory = Path('/workspace/outputs/processing/gold') / run_id
    info = json.loads((directory/'run.json').read_text())
    if json.loads((directory/'batch.json').read_text(encoding='utf-8-sig'))['status'] != 'complete':
        raise ValueError('Host runner did not complete')
    if info['status'] != 'complete' or info['testing'] or gold.file_hash(gold.__file__) != info['code_sha256']:
        raise ValueError('Need complete production run from current producer')
    if gold.file_hash(Path(gold.__file__).with_name('gold_contract.json')) != info['contract_sha256']:
        raise ValueError('Contract changed')
    spark = create_spark(Path('/tmp/gold-qa')/run_id, 2, 16)
    checks = []
    try:
        published = read_manifest(spark, info['final_path'])
        if published['status'] != 'complete' or published['folds'] != info['folds'] or published['tables'] != info['tables']:
            raise ValueError('Published and local manifests differ')
        source = read_manifest(spark, info['source'])
        if hashlib.sha256(json.dumps(source,sort_keys=True).encode()).hexdigest() != info['source_manifest_sha256']:
            raise ValueError('Silver manifest changed')
        fold_path = info.get('parent_gold_path', info['final_path'])
        if info.get('parent_gold_path'):
            parent = read_manifest(spark, fold_path)
            if parent['status'] != 'complete' or parent['folds'] != info['folds'] or parent['source_manifest_sha256'] != info['source_manifest_sha256']:
                raise ValueError('Parent fold lineage mismatch')
        for fold in ('validation','test'):
            entry = next(r for r in info['protocol']['evidence' if fold=='validation' else 'test_evidence'] if r['category']==info['category'])
            eda = json.loads(Path(entry['path']).read_text())
            if eda['status'] != 'complete' or eda['silver_manifest_sha256'] != info['source_manifest_sha256']:
                raise ValueError('EDA/source mismatch')
            start = info['protocol'][fold+'_start']
            expected = {(r['cohort'],r['measure'],r['bin']):r['count'] for r in eda['histories'] if r['start']==start and r['months']==info['protocol']['months']}
            cohorts = spark.read.parquet(fold_path+'/'+fold+'/cohorts')
            actual = {}
            for name,frame in (('all_queries',cohorts),('warm_full',cohorts.filter('warm_full'))):
                rows = frame.select(F.explode(F.array(*[F.struct(F.lit(m).alias('measure'),F.col(m+'_bin').alias('bin'))
                    for m in ('history_events','history_items','history_times')])).alias('v')).select('v.*').groupBy('measure','bin').count().collect()
                actual.update({(name,r.measure,r.bin):r['count'] for r in rows})
            if actual != expected:
                raise ValueError('Gold history/cohort distributions differ from EDA')
            queries = spark.read.parquet(fold_path+'/'+fold+'/queries')
            if set(queries.columns) & {'target_time','target_record_hash','targets_in','target_count','parent_asin','rating','review_text'}:
                raise ValueError('Target fields leaked into queries')
            if queries.filter(F.col('prediction_time') != F.lit(start).cast('timestamp')).limit(1).count():
                raise ValueError('Wrong query cutoff')
            labels = spark.read.parquet(fold_path+'/'+fold+'/labels')
            end = info['protocol'][fold+'_end']
            if labels.filter((F.col('target_time') < F.lit(start).cast('timestamp')) | (F.col('target_time') >= F.lit(end).cast('timestamp'))).limit(1).count():
                raise ValueError('Target outside selected window')
            checks.append(dict(fold=fold,history_bins_match_eda=True,query_inputs_separate=True,counts=info['folds'][fold]))
        archive = Path(info['archive']['path'])
        if gold.file_hash(archive) != info['archive']['sha256']:
            raise ValueError('ZIP hash mismatch')
        with zipfile.ZipFile(archive) as bundle:
            if not all(n in ('export_manifest.json','README.txt') or (n.startswith(('texts/','pilot/')) and n.endswith('.parquet')) for n in bundle.namelist()):
                raise ValueError('Unexpected data in Kaggle package')
            export = json.loads(bundle.read('export_manifest.json'))
            if export['status'] != 'complete' or export['gold_path'] != info['final_path']:
                raise ValueError('Wrong export manifest')
        import pyarrow.parquet as pq
        package = directory/'kaggle_texts'
        for entry in export['files'] + export['pilot_files']:
            path = (package/entry['path']).resolve()
            if not path.is_relative_to(package.resolve()) or gold.file_hash(path) != entry['sha256'] or pq.ParquetFile(path).metadata.num_rows != entry['rows']:
                raise ValueError('Text shard checksum/row mismatch')
            if set(pq.ParquetFile(path).schema_arrow.names) != {'text_id','text','kind','characters'}:
                raise ValueError('Unexpected fields in text shard')
        result = dict(status='passed',run_id=run_id,category=info['category'],checks=checks,
            zip_text_only=True,text_shards_verified=len(export['files']),gpu_inference_tested_locally=False)
        local_json(directory/'verification.json',result)
        print(json.dumps(result,ensure_ascii=False,indent=2))
    finally:
        spark.stop()


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id',required=True)
    verify(parser.parse_args().run_id)
