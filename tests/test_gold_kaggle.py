"""Gold training package checks and mocked Kaggle upload; no real Kaggle writes."""
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from processing import gold_kaggle as package


class TrainingPackageChecks(unittest.TestCase):
    def test_package_whitelist_checksums_and_upload(self):
        work = ROOT/'outputs/validation/gold-kaggle-fixture'
        work.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=work, prefix='package-') as temporary:
            root = Path(temporary).resolve()
            self.assertTrue(root.is_relative_to(work.resolve()))
            payload = root/'payload'; payload.mkdir()
            plan = dict(export_version=package.VERSION, bundle_id='fixture', categories={},
                protocol={}, contract=dict(encoder={},history_policy='strictly before',sampler={}))
            parquet_files = []
            for category in package.RUNS:
                directory = payload/category; directory.mkdir()
                export = dict(status='complete', category=category)
                entry = dict(text_export=export, gold_path='fixture/'+category)
                plan['categories'][category] = entry
                tables = {}
                all_tables = {f'{fold}/{name}': fields for fold in ('validation','test') for name,fields in package.TABLES.items()}
                all_tables.update(package.REFS)
                all_tables['history_ratings'] = ('record_hash rating','record_hash')
                for relative,(columns,_) in all_tables.items():
                    path = directory/relative/'part.parquet'; path.parent.mkdir(parents=True)
                    pq.write_table(pa.table({name:['fixture'] for name in columns.split()}),path)
                    tables[relative] = dict(rows=1, schema={}, files=[dict(path=path.relative_to(directory).as_posix(),
                        bytes=path.stat().st_size, sha256=package.sha256(path), rows=1)])
                    parquet_files.append(path)
                package.write_json(directory/'text_export_manifest.json', export)
                package.write_json(directory/'category_manifest.json', dict(status='complete',category=category,
                    bundle_id='fixture',sources={'gold_path':entry['gold_path']},tables=tables))
            package.write_json(payload/'export_plan.json',plan)
            result,_ = package.finalize(payload)
            self.assertEqual(result['categories'],list(package.RUNS))
            (payload/'extra_raw_text.txt').write_text('fixture text must not be uploaded')
            uploaded = []
            def upload(handle, folder, **kwargs):
                paths=[p.relative_to(folder).as_posix() for p in Path(folder).rglob('*') if p.is_file()]
                self.assertNotIn('extra_raw_text.txt',paths)
                self.assertIn('dataset_manifest.json',paths)
                self.assertEqual(handle,'fixture/gold-training')
                package.verify_package(folder)
                uploaded.append(paths)
            fake = SimpleNamespace(dataset_upload=upload)
            with patch.dict(sys.modules, {'kagglehub':fake}), patch.object(package,'configure_auth') as auth, patch.object(package,'remote_visibility',return_value='new_private') as visibility:
                package.upload(payload,'fixture/gold-training')
                visibility.assert_not_called()  # Dry run never authenticates/writes.
                auth.assert_not_called()
                self.assertEqual(uploaded,[])
                package.upload(payload,'fixture/gold-training',execute=True)
                self.assertEqual(len(uploaded),1)
                receipt=package.read_json(root/'payload_upload_receipt.json')
                self.assertEqual(receipt['status'],'submitted_processing')
                with self.assertRaises(FileExistsError):
                    package.upload(payload,'fixture/gold-training',execute=True)
            path=parquet_files[0]
            path.write_bytes(path.read_bytes()+b'corrupt')
            with self.assertRaisesRegex(ValueError,'checksum'):
                package.verify_package(payload)
            with self.assertRaises(ValueError):
                package.inside(payload,'../escape')

    def test_dotenv_legacy_auth_without_secret_output(self):
        import io
        from contextlib import redirect_stdout
        work=ROOT/'outputs/validation/gold-kaggle-fixture'
        work.mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(dir=work,prefix='auth-') as temporary:
            path=Path(temporary)/'.env'
            path.write_text('KAGGLE_USERNAME="fixture-user"\nKAGGLE_KEY="fixture-key-$literal" # comment\n',encoding='utf-8-sig')
            calls=[]
            tokens=[]
            fake_config=SimpleNamespace(set_kaggle_credentials=lambda user,key:calls.append((user,key)),
                set_kaggle_api_token=lambda token:tokens.append(token))
            fake_hub=SimpleNamespace(login=lambda:self.fail('Interactive login used instead of .env'))
            output=io.StringIO()
            with patch.dict(sys.modules,{'kagglehub.config':fake_config,'kagglehub':fake_hub}),redirect_stdout(output):
                self.assertEqual(package.configure_auth(login=True,env_path=path),'dotenv_legacy')
                self.assertEqual(calls,[('fixture-user','fixture-key-$literal')])
                self.assertNotIn('fixture-user',output.getvalue())
                self.assertNotIn('fixture-key',output.getvalue())
                path.write_text('KAGGLE_USERNAME=fixture-user\nKAGGLE_KEY=KGAT_fixture-token\n')
                self.assertEqual(package.configure_auth(login=True,env_path=path),'dotenv_api_token')
                self.assertEqual(tokens,['KGAT_fixture-token'])
                self.assertEqual(len(calls),1)
                self.assertNotIn('KGAT_fixture-token',output.getvalue())
                path.write_text('KAGGLE_API_TOKEN=KGAT_explicit-token\nKAGGLE_KEY=KGAT_other\n')
                self.assertEqual(package.configure_auth(env_path=path),'dotenv_api_token')
                self.assertEqual(tokens[-1],'KGAT_explicit-token')
                path.write_text('KAGGLE_USERNAME=fixture-user\n')
                with self.assertRaisesRegex(ValueError,'both KAGGLE_USERNAME and KAGGLE_KEY'):
                    package.configure_auth(env_path=path)

    def test_private_guard(self):
        from requests.exceptions import HTTPError
        class BackendError(Exception):
            error_code=404
        client=SimpleNamespace(datasets=SimpleNamespace(dataset_api_client=SimpleNamespace(
            get_dataset_metadata=lambda request: SimpleNamespace(info=SimpleNamespace(is_private=False)))))
        class Context:
            def __enter__(self): return client
            def __exit__(self,*args): pass
        modules={'kagglehub.clients':SimpleNamespace(BackendError=BackendError,build_kaggle_client=Context),
            'kagglesdk.datasets.types.dataset_api_service':SimpleNamespace(ApiGetDatasetMetadataRequest=SimpleNamespace)}
        with patch.dict(sys.modules,modules):
            with self.assertRaisesRegex(ValueError,'private'):
                package.remote_visibility('fixture/gold-training')
            client.datasets.dataset_api_client.get_dataset_metadata=lambda request: SimpleNamespace(info=SimpleNamespace(is_private=True))
            self.assertEqual(package.remote_visibility('fixture/gold-training'),'existing_private')
            def missing(request): raise BackendError()
            client.datasets.dataset_api_client.get_dataset_metadata=missing
            self.assertEqual(package.remote_visibility('fixture/gold-training'),'new_private')
            BackendError.error_code=403
            with self.assertRaisesRegex(RuntimeError,'Kaggle HTTP 403'):
                package.remote_visibility('fixture/gold-training')
            def http_error(request):
                raise HTTPError(response=SimpleNamespace(status_code=403))
            client.datasets.dataset_api_client.get_dataset_metadata=http_error
            with self.assertRaisesRegex(RuntimeError,'Kaggle HTTP 403'):
                package.remote_visibility('fixture/gold-training')
            def not_found(request):
                raise HTTPError(response=SimpleNamespace(status_code=404))
            client.datasets.dataset_api_client.get_dataset_metadata=not_found
            self.assertEqual(package.remote_visibility('fixture/gold-training'),'new_private')

    @unittest.skipUnless(os.environ.get('GOLD_KAGGLE_SPARK_TEST')=='1','Spark fixture opt-in')
    def test_spark_export_with_temporal_rating_lookup(self):
        from processing.gold import build_fold
        from processing.silver import create_spark
        work=ROOT/'outputs/validation/gold-kaggle-fixture'
        work.mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(dir=work,prefix='spark-') as temporary:
            root=Path(temporary).resolve()
            self.assertTrue(root.is_relative_to(work.resolve()))
            spark=create_spark(Path('/tmp/gold-kaggle-fixture')/root.name,2,2)
            try:
                reviews=spark.createDataFrame([
                    ('r0','u','a',datetime(2020,1,1),5.0),
                    ('r1','u','b',datetime(2021,2,1),None),
                    ('r2','cold','a',datetime(2022,2,1),1.0)],
                    'record_hash string,user_id string,parent_asin string,event_time timestamp,rating double')
                silver=root/'silver'; reviews.write.parquet((silver/'reviews_clean').as_uri())
                folds=root/'folds'; tables={}
                protocol=dict(validation_start='2021-01-01',validation_end='2021-07-01',test_start='2022-01-01',test_end='2022-07-01')
                for fold in ('validation','test'):
                    def emit(name,frame,keys):
                        relative=fold+'/'+name
                        tables[relative]=dict(rows=frame.count())
                        frame.write.parquet((folds/relative).as_uri())
                    build_fold(reviews.select('record_hash','user_id','parent_asin','event_time'),
                        'All_Beauty',fold,protocol[fold+'_start'],protocol[fold+'_end'],2,emit,lambda _:None)
                refs=root/'refs'
                spark.createDataFrame([('a','item-a','text-a',True),('b','item-b','text-b',True)],
                    'parent_asin string,source_record_hash string,text_id string,has_text boolean').write.parquet((refs/'item_text_refs').as_uri())
                spark.createDataFrame([('r0','u','a',datetime(2020,1,1),'review-a',True),('r1','u','b',datetime(2021,2,1),'review-b',True)],
                    'record_hash string,user_id string,parent_asin string,event_time timestamp,text_id string,has_text boolean').write.parquet((refs/'review_text_refs').as_uri())
                output=root/'export'
                result=package.export_tables(spark,folds.as_uri(),refs.as_uri(),silver.as_uri(),output,
                    {'tables':tables},{'tables':{'/item_text_refs':{'rows':2},'/review_text_refs':{'rows':2}}},protocol,0)
                self.assertEqual(result['history_ratings']['rows'],2)
                ratings=spark.read.parquet((output/'history_ratings').as_uri()).collect()
                self.assertEqual({r.record_hash for r in ratings},{'r0','r1'})
                self.assertTrue(any(r.rating is None for r in ratings))
                examples=spark.read.parquet((output/'test/train_examples').as_uri())
                self.assertNotIn('event_count',examples.columns)
                self.assertNotIn('rating',examples.columns)
                self.assertEqual(result['validation/history_events']['rows'],1)
                self.assertEqual(result['test/history_events']['rows'],2)
            finally:
                spark.stop()


if __name__ == '__main__':
    unittest.main()
