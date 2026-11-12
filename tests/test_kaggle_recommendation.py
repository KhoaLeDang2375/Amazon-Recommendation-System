"""Real Parquet/CSR fixtures: leakage, ranking, metrics, embeddings and notebook sync."""
import hashlib
import json
import os
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'notebooks'))
import kaggle_recommendation as rec


def fixture(root, future=False, missing_embedding=False):
    gold=root/'gold'; gold.mkdir()
    category=gold/'All_Beauty'; category.mkdir()
    tables={}; folds={}
    def write(name,frame):
        path=category/name/'part.parquet'; path.parent.mkdir(parents=True)
        table=pa.Table.from_pandas(frame,preserve_index=False)
        pq.write_table(table,path)
        tables[name]=dict(rows=len(frame),files=[dict(path=path.relative_to(category).as_posix(),rows=len(frame),bytes=path.stat().st_size,sha256=rec.sha256(path))])
    for fold,(start,end) in rec.BOUNDS.items():
        items=pd.DataFrame(dict(parent_asin=['a','b','c','d'],item_idx=np.arange(4,dtype=np.int32)))
        write(fold+'/items',items);write(fold+'/candidates',items)
        write(fold+'/users',pd.DataFrame(dict(user_id=['u0','u1','u2'],user_idx=np.arange(3,dtype=np.int32))))
        write(fold+'/train_pairs',pd.DataFrame(dict(user_idx=[0,0,1,1,2],item_idx=[0,1,1,2,3],strength=[1.]*5,
            first_pair_time=pd.to_datetime(['2021-01-01' if future else '2020-01-01']*5))))
        write(fold+'/queries',pd.DataFrame(dict(query_id=['q0','q1','q2','q3'],user_idx=[0.,1.,np.nan,2.],
            prediction_time=pd.to_datetime([start]*4),history_items=[2,2,0,1],unknown_user=[False,False,True,False])))
        write(fold+'/cohorts',pd.DataFrame(dict(query_id=['q0','q1','q2','q3'],warm_full=[True,True,False,False],
            target_count=[1,2,1,2],targets_in=[1,2,1,1],repeat_targets=[1,2,0,1],history_items=[2,2,0,1])))
        date=str(int(start[:4]))+'-02-01'
        write(fold+'/labels',pd.DataFrame(dict(query_id=['q0','q1','q1','q2','q3','q3'],parent_asin=['a','b','c','c','d','outside'],
            item_idx=[0.,1.,2.,2.,3.,np.nan],target_time=pd.to_datetime([date]*6),in_candidates=[True]*5+[False])))
        folds[fold]=dict(train_pairs=5,queries=4,warm_full_queries=2)
    encoder=rec.read_json(ROOT/'processing/gold_contract.json')['encoder']
    cleaning='gold_text_nfc_html_whitespace_v1'
    ids=[hashlib.sha256((cleaning+chr(0)+x).encode()).hexdigest() for x in ('title a','title b','title c')]
    write('item_text_refs',pd.DataFrame(dict(parent_asin=['a','b','c','d'],text_id=ids+[None],has_text=[True,True,True,False])))
    export=dict(status='complete',category='All_Beauty',gold_run='fixture',texts=3,gold_path='fixture/gold')
    rec.save_json(category/'text_export_manifest.json',export)
    rec.save_json(category/'category_manifest.json',dict(status='complete',category='All_Beauty',tables=tables,folds=folds,
        sources=dict(text_export_sha256=rec.sha256(category/'text_export_manifest.json'))))
    protocol=dict(status='locked',protocol_id=rec.PROTOCOL,timezone='UTC')
    for fold,(start,end) in rec.BOUNDS.items(): protocol.update({fold+'_start':start,fold+'_end':end})
    rec.save_json(gold/'training_contract.json',dict(protocol=protocol,encoder=encoder))
    rec.save_json(gold/'export_plan.json',{})
    (gold/'README.md').write_text('fixture',encoding='utf-8')
    rec.save_json(gold/'dataset_manifest.json',dict(status='complete',export_version='gold_training_export_v1',
        categories={name:rec.sha256(category/'category_manifest.json') if name=='All_Beauty' else 'not-loaded' for name in rec.CATEGORIES},
        contract_sha256=rec.sha256(gold/'training_contract.json'),plan_sha256=rec.sha256(gold/'export_plan.json'),readme_sha256=rec.sha256(gold/'README.md')))
    bundle=root/'embeddings'/'All_Beauty_fixture'
    (bundle/'texts').mkdir(parents=True)
    vectors=np.zeros((3,384),np.float32);vectors[0,0]=1;vectors[1,1]=1;vectors[2,0]=-1
    if missing_embedding: vectors=vectors[:2]
    path=bundle/'texts/part.parquet'
    pq.write_table(pa.table(dict(text_id=ids[:len(vectors)],embedding=pa.FixedSizeListArray.from_arrays(pa.array(vectors.ravel()),384))),path)
    signature=dict(encoder,cleaning_version=cleaning,export_manifest_sha256=rec.sha256(category/'text_export_manifest.json'))
    state=dict(status='complete',signature=signature,rows=len(vectors),output_sha256=rec.sha256(path))
    rec.save_json(path.with_suffix('.json'),state)
    rec.save_json(bundle/'embedding_manifest.json',dict(status='complete',signature=signature,texts=len(vectors),total_shards=1,
        shards=[dict(path='texts/part.parquet',**state)]))
    return gold,bundle


def configuration(root,gold,bundle,algorithm='content'):
    return dict(algorithm=algorithm,category='All_Beauty',phase='validation',gold_root=str(gold),embedding_root=str(bundle),
        embedding_bundle=None,als_backend='cpu',score_devices=['cpu'],seed=42,cpu_threads=1,query_batch=2,item_block=2,
        score_shard_queries=2,bootstrap_samples=20,min_free_gib=0,output_root=str(root/'results'),resume_root=None)


class RecommendationChecks(unittest.TestCase):
    def workspace(self):
        work=ROOT/'outputs/validation/recommendation-fixtures';work.mkdir(parents=True,exist_ok=True)
        temporary=tempfile.TemporaryDirectory(dir=work,prefix='case-')
        self.addCleanup(temporary.cleanup)
        path=Path(temporary.name).resolve();self.assertTrue(path.is_relative_to(work.resolve()))
        return path

    def test_metrics_profiles_ties_and_checkpoint(self):
        root=self.workspace();gold_path,bundle=fixture(root)
        gold=rec.Gold(gold_path,'All_Beauty');fold=gold.fold('validation')
        predictions=np.full((4,20),-1,np.int32)
        predictions[0,:2]=[1,0];predictions[1,:2]=[1,2];predictions[3,0]=3
        metrics=rec.query_metrics(fold,predictions)
        np.testing.assert_allclose(metrics['recall@10'],[1,1,0,.5])
        np.testing.assert_allclose(metrics['ndcg@10'],[1/np.log2(3),1,0,1/(1+1/np.log2(3))])
        repeated=predictions.copy();repeated[0,1]=1
        with self.assertRaisesRegex(ValueError,'Duplicate'):rec.query_metrics(fold,repeated)
        items,valid,info=rec.load_item_vectors(gold,fold,bundle)
        self.assertEqual(info['items_with_vectors'],3)
        positions,profiles,_=rec.content_profiles(fold.binary,fold.user_indices,items,valid)
        np.testing.assert_array_equal(positions,[0,1])
        np.testing.assert_allclose(profiles[:,:2],[[1/np.sqrt(2),1/np.sqrt(2)],[-1/np.sqrt(2),1/np.sqrt(2)]],atol=1e-6)
        cancel=rec.sparse.csr_matrix(np.array([[1,0,1,0]],np.float32))
        self.assertEqual(len(rec.content_profiles(cancel,np.array([0]),items,valid)[0]),0)
        expected=None
        for block in (1,2,4):
            ids,_=rec.rank_exact(profiles,items,valid,item_block=block,query_batch=1)
            if expected is None:expected=ids
            np.testing.assert_array_equal(ids,expected)
        self.assertEqual(expected[0,:4].tolist(),[0,1,2,-1])  # Keep seen items; missing content never outranks negative cosine.
        config=configuration(root,gold_path,bundle)
        first=rec.rank_checkpoints(positions,profiles,items,valid,root/'scores',{},config)
        np.testing.assert_array_equal(rec.rank_checkpoints(positions,profiles,items,valid,root/'scores',{},config),first)
        checkpoint=next((root/'scores').glob('part-*.npz'));checkpoint.write_bytes(checkpoint.read_bytes()+b'bad')
        with self.assertRaisesRegex(ValueError,'checkpoint'):rec.rank_checkpoints(positions,profiles,items,valid,root/'scores',{},config)
        try:
            import torch
        except ImportError: return
        scores=torch.tensor([[1.,1.,1.,0.],[2.,-float('inf'),2.,2.]])
        ids,_=rec.torch_topk(scores,5,2)
        np.testing.assert_array_equal(ids,[[5,6],[5,7]])

    def test_content_validation_test_and_resume(self):
        root=self.workspace();gold,bundle=fixture(root);config=configuration(root,gold,bundle)
        config['phase']='test'
        with self.assertRaisesRegex(ValueError,'validation first'):rec.run(config)
        config['phase']='validation';run=rec.run(config)
        with mock.patch.object(rec.Gold,'fold',side_effect=AssertionError('Complete phase must not load/train again')):
            self.assertEqual(rec.run(config),run)
        self.assertEqual(rec.read_json(run/'run.json')['status'],'complete')
        metrics=pd.read_csv(run/'validation/evaluation/metrics.csv')
        self.assertEqual(int(metrics.query("model=='content_native' and cohort=='all_queries'").iloc[0].queries),4)
        self.assertEqual(metrics.query("model=='content_native' and cohort=='all_queries'").iloc[0].native_coverage,.5)
        # Restore the validation Output into a new writable root, then refit/test.
        config.update(phase='test',resume_root=str(root/'results'),output_root=str(root/'new-session'))
        restored=rec.run(config)
        self.assertEqual(restored.name,run.name)
        self.assertTrue((restored/'test/phase_complete.json').is_file())
        comparison=rec.compare_results(restored/'test/evaluation',restored/'test/evaluation',root/'paired.json',20)
        self.assertEqual(comparison['all_queries']['mean_delta'],0)

    def test_suite_upload_snapshot_resume_and_failure(self):
        root=self.workspace();gold,bundle=fixture(root)
        config=configuration(root,gold,bundle)
        config.update(categories=['All_Beauty'],publish_results=True,auto_resume=True,
            result_datasets={'All_Beauty':'fixture/content-results-all-beauty'})
        uploaded=[]
        def upload(handle,directory,version_notes):
            staging=Path(directory)
            metadata=rec.read_json(staging/'result_bundle.json')
            self.assertEqual(handle,config['result_datasets']['All_Beauty'])
            self.assertEqual(metadata['phases'],['validation','test'])
            self.assertNotIn('KAGGLE_API_TOKEN',json.dumps(metadata))
            saved=staging/'recommendation_results'/metadata['run']
            for name,entry in metadata['files'].items(): self.assertEqual(rec.sha256(saved/name),entry['sha256'])
            target=root/'remote';target.mkdir(exist_ok=True)
            import shutil
            shutil.copytree(staging,target,dirs_exist_ok=True)
            uploaded.append(metadata)
        hub=SimpleNamespace(dataset_upload=mock.Mock(side_effect=upload),
            dataset_download=mock.Mock(return_value=str(root/'remote')))
        with mock.patch.dict(sys.modules,kagglehub=hub),mock.patch.object(rec,'configure_kaggle_auth'),mock.patch.object(rec,'result_dataset_private',return_value=False):
            results=rec.run_suite(config)
            self.assertEqual(results[0]['upload']['status'],'submitted_processing')
            run=Path(results[0]['run_root'])
            self.assertEqual(rec.read_json(run/'run.json')['phase'],'test')
            rec.run_suite(config)
            self.assertEqual(hub.dataset_upload.call_count,1)
        config['output_root']=str(root/'new-session')
        with mock.patch.dict(sys.modules,kagglehub=hub),mock.patch.object(rec,'configure_kaggle_auth'),mock.patch.object(rec,'result_dataset_private',return_value=True):
            with mock.patch.object(rec.Gold,'fold',side_effect=AssertionError('Remote complete phases must be reused')):
                restored=rec.run_suite(config)
            self.assertTrue(restored[0]['upload']['reused_remote'])
            self.assertEqual(hub.dataset_upload.call_count,1)
        # Changed payload creates a version; failed API must leave results and block blind retry.
        run=Path(restored[0]['run_root'])
        (run/'extra.csv').write_text('score\n1\n')
        hub.dataset_upload.side_effect=RuntimeError('network interrupted')
        with mock.patch.dict(sys.modules,kagglehub=hub),mock.patch.object(rec,'result_dataset_private',return_value=True):
            with self.assertRaisesRegex(RuntimeError,'local results preserved'):rec.publish_results(run,'fixture/content-results-all-beauty',0)
            calls=hub.dataset_upload.call_count
            with self.assertRaisesRegex(RuntimeError,'uncertain'):rec.publish_results(run,'fixture/content-results-all-beauty',0)
            self.assertEqual(hub.dataset_upload.call_count,calls)
        self.assertTrue((run/'test/evaluation/metrics.csv').is_file())

    def test_suite_category_order_and_validation_failure_gate(self):
        config=dict(categories=list(rec.CATEGORIES),algorithm='als',publish_results=False,
            output_root=str(self.workspace()),gold_root='fixture')
        calls=[]
        def run(current):
            calls.append((current['category'],current['phase']))
            return Path(config['output_root'])
        with mock.patch.object(rec,'Gold'),mock.patch.object(rec,'run',side_effect=run):
            rec.run_suite(config)
        self.assertEqual(calls,[(c,p) for c in rec.CATEGORIES for p in ('validation','test')])
        with mock.patch.object(rec,'Gold'),mock.patch.object(rec,'run',side_effect=RuntimeError('validation failed')) as execute:
            with self.assertRaisesRegex(RuntimeError,'validation failed'):rec.run_suite(config)
            self.assertEqual(execute.call_count,1)

    def test_result_privacy_errors_do_not_create_dataset(self):
        from kagglehub.clients import BackendError
        client=mock.MagicMock()
        context=mock.MagicMock();context.__enter__.return_value=client
        endpoint=client.datasets.dataset_api_client.get_dataset_metadata
        with mock.patch('kagglehub.clients.build_kaggle_client',return_value=context):
            endpoint.side_effect=BackendError('missing',error_code=404)
            self.assertFalse(rec.result_dataset_private('fixture/results'))
            for status in (401,403,500):
                endpoint.side_effect=BackendError('rejected',error_code=status)
                client.datasets.dataset_api_client.list_datasets.side_effect=BackendError('rejected',error_code=status)
                with self.assertRaisesRegex(RuntimeError,str(status)):rec.result_dataset_private('fixture/results')
            endpoint.side_effect=None
            endpoint.return_value.info.is_private=False
            with self.assertRaisesRegex(ValueError,'private'):rec.result_dataset_private('fixture/results')

    def test_metadata_403_resolved_with_authenticated_owned_inventory(self):
        from requests import HTTPError,Response
        client=mock.MagicMock()
        context=mock.MagicMock();context.__enter__.return_value=client
        response=Response();response.status_code=403
        client.datasets.dataset_api_client.get_dataset_metadata.side_effect=HTTPError('Forbidden',response=response)
        listing=client.datasets.dataset_api_client.list_datasets
        gold=SimpleNamespace(ref='fixture/gold-training-v2',is_private=True)
        empty=SimpleNamespace(datasets=[],next_page_token='')
        first=SimpleNamespace(datasets=[gold],next_page_token='')
        with mock.patch('kagglehub.clients.build_kaggle_client',return_value=context):
            listing.side_effect=[first,empty]
            self.assertFalse(rec.result_dataset_private('fixture/results'))
            self.assertEqual(listing.call_args_list[-1].args[0].page,2)
            for private in (True,False):
                target=SimpleNamespace(ref='fixture/results',is_private=private)
                listing.side_effect=[SimpleNamespace(datasets=[gold],next_page_token='next'),
                    SimpleNamespace(datasets=[target],next_page_token='')]
                if private:self.assertTrue(rec.result_dataset_private('fixture/results'))
                else:
                    with self.assertRaisesRegex(ValueError,'private'):rec.result_dataset_private('fixture/results')
            listing.side_effect=[SimpleNamespace(datasets=[SimpleNamespace(ref='other/gold',is_private=True)],next_page_token=''),empty]
            with self.assertRaisesRegex(RuntimeError,'RESULT_OWNER'):rec.result_dataset_private('fixture/results')
            listing.side_effect=[empty]
            with self.assertRaisesRegex(RuntimeError,'403'):rec.result_dataset_private('fixture/results')

    def test_explicit_auth_overrides_native_session_credentials(self):
        import kagglehub.config  # Resolve SDK paths before isolating the test environment.
        with mock.patch.dict(sys.modules,kaggle_secrets=None),mock.patch.dict(os.environ,{},clear=True),mock.patch('kagglehub.config.set_kaggle_api_token') as token,mock.patch('kagglehub.config.set_kaggle_credentials') as legacy:
            os.environ['KAGGLE_API_TOKEN']=' fixture-token '
            rec.configure_kaggle_auth()
            token.assert_called_once_with('fixture-token');legacy.assert_not_called()
            del os.environ['KAGGLE_API_TOKEN']
            os.environ.update(KAGGLE_USERNAME='fixture',KAGGLE_KEY='legacy-key')
            rec.configure_kaggle_auth()
            legacy.assert_called_once_with('fixture','legacy-key')

    def test_future_history_and_missing_vectors_fail(self):
        root=self.workspace();bad=root/'future';bad.mkdir();gold,bundle=fixture(bad,future=True)
        with self.assertRaisesRegex(ValueError,'cutoff'):rec.Gold(gold,'All_Beauty').fold('validation')
        bad=root/'missing';bad.mkdir();gold,bundle=fixture(bad,missing_embedding=True)
        source=rec.Gold(gold,'All_Beauty');fold=source.fold('validation')
        with self.assertRaisesRegex(ValueError,'missing'):rec.load_item_vectors(source,fold,bundle)
        data=rec.read_json(bundle/'embedding_manifest.json');data['status']='partial';rec.save_json(bundle/'embedding_manifest.json',data)
        with self.assertRaisesRegex(ValueError,'partial'):rec.discover_embedding(source,bundle)

    def test_notebook_schema_and_exact_embedded_source(self):
        import nbformat
        source=(ROOT/'notebooks/kaggle_recommendation.py').read_text(encoding='utf-8')
        for name in ('07_kaggle_als_full.ipynb','08_kaggle_content_based.ipynb'):
            notebook=nbformat.read(ROOT/'notebooks'/name,as_version=4);nbformat.validate(notebook)
            embedded=[]
            for cell in notebook.cells:
                if cell.cell_type!='code':continue
                self.assertEqual(cell.outputs,[]);self.assertIsNone(cell.execution_count)
                code=cell.source
                if code.startswith('%pip'):continue
                if code.startswith('%%writefile'): code=code.split(chr(10),1)[1];embedded.append(code)
                compile(code,name,'exec')
            self.assertEqual(embedded,[source])

    def test_scoring_gpu_diagnostics_and_explicit_devices(self):
        config=configuration(Path('.'),Path('.'),Path('.'))
        config['score_devices']=['cuda:0','cuda:1']
        torch=mock.MagicMock()
        torch.__version__='fixture'
        torch.version.cuda='fixture-cuda'
        for available,count in ((False,0),(True,1)):
            torch.cuda.is_available.return_value=available
            torch.cuda.device_count.return_value=count
            with self.subTest(available=available,count=count),mock.patch.dict(sys.modules,torch=torch):
                with self.assertRaisesRegex(RuntimeError,f'visible GPUs={count}'):
                    rec.preflight(config)
                self.assertEqual(config['score_devices'],['cuda:0','cuda:1'])
        config['score_devices']=['cuda:0']
        with mock.patch.dict(sys.modules,torch=torch):
            self.assertEqual(len(rec.preflight(config)['gpus']),1)
        config['score_devices']=['cpu']
        with mock.patch.dict(sys.modules,torch=None):
            self.assertEqual(rec.preflight(config)['gpus'],[])

    def test_environment_setup_branches_and_failure_gate(self):
        for filename in ('07_kaggle_als_full.ipynb','08_kaggle_content_based.ipynb'):
            notebook=json.loads((ROOT/'notebooks'/filename).read_text(encoding='utf-8'))
            cells=[''.join(c['source']) for c in notebook['cells'] if c['cell_type']=='code']
            for version in ((3,11,0),(3,13,0)):
                namespace={}
                with mock.patch.object(sys,'version_info',version),mock.patch('subprocess.run') as run:
                    exec(cells[0],namespace);exec(cells[1],namespace)
                packages=run.call_args_list[0].args[0]
                self.assertIn('--only-binary=:all:',packages)
                self.assertIn('numpy>=2.1,<3' if version[1]==13 else 'numpy==1.26.4',packages)
                self.assertEqual(namespace['DEFAULT_ALS_BACKEND'],'cpu' if version[1]==13 else 'gpu')
                self.assertTrue(namespace['ENVIRONMENT_READY'])
                if filename.startswith('07'):
                    self.assertIn('implicit==0.7.3' if version[1]==13 else 'implicit==0.7.2',packages)
                else:self.assertFalse(any(p.startswith('implicit') for p in packages))
            with mock.patch('subprocess.run',side_effect=RuntimeError('pip failed')):
                namespace={};exec(cells[0],namespace)
                with self.assertRaisesRegex(RuntimeError,'pip failed'):exec(cells[1],namespace)
            self.assertFalse(namespace['ENVIRONMENT_READY'])
            train=next(c for c in cells if 'config_path.write_text' in c)
            with self.assertRaisesRegex(RuntimeError,'dependency'):exec(train,namespace)

    @unittest.skipUnless(os.environ.get('RECOMMENDATION_ALS_TEST')=='1','Actual implicit CPU fit opt-in')
    def test_actual_als_full_pipeline(self):
        root=self.workspace();gold,bundle=fixture(root);config=configuration(root,gold,bundle,'als')
        run=rec.run(config)
        best=rec.read_json(run/'best_config.json')
        self.assertEqual(best['status'],'locked')
        self.assertEqual(len(pd.read_csv(run/'trials.csv')),5)
        best_dir=run/'trials'/best['best']['trial']
        before=rec.sha256(best_dir/'user_factors.npy')
        rec.run(config)  # Reuse completed trials/model/ranking, without fitting again.
        self.assertEqual(before,rec.sha256(best_dir/'user_factors.npy'))
        config['phase']='test';rec.run(config)
        self.assertTrue((run/'test/model/fit_manifest.json').is_file())
        self.assertTrue((run/'test/evaluation/als_native_top20.parquet').is_file())


if __name__=='__main__':unittest.main()
