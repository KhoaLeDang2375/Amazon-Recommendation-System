"""Execute the reader notebook against real, small Parquet/checkpoint fixtures."""
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK = ROOT/'notebooks/06_download_read_embeddings.ipynb'


class LocalReaderChecks(unittest.TestCase):
    def test_reader_and_corruption(self):
        notebook = json.loads(NOTEBOOK.read_text(encoding='utf-8'))
        namespace = dict(Path=Path, json=json)
        for cell in notebook['cells']:
            if cell['cell_type'] == 'code':
                source = ''.join(cell['source'])
                if not source.startswith('%pip'):
                    compile(source, 'reader-notebook', 'exec')
                if 'embedding-reader-functions' in cell['metadata'].get('tags', []):
                    exec(source, namespace)
        validate = namespace['validate_bundle']
        expected = json.loads((ROOT/'processing/gold_contract.json').read_text())['encoder']
        fixture_root = ROOT/'outputs/validation/local-embedding-notebook'
        fixture_root.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=fixture_root, prefix='reader-') as directory:
            root = Path(directory).resolve()
            self.assertTrue(root.is_relative_to(fixture_root.resolve()))
            bundle = root/'All_Beauty_fixture'; (bundle/'texts').mkdir(parents=True)
            path = bundle/'texts/part.parquet'
            ids = [hashlib.sha256(x.encode()).hexdigest() for x in ('a','b','c')]
            vectors = np.zeros((3, 384), dtype='float32'); vectors[:, 0] = 1
            table = pa.table(dict(text_id=pa.array(ids),
                embedding=pa.FixedSizeListArray.from_arrays(pa.array(vectors.ravel()), 384),
                encoded_tokens=pa.array([4,5,6],type=pa.int64()), chunks=pa.array([1,1,1],type=pa.int32())))
            pq.write_table(table, path)
            signature = dict(expected, cleaning_version='gold_text_nfc_html_whitespace_v1')
            state = dict(status='complete', signature=signature, input_sha256='source', rows=3,
                encoded_tokens=15, chunks=3,
                text_ids_sha256=hashlib.sha256(''.join(x+'\n' for x in ids).encode()).hexdigest(),
                output_sha256=namespace['file_sha256'](path), seconds=1)
            sidecar = path.with_suffix('.json'); sidecar.write_text(json.dumps(state))
            manifest = dict(status='complete', signature=signature, shards=[dict(path='texts/part.parquet',**state)],
                texts=3,completed_texts=3,total_shards=1,gold_path='hdfs://namenode:8020/project/processed/gold/all_beauty/v1/run_fixture/protocol')
            manifest_path = bundle/'embedding_manifest.json'
            manifest_path.write_text(json.dumps(manifest))
            _, report, dataset = validate(bundle, expected)
            self.assertEqual(report['rows'], 3)
            self.assertTrue(report['complete_for_next_stage'])
            self.assertEqual(dataset.head(2)['text_id'].to_pylist(), ids[:2])
            matrix = namespace['vectors_from_batch'](next(dataset.to_batches(batch_size=2)))
            self.assertEqual(matrix.shape,(2,384))
            # Execute download cell in offline mode, selection, report, preview and batch cells.
            namespace.update(REPO_ROOT=ROOT, REPORT_ROOT=root/'reports', CATEGORY='All_Beauty',
                LOCAL_DATASET_DIR=str(root), DATASET_HANDLE='fixture/private-data', DATASET_VERSION=None,
                BUNDLE_NAME=None, AUTHENTICATE=False, ALLOW_PARTIAL=False, FULL_VECTOR_CHECK=True,
                display=lambda value: None)
            for cell in notebook['cells']:
                source=''.join(cell['source'])
                if cell['cell_type']=='code' and source.startswith(('if LOCAL_DATASET_DIR', 'manifest_paths =', 'import pandas as pd', 'for batch in embedding_dataset')):
                    exec(source,namespace)
            self.assertTrue(namespace['validation_report']['complete_for_next_stage'])
            saved=json.loads(namespace['report_path'].read_text(encoding='utf-8'))
            self.assertEqual(saved['rows'],3)
            manifest['status']='partial'; manifest['texts']=6; manifest['total_shards']=2
            manifest_path.write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError,'status=partial'):
                validate(bundle, expected)
            _, partial, _ = validate(bundle, expected, allow_partial=True)
            self.assertFalse(partial['complete_for_next_stage'])
            with self.assertRaises(ValueError):
                namespace['shard_path'](bundle,'../escape.parquet')
            with self.assertRaises(ValueError):
                namespace['shard_path'](bundle,'_pilot/a.parquet')
            with self.assertRaisesRegex(ValueError,'dimension'):
                validate(bundle,dict(expected,dimension=2),allow_partial=True)
            path.write_bytes(path.read_bytes()+b'corrupt')
            with self.assertRaisesRegex(ValueError,'SHA256'):
                validate(bundle,expected,allow_partial=True)


if __name__ == '__main__':
    unittest.main()
