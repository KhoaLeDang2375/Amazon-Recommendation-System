"""Real Parquet IO, key alignment and resume checks without downloading a model."""
import json
import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'notebooks'))
import kaggle_text_embeddings as emb


class EmbeddingIOChecks(unittest.TestCase):
    def test_shard_resume_corruption_and_vector_validation(self):
        import hashlib
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); source=root/'input'; source.mkdir()
            cleaning='fixture_v1'
            texts=['Not bad','Keep 10 mm','Longer review']
            ids=[hashlib.sha256((cleaning+'\0'+text).encode()).hexdigest() for text in texts]
            path=source/'part.parquet'
            pq.write_table(pa.table({'text_id':ids,'text':texts}),path)
            entry=dict(path='part.parquet',sha256=emb.sha256(path),bytes=path.stat().st_size,rows=3)
            signature=dict(dimension=2,cleaning_version=cleaning)
            calls=[]
            def encode(values):
                calls.append(len(values))
                return np.tile([0.6,0.8],(len(values),1)),[4]*len(values),[1]*len(values)
            output=root/'output'/'part.parquet'
            state=emb.encode_shard(source,entry,output,signature,encode,io_rows=2,min_free_gib=0)
            self.assertEqual(calls,[2,1])
            self.assertEqual(pq.read_table(output)['text_id'].to_pylist(),ids)
            self.assertEqual(state['rows'],3)
            emb.encode_shard(source,entry,output,signature,encode,io_rows=2,min_free_gib=0)
            self.assertEqual(calls,[2,1])
            restored=root/'restored'/'part.parquet'
            emb.encode_shard(source,entry,restored,signature,encode,min_free_gib=0,resume=output)
            self.assertEqual(calls,[2,1])
            task=dict(entry=entry,output=str(output),signature=signature,resume=None)
            self.assertEqual(emb.pending_tasks([task],1,0),[])
            missing=dict(task,output=str(root/'missing'/'part.parquet'))
            self.assertEqual(emb.pending_tasks([task,missing],1,0),[missing])
            with self.assertRaises(ValueError):
                emb.pending_tasks([task],0,0)
            manifest=dict(status='partial',signature=signature,shards=[dict(path='part.parquet',**state)])
            emb.save_json(output.parent/'embedding_manifest.json',manifest)
            snapshot=emb.dataset_snapshot(output.parent,root/'snapshot')
            self.assertEqual(emb.sha256(snapshot/'output'/'part.parquet'),emb.sha256(output))
            self.assertFalse((snapshot/'output'/'_pilot').exists())
            self.assertEqual(json.loads((snapshot/'output'/'embedding_manifest.json').read_text())['status'],'partial')
            with self.assertRaisesRegex(ValueError,'another input/config'):
                emb.encode_shard(source,entry,output,dict(signature,dimension=3),encode,min_free_gib=0)
            with self.assertRaisesRegex(ValueError,'checksum'):
                emb.validate_input(source,dict(entry,sha256='0'*64))
            with self.assertRaises(ValueError):
                emb.child_path(source,'../escape.parquet')
            def invalid(values):
                return np.zeros((len(values),2)),[1]*len(values),[1]*len(values)
            with self.assertRaisesRegex(ValueError,'normalized'):
                emb.encode_shard(source,entry,root/'bad.parquet',signature,invalid,min_free_gib=0)
            self.assertFalse((root/'bad.json').exists())
            np.testing.assert_allclose(emb.normalize_rows(np.array([[3.,4.]],dtype='float32')),[[0.6,0.8]])

    def test_notebook_contains_the_tested_script(self):
        root=Path(__file__).resolve().parents[1]
        notebook=json.loads((root/'notebooks/05_kaggle_text_embeddings.ipynb').read_text(encoding='utf-8'))
        script=(root/'notebooks/kaggle_text_embeddings.py').read_text(encoding='utf-8')
        source=next(''.join(c['source']) for c in notebook['cells'] if c['cell_type']=='code' and ''.join(c['source']).startswith('%%writefile'))
        self.assertEqual(source.split('\n',1)[1],script)
        for cell in notebook['cells']:
            if cell['cell_type']=='code':
                text=''.join(cell['source'])
                if text.startswith('%%writefile'):
                    text=text.split('\n',1)[1]
                elif text.startswith('%pip'):
                    continue
                compile(text,'notebook','exec')


if __name__=='__main__':
    unittest.main()
