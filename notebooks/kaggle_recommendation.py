"""Shared, self-contained Gold loader, exact ranker and ALS/content experiments."""
import argparse
import hashlib
import importlib.metadata
import json
import math
import os
import re
import shutil
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from scipy import sparse

PROTOCOL = 'fixed_origin_first_next_all_reviews_6m_v1'
CATEGORIES = ('All_Beauty', 'Video_Games', 'Industrial_and_Scientific')
BOUNDS = {'validation': ('2021-01-01', '2021-07-01'), 'test': ('2022-01-01', '2022-07-01')}
KS = (10, 20)


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(8 * 1024**2), b''):
            digest.update(block)
    return digest.hexdigest()


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def array_hash(array):
    array = np.asarray(array)
    digest = hashlib.sha256(str((array.shape, array.dtype.str)).encode())
    for start in range(0, len(array), 8192):
        digest.update(np.ascontiguousarray(array[start:start+8192]).tobytes())
    return digest.hexdigest()


def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8-sig'))


def save_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix+'.tmp')
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False)+chr(10), encoding='utf-8')
    temporary.replace(path)


def child(root, relative):
    root = Path(root).resolve()
    path = (root/relative).resolve()
    if path == root or not path.is_relative_to(root):
        raise ValueError('Path escapes artifact root')
    return path


def disk_guard(directory, additional=0, reserve_gib=5):
    if not math.isfinite(reserve_gib) or reserve_gib < 0:
        raise ValueError('Invalid disk reserve')
    if shutil.disk_usage(directory).free < additional + reserve_gib * 1024**3:
        raise RuntimeError('Insufficient output disk; save Output and use another session')


def save_array(path, array):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix+'.tmp')
    with temporary.open('wb') as stream:
        np.save(stream, array, allow_pickle=False)
    temporary.replace(path)
    return sha256(path)


def versions():
    result = {}
    for name in ('numpy','scipy','pandas','pyarrow','torch','implicit'):
        try: result[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError: result[name] = None
    return result


class Monitor:
    """Sample process RSS and device-wide used VRAM, including non-Torch ALS allocations."""
    def __init__(self, devices=()):
        self.devices = devices; self.peak_rss = 0; self.vram = {}; self.stop = threading.Event()

    def sample(self):
        import psutil
        self.peak_rss = max(self.peak_rss, psutil.Process().memory_info().rss)
        if any(str(d).startswith('cuda:') for d in self.devices):
            import torch
            for device in self.devices:
                free, total = torch.cuda.mem_get_info(device)
                self.vram[device] = max(self.vram.get(device, 0), int(total-free))

    def __enter__(self):
        self.started = time.monotonic(); self.sample()
        def poll():
            while not self.stop.wait(1):
                self.sample()
        self.thread = threading.Thread(target=poll, daemon=True); self.thread.start()
        return self

    def __exit__(self, *_):
        self.stop.set(); self.thread.join(); self.sample()
        self.result = dict(seconds=round(time.monotonic()-self.started, 3),
            sampled_peak_rss_bytes=self.peak_rss, sampled_device_used_vram_bytes=self.vram,
            measurement='1s samples; device-wide VRAM includes all allocators, not exclusive process accounting')


class Gold:
    def __init__(self, root, category):
        self.root = Path(root).resolve(); self.category = category; self.checked = set()
        if category not in CATEGORIES: raise ValueError('Unknown category')
        self.manifest = read_json(self.root/'dataset_manifest.json')
        if self.manifest.get('status') != 'complete' or set(self.manifest['categories']) != set(CATEGORIES):
            raise ValueError('Gold Dataset must contain all three complete categories')
        for name, key in (('training_contract.json','contract_sha256'),('export_plan.json','plan_sha256'),('README.md','readme_sha256')):
            if sha256(self.root/name) != self.manifest[key]: raise ValueError('Gold metadata checksum mismatch')
        self.contract = read_json(self.root/'training_contract.json')
        protocol = self.contract['protocol']
        if protocol.get('status') != 'locked' or protocol['protocol_id'] != PROTOCOL or protocol['timezone'] != 'UTC':
            raise ValueError('Wrong Gold protocol/timezone')
        for fold, (start, end) in BOUNDS.items():
            if (protocol[fold+'_start'], protocol[fold+'_end']) != (start, end):
                raise ValueError('Do not change locked temporal split')
        self.directory = self.root/category
        if sha256(self.directory/'category_manifest.json') != self.manifest['categories'][category]:
            raise ValueError('Category manifest checksum mismatch')
        self.info = read_json(self.directory/'category_manifest.json')
        if self.info.get('status') != 'complete' or self.info['category'] != category:
            raise ValueError('Wrong/incomplete category')
        if sha256(self.directory/'text_export_manifest.json') != self.info['sources']['text_export_sha256']:
            raise ValueError('Text export manifest mismatch')
        self.signature = dict(dataset_manifest=sha256(self.root/'dataset_manifest.json'),
            category_manifest=sha256(self.directory/'category_manifest.json'), category=category)

    def files(self, relative):
        spec = self.info['tables'][relative]
        paths = []
        for entry in spec['files']:
            path = child(self.directory, entry['path'])
            if path.parent != child(self.directory, relative) or path.suffix != '.parquet':
                raise ValueError('Unexpected table shard')
            if path not in self.checked:
                if path.stat().st_size != entry['bytes'] or sha256(path) != entry['sha256']:
                    raise ValueError('Gold shard checksum mismatch: '+relative)
                if pq.ParquetFile(path).metadata.num_rows != entry['rows']:
                    raise ValueError('Gold shard row count mismatch')
                self.checked.add(path)
            paths.append(path)
        if not paths or len(set(paths)) != len(paths) or sum(e['rows'] for e in spec['files']) != spec['rows']:
            raise ValueError('Invalid Gold shard list')
        return paths

    def table(self, relative, columns):
        table = pq.read_table(self.files(relative), columns=columns)
        if table.num_rows != self.info['tables'][relative]['rows']: raise ValueError('Gold read count mismatch')
        return table.to_pandas()

    def fold(self, name):
        return Fold(self, name)


class Fold:
    def __init__(self, gold, name):
        if name not in BOUNDS: raise ValueError('Unknown fold')
        self.gold = gold; self.name = name; self.start, self.end = BOUNDS[name]
        load = lambda table, cols: gold.table(name+'/'+table, cols)
        self.items = load('items',['parent_asin','item_idx']).sort_values('item_idx').reset_index(drop=True)
        users = load('users',['user_idx'])
        self.nusers, self.nitems = len(users), len(self.items)
        if not np.array_equal(np.sort(users.user_idx), np.arange(self.nusers)) or not np.array_equal(self.items.item_idx, np.arange(self.nitems)) or self.items.parent_asin.duplicated().any():
            raise ValueError('Mappings must be unique contiguous train-only indices')
        del users
        candidates = load('candidates',['parent_asin','item_idx']).sort_values('item_idx').reset_index(drop=True)
        if not candidates.equals(self.items): raise ValueError('Candidates differ from training item mapping')
        pairs = load('train_pairs',['user_idx','item_idx','strength','first_pair_time'])
        if pairs.isna().any().any() or not (pairs.strength == 1).all() or (pairs.first_pair_time >= pd.Timestamp(self.start)).any():
            raise ValueError('Invalid binary training pairs/cutoff')
        rows = pairs.user_idx.to_numpy(np.int32); cols = pairs.item_idx.to_numpy(np.int32)
        if (rows < 0).any() or (rows >= self.nusers).any() or (cols < 0).any() or (cols >= self.nitems).any():
            raise ValueError('Training index out of range')
        self.binary = sparse.csr_matrix((np.ones(len(pairs),np.float32),(rows,cols)), shape=(self.nusers,self.nitems))
        self.binary.sum_duplicates(); self.binary.sort_indices()
        if self.binary.nnz != len(pairs) or not (self.binary.data == 1).all(): raise ValueError('Duplicate train pairs')
        del pairs, rows, cols
        self.queries = load('queries',['query_id','user_idx','prediction_time','history_items','unknown_user'])
        if self.queries.query_id.duplicated().any() or self.queries.query_id.isna().any() or not (self.queries.prediction_time == pd.Timestamp(self.start)).all():
            raise ValueError('Invalid query keys/cutoff')
        self.user_indices = self.queries.user_idx.fillna(-1).to_numpy(np.int32)
        if (self.user_indices < -1).any() or (self.user_indices >= self.nusers).any(): raise ValueError('Invalid query user index')
        if not np.array_equal(self.user_indices < 0, self.queries.unknown_user.to_numpy(bool)):
            raise ValueError('Cold-user flag mismatch')
        history_counts=np.zeros(len(self.queries),np.int64)
        known=self.user_indices>=0
        history_counts[known]=np.diff(self.binary.indptr)[self.user_indices[known]]
        if not np.array_equal(history_counts,self.queries.history_items.to_numpy()):
            raise ValueError('Query history support differs from training pairs')
        self.nqueries = len(self.queries)
        self.cohorts = load('cohorts',['query_id','warm_full','target_count','targets_in','repeat_targets','history_items'])
        if self.cohorts.query_id.duplicated().any(): raise ValueError('Duplicate cohort query')
        self.cohorts = self.queries[['query_id']].merge(self.cohorts,on='query_id',how='left',validate='one_to_one')
        if self.cohorts.isna().any().any() or not np.array_equal(self.cohorts.history_items,self.queries.history_items): raise ValueError('Missing/mismatched query cohort')
        labels = load('labels',['query_id','parent_asin','item_idx','target_time','in_candidates'])
        if labels[['query_id','parent_asin']].isna().any().any() or labels.duplicated(['query_id','parent_asin']).any():
            raise ValueError('Duplicate/null target key')
        if labels.target_time.isna().any() or labels.in_candidates.isna().any() or ((labels.target_time < pd.Timestamp(self.start)) | (labels.target_time >= pd.Timestamp(self.end))).any():
            raise ValueError('Target outside evaluation window')
        qmap = pd.Series(np.arange(self.nqueries),index=self.queries.query_id)
        imap = pd.Series(np.arange(self.nitems),index=self.items.parent_asin)
        qrows = labels.query_id.map(qmap)
        if qrows.isna().any(): raise ValueError('Label with no query')
        qrows = qrows.to_numpy(np.int32)
        indices = labels.parent_asin.map(imap).fillna(-1).to_numpy(np.int32)
        if not np.array_equal(indices >= 0, labels.in_candidates.to_numpy(bool)) or not np.array_equal(indices,labels.item_idx.fillna(-1).to_numpy(np.int32)):
            raise ValueError('Label candidate mapping mismatch')
        self.target_counts = np.bincount(qrows,minlength=self.nqueries)
        if (self.target_counts == 0).any() or not np.array_equal(self.target_counts,self.cohorts.target_count):
            raise ValueError('Empty/mismatched label population')
        inside = indices >= 0
        self.truth = sparse.csr_matrix((np.ones(inside.sum(),np.float32),(qrows[inside],indices[inside])), shape=(self.nqueries,self.nitems))
        if not np.array_equal(np.asarray(self.truth.sum(axis=1)).ravel(),self.cohorts.targets_in): raise ValueError('Target coverage mismatch')
        self.warm = self.cohorts.warm_full.to_numpy(bool)
        if not np.array_equal(self.warm,(self.user_indices >= 0) & (self.cohorts.targets_in.to_numpy()==self.target_counts)):
            raise ValueError('Warm-full definition changed')
        expected = gold.info['folds'][name]
        if (self.binary.nnz,self.nqueries,int(self.warm.sum())) != (expected['train_pairs'],expected['queries'],expected['warm_full_queries']):
            raise ValueError('Population differs from Gold manifest')
        counts = np.asarray(self.binary.sum(axis=0)).ravel()
        self.popularity = np.lexsort((np.arange(self.nitems),-counts)).astype(np.int32)
        print(f'{name}: users={self.nusers:,}; items={self.nitems:,}; pairs={self.binary.nnz:,}; queries={self.nqueries:,}; warm-full={self.warm.sum():,}',flush=True)

    def copy_mappings(self, root):
        for table in ('users','items'):
            for path in self.gold.files(self.name+'/'+table):
                target = Path(root)/'mappings'/table/path.name
                target.parent.mkdir(parents=True,exist_ok=True)
                if not target.exists(): shutil.copyfile(path,target)
                if sha256(target) != sha256(path): raise ValueError('Saved mapping corrupted')


def normalize_rows(matrix):
    matrix = np.asarray(matrix,dtype=np.float32)
    if not np.isfinite(matrix).all(): raise ValueError('Non-finite profile/vector')
    norms = np.linalg.norm(matrix,axis=1)
    valid = norms > 1e-12
    matrix[valid] /= norms[valid,None]
    matrix[~valid] = 0
    return matrix, valid


def content_profiles(binary, query_user_indices, item_vectors, item_valid):
    positions = np.flatnonzero(query_user_indices >= 0)
    histories = binary[query_user_indices[positions]]
    counts = np.asarray(histories @ item_valid.astype(np.float32)).ravel()
    profiles = np.asarray(histories @ item_vectors,dtype=np.float32)
    profiles /= np.maximum(counts,1)[:,None]
    profiles, valid = normalize_rows(profiles)
    valid &= counts > 0
    return positions[valid], profiles[valid], dict(known_queries=len(positions), usable_profile_queries=int(valid.sum()),
        queries_with_no_usable_history=int((counts==0).sum()), usable_history_items_sum=int(counts.sum()))


def load_item_vectors(gold, fold, embedding_root, bundle_name=None):
    export_hash = sha256(gold.directory/'text_export_manifest.json')
    matches = []
    for path in Path(embedding_root).rglob('embedding_manifest.json'):
        manifest = read_json(path)
        if manifest.get('signature',{}).get('export_manifest_sha256') == export_hash and (bundle_name is None or path.parent.name == bundle_name):
            matches.append((path,manifest))
    if len(matches) != 1: raise ValueError('Need exactly one matching embedding bundle; set EMBEDDING_ROOT/BUNDLE_NAME. Found: '+str([p.parent.name for p,_ in matches]))
    path, manifest = matches[0]; signature = manifest['signature']; encoder = gold.contract['encoder']
    if manifest.get('status') != 'complete': raise ValueError('Embedding bundle is partial; finish encoding first')
    for key in ('model','revision','dimension','max_seq_length','normalize_embeddings','output_dtype','long_text_policy'):
        if signature.get(key) != encoder[key]: raise ValueError('Embedding encoder mismatch: '+key)
    if signature.get('cleaning_version') != 'gold_text_nfc_html_whitespace_v1' or encoder['dimension'] != 384:
        raise ValueError('Wrong cleaning/dimension')
    refs = gold.table('item_text_refs',['parent_asin','text_id','has_text'])
    if refs.parent_asin.duplicated().any() or not np.array_equal(refs.text_id.notna(),refs.has_text): raise ValueError('Invalid item text refs')
    merged = fold.items.merge(refs,on='parent_asin',how='left',validate='one_to_one')
    if merged.has_text.isna().any(): raise ValueError('Candidate missing item ref')
    wanted = set(merged.loc[merged.has_text,'text_id'])
    found = {}; listed = manifest['shards']
    if len({s['path'] for s in listed}) != len(listed) or sum(s['rows'] for s in listed) != manifest['texts'] or len(listed) != manifest['total_shards']:
        raise ValueError('Incomplete/duplicate embedding shard list')
    for i,state in enumerate(listed,1):
        shard = child(path.parent,state['path'])
        if not state['path'].startswith('texts/') or shard.suffix != '.parquet' or state.get('signature') != signature or state.get('status') != 'complete':
            raise ValueError('Unexpected embedding shard')
        if read_json(shard.with_suffix('.json')) != {k:v for k,v in state.items() if k!='path'} or sha256(shard) != state['output_sha256']:
            raise ValueError('Embedding checkpoint/checksum mismatch')
        parquet = pq.ParquetFile(shard)
        if parquet.metadata.num_rows != state['rows']: raise ValueError('Embedding row count mismatch')
        for batch in parquet.iter_batches(batch_size=1024,columns=['text_id','embedding']):
            ids = batch.column(0).to_pylist()
            selected = [j for j,key in enumerate(ids) if key in wanted]
            if not selected: continue
            picked = batch.take(pa.array(selected,pa.int32()))
            column = picked.column(1)
            if not pa.types.is_fixed_size_list(column.type) or column.type.list_size != 384 or column.type.value_type != pa.float32() or column.null_count or column.flatten().null_count:
                raise ValueError('Wrong vector schema/null value')
            vectors = column.flatten().to_numpy(zero_copy_only=False).reshape(len(selected),384)
            if not np.isfinite(vectors).all() or not np.allclose(np.linalg.norm(vectors,axis=1),1,atol=0.002): raise ValueError('Invalid item vector norm')
            for key,vector in zip(picked.column(0).to_pylist(),vectors):
                if key in found: raise ValueError('Duplicate required text_id across embedding shards')
                found[key] = vector.copy()
        if i == 1 or i % 20 == 0 or i == len(listed): print(f'Embedding scan {i}/{len(listed)}; matched {len(found):,}/{len(wanted):,} item texts',flush=True)
    if set(found) != wanted: raise ValueError('has_text=True item vectors missing from complete embedding bundle')
    vectors = np.zeros((fold.nitems,384),np.float32)
    valid = merged.has_text.to_numpy(bool)
    for row in merged.loc[valid,['item_idx','text_id']].itertuples(index=False): vectors[row.item_idx] = found[row.text_id]
    return vectors, valid, dict(manifest_sha256=sha256(path), bundle=path.parent.name,
        candidate_items=fold.nitems, items_with_vectors=int(valid.sum()), distinct_item_texts=len(wanted), encoder=encoder)


def numpy_topk(scores, ids, k):
    scores = np.asarray(scores,dtype=np.float32)
    ids = np.broadcast_to(np.asarray(ids,dtype=np.int32),scores.shape)
    order = np.lexsort((ids,-scores),axis=1)[:,:min(k,scores.shape[1])]
    return np.take_along_axis(ids,order,1),np.take_along_axis(scores,order,1)


def torch_topk(scores, start, k):
    """Exact score top-K; resolve boundary ties by ascending item index."""
    import torch
    n = scores.shape[1]; take = min(k,n)
    values, indices = torch.topk(scores,min(k+1,n),dim=1,sorted=True)
    selected = indices[:,:take].clone()
    if n > take:
        ties = (values[:,take-1] == values[:,take]) & torch.isfinite(values[:,take-1])
        if ties.any(): selected[ties] = torch.argsort(scores[ties],dim=1,descending=True,stable=True)[:,:take]
    values = torch.gather(scores,1,selected)
    ids = selected.cpu().numpy().astype(np.int32)+start
    return numpy_topk(values.cpu().numpy(),ids,take)


def rank_exact(queries, items, valid_items, k=20, query_batch=512, item_block=16384, device='cpu'):
    if query_batch < 1 or item_block < 1 or not np.isfinite(queries).all() or not np.isfinite(items).all(): raise ValueError('Invalid ranker input')
    out_ids = np.full((len(queries),k),-1,np.int32); out_scores = np.full((len(queries),k),-np.inf,np.float32)
    use_gpu = str(device).startswith('cuda:')
    if use_gpu:
        import torch
        torch.backends.cuda.matmul.allow_tf32 = False
        torch.set_float32_matmul_precision('highest')
        item_tensor = torch.as_tensor(items,device=device,dtype=torch.float32)
        mask_tensor = torch.as_tensor(valid_items,device=device,dtype=torch.bool)
    for start in range(0,len(queries),query_batch):
        end = min(len(queries),start+query_batch)
        best_ids = np.empty((end-start,0),np.int32); best_scores = np.empty((end-start,0),np.float32)
        if use_gpu: query_tensor = torch.as_tensor(queries[start:end],device=device,dtype=torch.float32)
        for lo in range(0,len(items),item_block):
            hi = min(len(items),lo+item_block)
            if use_gpu:
                with torch.inference_mode():
                    scores = query_tensor @ item_tensor[lo:hi].T
                    scores[:,~mask_tensor[lo:hi]] = -torch.inf
                    idx,val = torch_topk(scores,lo,k)
            else:
                scores = queries[start:end] @ items[lo:hi].T
                scores[:,~valid_items[lo:hi]] = -np.inf
                idx,val = numpy_topk(scores,np.arange(lo,hi),k)
            best_ids,best_scores = numpy_topk(np.concatenate((best_scores,val),axis=1),np.concatenate((best_ids,idx),axis=1),k)
        out_ids[start:end,:best_ids.shape[1]] = np.where(np.isfinite(best_scores),best_ids,-1)
        out_scores[start:end,:best_scores.shape[1]] = best_scores
    return out_ids,out_scores


def rank_checkpoints(query_positions, queries, items, item_valid, root, signature, config):
    root = Path(root); root.mkdir(parents=True,exist_ok=True)
    devices = config['score_devices']
    signature = dict(signature, query_positions=array_hash(query_positions), query_vectors=array_hash(queries),
        item_vectors=array_hash(items), item_valid=array_hash(item_valid), score_dtype='float32',
        backend='cuda' if any(d.startswith('cuda:') for d in devices) else 'numpy')
    digest = fingerprint(signature)
    shard_rows = config['score_shard_queries']
    output = np.full((len(query_positions),20),-1,np.int32)
    def work(task):
        index,start,end = task; path=root/f'part-{index:05d}.npz'; meta=path.with_suffix('.json')
        if path.exists() and meta.exists():
            state=read_json(meta)
            if state.get('signature') != digest or state.get('sha256') != sha256(path): raise ValueError('Scoring checkpoint mismatch')
            with np.load(path,allow_pickle=False) as data:
                if not np.array_equal(data['query_positions'],query_positions[start:end]) or data['items'].shape != (end-start,20): raise ValueError('Wrong checkpoint query range')
                predictions=data['items'].copy()
        else:
            device=devices[index%len(devices)]
            predictions,scores=rank_exact(queries[start:end],items,item_valid,query_batch=config['query_batch'],item_block=config['item_block'],device=device)
            disk_guard(root,reserve_gib=config['min_free_gib'])
            temporary=path.with_suffix('.npz.tmp')
            with temporary.open('wb') as stream: np.savez(stream,query_positions=query_positions[start:end],items=predictions,scores=scores)
            temporary.replace(path)
            save_json(meta,dict(signature=digest,sha256=sha256(path),rows=end-start))
        output[start:end] = predictions
        print(f'Scored/reused queries {end:,}/{len(queries):,}',flush=True)
    tasks=[(i,start,min(len(queries),start+shard_rows)) for i,start in enumerate(range(0,len(queries),shard_rows))]
    # One long-lived worker per device; no two jobs simultaneously allocate on one GPU.
    def device_work(rank):
        for task in tasks[rank::len(devices)]: work(task)
    with ThreadPoolExecutor(max_workers=len(devices)) as executor:
        list(executor.map(device_work,range(len(devices))))
    save_json(root/'manifest.json',dict(status='complete',signature=digest,queries=len(query_positions),shards=len(tasks),query_batch=config['query_batch'],item_block=config['item_block']))
    return output


def with_popularity(native, popularity):
    output = native.copy(); fallback = np.zeros(len(native),bool)
    limit = min(native.shape[1],len(popularity))
    for row in np.flatnonzero((native[:,:limit] < 0).any(axis=1)):
        chosen=list(native[row][native[row]>=0]); used=set(chosen)
        for item in popularity:
            if len(chosen) >= limit: break
            if int(item) not in used: chosen.append(int(item)); used.add(int(item))
        output[row,:len(chosen)] = chosen
        fallback[row] = True
    return output,fallback


def query_metrics(fold, predictions):
    predictions=np.asarray(predictions,dtype=np.int32)
    if predictions.shape != (fold.nqueries,20) or (predictions >= fold.nitems).any() or (predictions < -1).any(): raise ValueError('Invalid predictions')
    sorted_ids=np.sort(predictions,axis=1)
    if ((sorted_ids[:,1:] == sorted_ids[:,:-1]) & (sorted_ids[:,1:]>=0)).any(): raise ValueError('Duplicate recommended item')
    safe=np.maximum(predictions,0)
    matched=fold.truth[np.arange(fold.nqueries)[:,None],safe]
    hits=(matched.toarray() if sparse.issparse(matched) else np.asarray(matched)) > 0
    hits &= predictions >= 0
    result={}
    for k in KS:
        result[f'recall@{k}']=hits[:,:k].sum(axis=1)/fold.target_counts
        weights=1/np.log2(np.arange(k)+2)
        ideal=np.cumsum(weights)[np.minimum(k,fold.target_counts)-1]
        result[f'ndcg@{k}']=(hits[:,:k]*weights).sum(axis=1)/ideal
    result['hit@10']=hits[:,:10].any(axis=1).astype(float)
    return result


def group_masks(fold):
    history=fold.queries.history_items.to_numpy()
    masks={'all_queries':np.ones(fold.nqueries,bool),'warm_full':fold.warm}
    masks.update({'history_0':history==0,'history_1':history==1,'history_2':history==2,'history_3_4':(history>=3)&(history<=4),'history_5_plus':history>=5})
    repeats=fold.cohorts.repeat_targets.to_numpy()
    masks.update(target_novel=repeats==0,target_repeat=repeats==fold.target_counts,target_mixed=(repeats>0)&(repeats<fold.target_counts))
    return masks


def paired_ci(delta, samples=1000, seed=42):
    if len(delta)==0: return None
    rng=np.random.default_rng(seed)
    means=np.empty(samples)
    for i in range(samples): means[i]=np.mean(delta[rng.integers(0,len(delta),size=len(delta),dtype=np.int32)])
    lo,hi=np.quantile(means,[0.025,0.975])
    return dict(mean_delta=float(np.mean(delta)),ci95=[float(lo),float(hi)],bootstrap_samples=samples,queries=len(delta))


def evaluate(fold, native, root, model, config):
    root=Path(root); root.mkdir(parents=True,exist_ok=True)
    system,fallback=with_popularity(native,fold.popularity)
    popular=np.full(native.shape,-1,np.int32)
    popular[:,:min(20,fold.nitems)]=fold.popularity[:20]
    masks=group_masks(fold); rows=[]; per_query={}; native_available=(native>=0).any(axis=1)
    for name,predictions,used in (('popularity',popular,np.zeros(fold.nqueries,bool)),(model+'_native',native,np.zeros(fold.nqueries,bool)),(model+'_popularity',system,fallback)):
        measures=query_metrics(fold,predictions)
        reasons=np.full(fold.nqueries,'none',dtype=object)
        reasons[used & ~native_available & (fold.user_indices<0)]='cold_user'
        reasons[used & ~native_available & (fold.user_indices>=0)]='no_usable_content_profile' if model=='content' else 'zero_or_invalid_user_factor'
        reasons[used & native_available]='partial_list_fill'
        frame=pd.DataFrame(dict(query_id=fold.queries.query_id,warm_full=fold.warm,native_available=native_available,
            fallback_used=used,fallback_reason=reasons,**measures))
        frame.to_parquet(root/(name+'_per_query.parquet'),index=False)
        per_query[name]=measures
        for group,mask in masks.items():
            count=int(mask.sum())
            selected=predictions[mask,:10]; distinct=np.unique(selected[selected>=0])
            row=dict(model=name,cohort=group,queries=count,catalog_coverage_at10=len(distinct)/fold.nitems if count else None,
                prediction_coverage=float((predictions[mask]>=0).any(axis=1).mean()) if count else None,
                native_coverage=float(native_available[mask].mean()) if count and name!='popularity' else None,
                fallback_rate=float(used[mask].mean()) if count else None,
                candidate_target_coverage=float(fold.cohorts.targets_in.to_numpy()[mask].sum()/fold.target_counts[mask].sum()) if count else None)
            row.update({key:float(value[mask].mean()) if count else None for key,value in measures.items()})
            rows.append(row)
        # Compact indices are decoded with the saved fold item mapping, not row order from another fold.
        schema=pa.schema([('query_id',pa.string()),('item_indices',pa.list_(pa.int32())),('fallback',pa.bool_())])
        with pq.ParquetWriter(root/(name+'_top20.parquet'),schema,compression='snappy') as writer:
            for start in range(0,fold.nqueries,4096):
                end=min(fold.nqueries,start+4096)
                writer.write_table(pa.Table.from_arrays([pa.array(fold.queries.query_id.iloc[start:end]),
                    pa.array(predictions[start:end].tolist(),type=pa.list_(pa.int32())),pa.array(used[start:end])],schema=schema))
    pd.DataFrame(rows).to_csv(root/'metrics.csv',index=False)
    confidence={}
    for name in (model+'_native',model+'_popularity'):
        for group in ('all_queries','warm_full'):
            mask=masks[group]
            delta=per_query[name]['ndcg@10'][mask]-per_query['popularity']['ndcg@10'][mask]
            confidence[name+'/'+group]=paired_ci(delta,config['bootstrap_samples'],config['seed'])
    save_json(root/'confidence_vs_popularity.json',confidence)
    save_json(root/'evaluation_manifest.json',dict(status='complete',source=fold.gold.signature,fold=fold.name,
        queries=fold.nqueries,warm_full=int(fold.warm.sum()),model=model,metric_policy='macro query; targets outside candidates retained; repeats allowed',metrics=rows))
    return rows


def preflight(config):
    if config['algorithm'] not in ('als','content') or config['phase'] not in BOUNDS or config['category'] not in CATEGORIES:
        raise ValueError('Invalid algorithm/phase/category')
    for key in ('query_batch','item_block','score_shard_queries','bootstrap_samples','cpu_threads'):
        if type(config[key]) is not int or config[key] < 1: raise ValueError('Positive integer required: '+key)
    devices=config['score_devices']
    if not devices or len(set(devices)) != len(devices) or ('cpu' in devices and devices!=['cpu']):
        raise ValueError('Use unique CUDA devices, or [cpu] explicitly')
    hardware=dict(python=os.sys.version.split()[0],score_devices=devices,gpus=[])
    if any(d!='cpu' for d in devices):
        import torch
        cuda_available=torch.cuda.is_available()
        gpu_count=torch.cuda.device_count()
        print('CUDA diagnostics:',dict(torch=torch.__version__,torch_cuda=torch.version.cuda,
            cuda_available=cuda_available,gpu_count=gpu_count,requested=devices),flush=True)
        for device in devices:
            if not device.startswith('cuda:') or not device[5:].isdigit():
                raise ValueError('Invalid scoring device: '+str(device)+'; use cuda:0, cuda:1, ... or cpu')
            if not cuda_available or int(device[5:]) >= gpu_count:
                raise RuntimeError(f'Scoring requested {devices}, but CUDA available={cuda_available}, visible GPUs={gpu_count}, '
                    f'Torch={torch.__version__}, Torch CUDA={torch.version.cuda}. '
                    "Enable the Kaggle GPU accelerator and restart the session; with one usable GPU set SCORE_DEVICES=['cuda:0']; "
                    "for CPU scoring explicitly set SCORE_DEVICES=['cpu']. ALS_BACKEND does not control scoring.")
        torch.backends.cuda.matmul.allow_tf32=False
        torch.set_float32_matmul_precision('highest')
        hardware['gpus']=[dict(device=d,name=torch.cuda.get_device_name(d),total_memory=torch.cuda.get_device_properties(d).total_memory) for d in devices]
        hardware['torch_cuda']=torch.version.cuda
    if config['algorithm']=='als':
        try:
            import implicit
        except ModuleNotFoundError as error:
            if error.name != 'implicit': raise
            raise RuntimeError('Missing implicit in training interpreter. Run the dependency installation cell successfully before training.') from error
        if implicit.__version__ not in ('0.7.2','0.7.3'): raise RuntimeError('Supported implicit versions: 0.7.2 / 0.7.3')
        if config['als_backend']=='gpu':
            import implicit.gpu
            if not implicit.gpu.HAS_CUDA: raise RuntimeError('implicit CUDA backend unavailable. Version 0.7.3 requires CUDA 13 + RMM (install implicit[gpu]==0.7.3). Verify GPU/runtime support, or explicitly use ALS_BACKEND=cpu; GPU scoring remains independent. No silent fallback.')
            from implicit.gpu.als import AlternatingLeastSquares
            probe=AlternatingLeastSquares(factors=32,iterations=2,regularization=0.1,alpha=1,random_state=config['seed'])
            probe.fit(sparse.eye(3,format='csr',dtype=np.float32)*2,show_progress=False)
            cpu_probe=probe.to_cpu()
            if not np.isfinite(cpu_probe.user_factors).all(): raise RuntimeError('GPU ALS preflight produced invalid factors')
            del probe,cpu_probe
            hardware['als_fit_device']='cuda:0'
        elif config['als_backend']!='cpu': raise ValueError('ALS_BACKEND must be gpu or cpu')
    print('Runtime:',versions(),'scoring:',devices,'algorithm:',config['algorithm'],
        'ALS backend:',config.get('als_backend') if config['algorithm']=='als' else 'not used',flush=True)
    return hardware


def fit_als(fold, parameters, directory, signature, config):
    directory=Path(directory); directory.mkdir(parents=True,exist_ok=True)
    manifest_path=directory/'fit_manifest.json'
    expected=fingerprint(dict(source=signature,fold=fold.name,parameters=parameters,seed=config['seed'],backend=config['als_backend']))
    if manifest_path.exists():
        state=read_json(manifest_path)
        if state.get('signature')!=expected: raise ValueError('ALS model source/config mismatch')
        if state.get('status')=='complete' and not state.get('discarded'):
            for name in ('user_factors','item_factors'):
                if sha256(directory/(name+'.npy')) != state['hashes'][name]: raise ValueError('ALS factor checkpoint corrupted')
            return np.load(directory/'user_factors.npy',allow_pickle=False),np.load(directory/'item_factors.npy',allow_pickle=False),state
    confidence=fold.binary.copy()
    confidence.data.fill(1+parameters['confidence_alpha'])
    if config['als_backend']=='gpu':
        from implicit.gpu.als import AlternatingLeastSquares
        model=AlternatingLeastSquares(factors=parameters['factors'],regularization=parameters['regularization'],
            alpha=1.0,iterations=parameters['iterations'],random_state=config['seed'],dtype=np.float32,calculate_training_loss=True)
    else:
        from implicit.cpu.als import AlternatingLeastSquares
        model=AlternatingLeastSquares(factors=parameters['factors'],regularization=parameters['regularization'],
            alpha=1.0,iterations=parameters['iterations'],random_state=config['seed'],dtype=np.float32,
            num_threads=config['cpu_threads'],calculate_training_loss=True)
    save_json(manifest_path,dict(status='running',signature=expected,parameters=parameters))
    def progress(epoch,elapsed,loss):
        save_json(directory/'fit_progress.json',dict(completed_iterations=int(epoch)+1,iteration_seconds=float(elapsed),
            loss=float(loss) if loss is not None and np.isfinite(loss) else None))
    devices=config['score_devices'] if config['als_backend']=='gpu' else []
    with Monitor(devices) as monitor:
        model.fit(confidence,show_progress=True,callback=progress)
        if config['als_backend']=='gpu': model=model.to_cpu()
        users=np.asarray(model.user_factors,dtype=np.float32); items=np.asarray(model.item_factors,dtype=np.float32)
    if users.shape!=(fold.nusers,parameters['factors']) or items.shape!=(fold.nitems,parameters['factors']) or not np.isfinite(users).all() or not np.isfinite(items).all():
        raise ValueError('Invalid ALS factor shapes/values')
    disk_guard(directory,users.nbytes+items.nbytes,config['min_free_gib'])
    hashes={name:save_array(directory/(name+'.npy'),array) for name,array in (('user_factors',users),('item_factors',items))}
    state=dict(status='complete',signature=expected,parameters=parameters,seed=config['seed'],backend=config['als_backend'],
        fold=fold.name,train_pairs=int(fold.binary.nnz),confidence_policy='observed confidence=1+confidence_alpha; absent=1; library alpha=1',hashes=hashes,resources=monitor.result)
    save_json(manifest_path,state)
    return users,items,state


def als_predictions(fold, users, items, root, signature, config, only_warm=False):
    eligible=fold.user_indices>=0
    if only_warm: eligible &= fold.warm
    positions=np.flatnonzero(eligible)
    vectors=users[fold.user_indices[positions]]
    valid=np.isfinite(vectors).all(axis=1) & (np.linalg.norm(vectors,axis=1)>1e-12)
    positions=positions[valid]; vectors=vectors[valid]
    predictions=np.full((fold.nqueries,20),-1,np.int32)
    predictions[positions]=rank_checkpoints(positions,vectors,items,np.ones(fold.nitems,bool),root,signature,config)
    return predictions


def selection_score(result):
    p=result['parameters']
    return (result['ndcg@10'],result['recall@10'],-p['factors'])


def warm_score(fold,predictions):
    if not fold.warm.any(): raise ValueError('No warm-full validation queries')
    metrics=query_metrics(fold,predictions)
    return {key:float(value[fold.warm].mean()) for key,value in metrics.items()}


def tune_als(fold, root, signature, config):
    root=Path(root); completed=[]
    for index in range(5):
        if index==0: params=dict(factors=16,regularization=0.1,confidence_alpha=1,iterations=5)
        elif index in (1,2): params=dict(factors=64,regularization=0.1,confidence_alpha=10 if index==1 else 40,iterations=20)
        elif index==3:
            preferred=max(completed[1:3],key=selection_score)['parameters']
            params=dict(preferred,factors=128)
        else:
            preferred=max(completed[1:4],key=selection_score)['parameters']
            params=dict(preferred,regularization=0.01)
        name='A'+str(index); directory=root/'trials'/name; result_path=directory/'trial_result.json'
        expected=fingerprint(dict(source=signature,parameters=params))
        if result_path.exists():
            result=read_json(result_path)
            if result.get('status')!='complete' or result.get('signature')!=expected: raise ValueError('Trial checkpoint mismatch')
        else:
            users,items,fit=fit_als(fold,params,directory,signature,config)
            with Monitor(config['score_devices']) as monitor:
                predictions=als_predictions(fold,users,items,directory/'ranking',dict(source=signature,trial=name),config,only_warm=True)
            result=dict(status='complete',signature=expected,trial=name,parameters=params,**warm_score(fold,predictions),
                warm_queries=int(fold.warm.sum()),fit_resources=fit['resources'],ranking_resources=monitor.result)
            save_json(result_path,result)
            del users,items,predictions
        completed.append(result)
        best=max(completed,key=selection_score)
        save_json(root/'selection_progress.json',dict(best=best,trials=completed))
        # Only remove our completed nonwinning factor files; retain metrics/ranking for every trial.
        for old in completed:
            if old['trial']==best['trial']: continue
            model_dir=root/'trials'/old['trial']
            for name in ('user_factors.npy','item_factors.npy'):
                path=child(root,str(path_relative(model_dir/name,root)))
                if path.exists(): path.unlink()
            state_path=model_dir/'fit_manifest.json'
            if state_path.exists(): save_json(state_path,dict(read_json(state_path),discarded=True))
        print('Trial complete:',result['trial'],'NDCG@10=',result['ndcg@10'],'current best=',best['trial'],flush=True)
    best=max(completed,key=selection_score)
    save_json(root/'best_config.json',dict(status='locked',source=signature,best=best,selection='warm-full validation NDCG@10, Recall@10, fewer factors; seed fixed'))
    pd.DataFrame([dict(trial=r['trial'],**r['parameters'],**{k:r[k] for k in ('ndcg@10','recall@10','ndcg@20','recall@20')}) for r in completed]).to_csv(root/'trials.csv',index=False)
    return best


def path_relative(path, root):
    return Path(path).resolve().relative_to(Path(root).resolve())


def discover_gold(root=None):
    if root: return Path(root).resolve()
    paths=sorted(Path('/kaggle/input').rglob('dataset_manifest.json'))
    matches=[p.parent for p in paths if read_json(p).get('export_version')=='gold_training_export_v1']
    if len(matches)!=1: raise ValueError('Attach exactly one gold-training-v2 Dataset or set GOLD_ROOT explicitly')
    return matches[0]


def discover_embedding(gold, root=None, bundle_name=None):
    root=Path(root or '/kaggle/input')
    expected=sha256(gold.directory/'text_export_manifest.json')
    matches=[]
    for path in root.rglob('embedding_manifest.json'):
        data=read_json(path)
        if data.get('signature',{}).get('export_manifest_sha256')==expected and (bundle_name is None or path.parent.name==bundle_name):
            matches.append(path)
    if len(matches)!=1: raise ValueError('Attach the matching complete category embedding Dataset or set EMBEDDING_ROOT/BUNDLE_NAME. Matches: '+str([p.parent.name for p in matches]))
    if read_json(matches[0]).get('status')!='complete': raise ValueError('Finish the partial embedding Dataset before training')
    return matches[0]


def resume_output(output, previous, minimum):
    if not previous or not Path(previous).exists(): return
    previous=Path(previous).resolve()
    if previous==Path(output).resolve(): return
    files=[p for p in previous.rglob('*') if p.is_file() and p.suffix in ('.json','.npy','.npz','.parquet','.csv','.pt')]
    if any(p.is_symlink() for p in files): raise ValueError('Resume must not contain symlinks')
    files=[p for p in files if not child(output,p.relative_to(previous)).exists()]
    disk_guard(output.parent,sum(p.stat().st_size for p in files),minimum)
    for path in files:
        target=child(output,path.relative_to(previous))
        target.parent.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(path,target)


def compare_results(first, second, output, samples=1000, seed=42):
    first=Path(first); second=Path(second)
    a=read_json(first/'evaluation_manifest.json'); b=read_json(second/'evaluation_manifest.json')
    if a['status']!='complete' or b['status']!='complete' or (a['source'],a['fold'])!=(b['source'],b['fold']):
        raise ValueError('Compare only matching Gold/category/fold evaluation results')
    af=pd.read_parquet(first/(a['model']+'_popularity_per_query.parquet'))
    bf=pd.read_parquet(second/(b['model']+'_popularity_per_query.parquet'))
    merged=af.merge(bf,on='query_id',how='outer',suffixes=('_a','_b'),validate='one_to_one',indicator=True)
    if not (merged['_merge']=='both').all() or not (merged.warm_full_a==merged.warm_full_b).all(): raise ValueError('Comparison query populations differ')
    delta=merged['ndcg@10_b'].to_numpy()-merged['ndcg@10_a'].to_numpy()
    result=dict(direction=b['model']+' minus '+a['model'],all_queries=paired_ci(delta,samples,seed),
        warm_full=paired_ci(delta[merged.warm_full_a.to_numpy(bool)],samples,seed))
    save_json(output,result)
    return result


def run(config):
    hardware=preflight(config)
    gold=Gold(discover_gold(config.get('gold_root')),config['category'])
    emb_path=discover_embedding(gold,config.get('embedding_root'),config.get('embedding_bundle')) if config['algorithm']=='content' else None
    signature=dict(gold=gold.signature,algorithm=config['algorithm'],seed=config['seed'],versions=versions(),
        code_sha256=sha256(__file__),als_backend=config.get('als_backend') if config['algorithm']=='als' else None,
        cpu_threads=config['cpu_threads'],embedding_manifest=sha256(emb_path) if emb_path else None,
        scoring_backend='cuda' if config['score_devices'][0].startswith('cuda:') else 'numpy',
        score_shard_queries=config['score_shard_queries'],policy=PROTOCOL)
    parent=Path(config['output_root']).resolve(); parent.mkdir(parents=True,exist_ok=True)
    root=parent/(config['algorithm']+'_'+config['category']+'_'+fingerprint(signature)[:16])
    previous=Path(config['resume_root'])/root.name if config.get('resume_root') else None
    resume_output(root,previous,config['min_free_gib'])
    root.mkdir(parents=True,exist_ok=True)
    if (root/'run.json').exists() and read_json(root/'run.json')['signature']!=signature: raise ValueError('Run source/config mismatch')
    phase=config['phase']; phase_root=root/phase
    phase_root.mkdir(exist_ok=True)
    if phase=='test':
        if not (root/'validation_complete.json').is_file() or read_json(root/'validation_complete.json')['source']!=signature:
            raise ValueError('Run/restore validation first. Test needs locked validation results from this exact source/config.')
    complete=phase_root/'phase_complete.json'
    if complete.is_file():
        saved=read_json(complete)
        artifacts=saved.get('artifacts',{})
        if (saved.get('status')=='complete' and saved.get('source')==signature and saved.get('fold')==phase
                and artifacts and all(child(root,p).is_file() and sha256(child(root,p))==digest for p,digest in artifacts.items())):
            print('Reused complete phase:',phase,root,flush=True)
            return root
    save_json(root/'run.json',dict(status='running',phase=phase,signature=signature,config=config,hardware=hardware))
    started=time.monotonic()
    try:
        disk_guard(root,reserve_gib=config['min_free_gib'])
        with Monitor(config['score_devices']) as loading:
            fold=gold.fold(phase)
        save_json(phase_root/'load_resources.json',loading.result)
        fold.copy_mappings(phase_root)
        if config['algorithm']=='als':
            if phase=='validation': best=tune_als(fold,root,signature,config)
            else:
                selection=read_json(root/'best_config.json')
                if selection.get('status')!='locked' or selection['source']!=signature: raise ValueError('No matching locked ALS config')
                best=selection['best']
            model_root=root/'trials'/best['trial'] if phase=='validation' else phase_root/'model'
            users,items,state=fit_als(fold,best['parameters'],model_root,signature,config)
            with Monitor(config['score_devices']) as scoring:
                native=als_predictions(fold,users,items,phase_root/'ranking',dict(source=signature,model=state['hashes']),config)
            save_json(phase_root/'score_resources.json',scoring.result)
        else:
            with Monitor(config['score_devices']) as building:
                items,item_valid,emb_info=load_item_vectors(gold,fold,emb_path.parent,emb_path.parent.name)
                items,normalized=normalize_rows(items)
                if not np.array_equal(item_valid,normalized): raise ValueError('Content vector coverage changed')
                positions,profiles,profile_info=content_profiles(fold.binary,fold.user_indices,items,item_valid)
            save_json(phase_root/'content_profiles.json',dict(embedding=emb_info,profiles=profile_info,resources=building.result,
                policy='uniform mean of distinct train items; normalized; static metadata snapshot'))
            disk_guard(root,items.nbytes+profiles.nbytes,config['min_free_gib'])
            save_array(phase_root/'item_vectors.npy',items)
            save_array(phase_root/'query_profiles.npy',profiles)
            save_array(phase_root/'profile_query_positions.npy',positions)
            with Monitor(config['score_devices']) as scoring:
                native=np.full((fold.nqueries,20),-1,np.int32)
                native[positions]=rank_checkpoints(positions,profiles,items,item_valid,phase_root/'ranking',dict(source=signature),config)
            save_json(phase_root/'score_resources.json',scoring.result)
        with Monitor(config['score_devices']) as metrics_monitor:
            rows=evaluate(fold,native,phase_root/'evaluation',config['algorithm'],config)
        save_json(phase_root/'metric_resources.json',metrics_monitor.result)
        if phase=='validation': save_json(root/'validation_complete.json',dict(status='complete',source=signature))
        artifacts={p.relative_to(root).as_posix():sha256(p) for p in sorted(phase_root.rglob('*')) if p.is_file() and p!=complete}
        if phase=='validation':
            for name in ('validation_complete.json','best_config.json'):
                if (root/name).is_file(): artifacts[name]=sha256(root/name)
        save_json(complete,dict(status='complete',source=signature,fold=phase,
            seconds=round(time.monotonic()-started,3),artifacts=artifacts))
        save_json(root/'run.json',dict(status='complete',phase=phase,signature=signature,config=config,hardware=hardware))
        print(pd.DataFrame(rows).query("cohort in ['all_queries','warm_full']").to_string(index=False))
        print('Saved results:',root,flush=True)
        return root
    except BaseException as exc:
        save_json(root/'run.json',dict(status='failed',phase=phase,signature=signature,config=config,hardware=hardware,error_type=type(exc).__name__,error=str(exc)))
        raise


def configure_kaggle_auth():
    """Read optional Kaggle Secrets without writing credentials into configs or artifacts."""
    try:
        from kaggle_secrets import UserSecretsClient
    except ImportError:
        secrets=None
    else:
        secrets=UserSecretsClient()
    if secrets is not None:
        for name in ('KAGGLE_API_TOKEN','KAGGLE_USERNAME','KAGGLE_KEY'):
            if os.environ.get(name): continue
            try: value=secrets.get_secret(name)
            except Exception: continue  # Optional; native auth is checked by the Dataset request below.
            if value: os.environ[name]=value
    # KaggleHub otherwise prioritizes the notebook session token file over env credentials.
    from kagglehub.config import set_kaggle_api_token, set_kaggle_credentials
    token=os.environ.get('KAGGLE_API_TOKEN','').strip()
    username=os.environ.get('KAGGLE_USERNAME','').strip()
    key=os.environ.get('KAGGLE_KEY','').strip()
    if token or key.startswith('KGAT_'):
        set_kaggle_api_token(token or key)
        print('Kaggle auth: explicit API token selected (value hidden).',flush=True)
    elif username and key:
        set_kaggle_credentials(username,key)
        print('Kaggle auth: explicit legacy credentials selected (values hidden).',flush=True)
    else:
        print('Kaggle auth: native/session configuration; no explicit API token or complete legacy pair found.',flush=True)


def owned_result_dataset(handle):
    """Resolve ambiguous metadata 403 using the authenticated account's own inventory."""
    from kagglehub.clients import build_kaggle_client
    from kagglesdk.datasets.types.dataset_api_service import ApiListDatasetsRequest, DatasetSelectionGroup
    owner=handle.split('/')[0].casefold()
    owner_verified=False; seen_pages=set(); token=''
    with build_kaggle_client() as api:
        for page in range(1,1001):
            request=ApiListDatasetsRequest()
            request.group=DatasetSelectionGroup.DATASET_SELECTION_GROUP_MY
            request.page=page; request.page_size=100; request.page_token=token
            result=api.datasets.dataset_api_client.list_datasets(request)
            datasets=result.datasets or []
            refs=tuple(sorted(d.ref.casefold() for d in datasets))
            if not refs: break
            if refs in seen_pages: raise RuntimeError('Kaggle owned Dataset pagination repeated; cannot establish absence')
            seen_pages.add(refs)
            for dataset in datasets:
                ref=dataset.ref.casefold()
                owner_verified |= ref.split('/')[0]==owner
                if ref==handle.casefold():
                    if dataset.is_private is not True: raise ValueError('Result Dataset must be private: '+handle)
                    return True
            next_token=result.next_page_token
            if token and not next_token: break
            token=next_token
        else:
            raise RuntimeError('Too many owned Dataset pages; cannot establish absence')
    if not owner_verified:
        raise RuntimeError('Cannot verify RESULT_OWNER against authenticated owned Datasets: '+handle)
    print('Result Dataset absent from verified owned inventory; API will create private Dataset:',handle,flush=True)
    return False


def result_dataset_private(handle):
    """Metadata 403 is ambiguous; verify owned inventory before deciding to create."""
    if not isinstance(handle,str) or not re.fullmatch(r'[A-Za-z0-9_-]+/[A-Za-z0-9_-]+',handle):
        raise ValueError('Result Dataset must be owner/slug without a version')
    from kagglehub.clients import BackendError, build_kaggle_client
    from requests.exceptions import HTTPError
    from kagglesdk.datasets.types.dataset_api_service import ApiGetDatasetMetadataRequest
    request=ApiGetDatasetMetadataRequest()
    request.owner_slug,request.dataset_slug=handle.split('/')
    try:
        with build_kaggle_client() as api:
            response=api.datasets.dataset_api_client.get_dataset_metadata(request)
    except (BackendError,HTTPError) as exc:
        response=getattr(exc,'response',None)
        status=response.status_code if response is not None else getattr(exc,'error_code',None)
        if status==404: return False
        if status==403:
            try: return owned_result_dataset(handle)
            except ValueError: raise
            except Exception:
                raise RuntimeError('Kaggle metadata HTTP 403 and owned inventory could not verify access/absence for '+handle+
                    '. Check token and RESULT_OWNER; no upload attempted.') from None
        raise RuntimeError(f'Cannot verify result Dataset {handle} (HTTP {status}). Check Internet, Kaggle Secrets/API permissions; no upload attempted.') from None
    if response.info.is_private is not True:
        raise ValueError('Result Dataset must be private: '+handle)
    return True


def publish_results(root, handle, minimum=5, previous=None):
    """Snapshot one complete category run; API failures never delete local results."""
    import kagglehub
    root=Path(root).resolve()
    state=read_json(root/'run.json')
    for phase in BOUNDS:
        done=read_json(root/phase/'phase_complete.json')
        if done.get('status')!='complete' or done.get('source')!=state['signature']:
            raise ValueError('Upload requires matching complete validation and test')
        if not done.get('artifacts') or any(not child(root,p).is_file() or sha256(child(root,p))!=digest
                for p,digest in done['artifacts'].items()):
            raise ValueError('Completed phase artifacts changed; rerun the phase before upload')
    allowed={'.json','.npy','.npz','.parquet','.csv','.pt'}
    files=[p for p in sorted(root.rglob('*')) if p.is_file() and p.suffix in allowed]
    if any(p.is_symlink() for p in root.rglob('*')): raise ValueError('Result snapshot must not contain symlinks')
    inventory={p.relative_to(root).as_posix():dict(bytes=p.stat().st_size,sha256=sha256(p)) for p in files}
    bundle=dict(format='gold_recommendation_results_v1',handle=handle,run=root.name,
        source=state['signature'],phases=list(BOUNDS),files=inventory)
    digest=fingerprint(bundle)
    receipt_path=root.parent/(root.name+'_upload_receipt.json')
    if receipt_path.is_file():
        receipt=read_json(receipt_path)
        if receipt.get('handle')==handle and receipt.get('bundle_sha256')==digest:
            if receipt.get('status')=='submitted_processing':
                print('Upload already submitted:',handle,flush=True)
                return receipt
            raise RuntimeError('Previous upload status is uncertain. Check the Dataset before removing receipt to retry: '+str(receipt_path))
    if previous and previous.get('bundle_sha256')==digest:
        receipt=dict(status='submitted_processing',handle=handle,bundle_sha256=digest,reused_remote=True)
        save_json(receipt_path,receipt)
        return receipt
    result_dataset_private(handle)
    # Reserve snapshot plus SDK archive space; keep only this category in the upload.
    disk_guard(root.parent,2*sum(v['bytes'] for v in inventory.values()),minimum)
    with tempfile.TemporaryDirectory(prefix='recommendation-upload-',dir=root.parent) as temporary:
        staging=Path(temporary)
        for path in files:
            target=staging/'recommendation_results'/root.name/path.relative_to(root)
            target.parent.mkdir(parents=True,exist_ok=True)
            shutil.copyfile(path,target)
        save_json(staging/'result_bundle.json',dict(bundle,bundle_sha256=digest))
        receipt=dict(status='upload_started',handle=handle,bundle_sha256=digest)
        save_json(receipt_path,receipt)
        try:
            kagglehub.dataset_upload(handle,str(staging),version_notes='Validation + locked test; '+digest[:16])
        except BaseException:
            save_json(receipt_path,dict(receipt,status='upload_uncertain'))
            raise RuntimeError('Kaggle upload interrupted; local results preserved. Check remote version before retrying: '+handle) from None
    receipt=dict(receipt,status='submitted_processing',url='https://www.kaggle.com/datasets/'+handle)
    save_json(receipt_path,receipt)
    print('Results uploaded; Kaggle processing may still be pending:',receipt['url'],flush=True)
    return receipt


def run_suite(config):
    """Validation -> locked test -> private Dataset, sequentially for each category."""
    categories=config['categories']
    if not categories or len(set(categories))!=len(categories) or any(c not in CATEGORIES for c in categories):
        raise ValueError('Select unique supported categories')
    publish=config.get('publish_results',True)
    handles=config.get('result_datasets',{})
    if publish:
        if len({handles.get(c) for c in categories})!=len(categories): raise ValueError('Use a separate result Dataset per category')
        configure_kaggle_auth()
        remote={c:result_dataset_private(handles[c]) for c in categories}
    # Validate every selected embedding before starting a long first category.
    for category in categories:
        gold=Gold(discover_gold(config.get('gold_root')),category)
        if config['algorithm']=='content': discover_embedding(gold,config.get('embedding_root'),config.get('embedding_bundle'))
    parent=Path(config['output_root']);parent.mkdir(parents=True,exist_ok=True)
    summary=[]
    for category in categories:
        current=dict(config,category=category)
        previous=None
        if publish and remote[category] and config.get('auto_resume',True):
            import kagglehub
            restored=Path(kagglehub.dataset_download(handles[category]))
            bundle=read_json(restored/'result_bundle.json')
            if bundle.get('format')!='gold_recommendation_results_v1' or bundle.get('handle')!=handles[category]:
                raise ValueError('Unexpected result Dataset layout: '+handles[category])
            previous=bundle
            current['resume_root']=str(restored/'recommendation_results')
        for phase in BOUNDS:
            print('\n===',config['algorithm'],category,phase,'===',flush=True)
            root=run(dict(current,phase=phase))
        receipt=publish_results(root,handles[category],config['min_free_gib'],previous) if publish else None
        summary.append(dict(category=category,run_root=str(root),dataset=handles.get(category),upload=receipt))
        save_json(parent/(config['algorithm']+'_suite_results.json'),summary)
    return summary


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',required=True)
    parser.add_argument('--suite',action='store_true')
    arguments=parser.parse_args()
    (run_suite if arguments.suite else run)(read_json(arguments.config))
