"""Standalone Gold NeuMF / DeepFM training, temporal features and resumable exact ranking."""
import argparse
import copy
import gc
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing

os.environ.setdefault('CUBLAS_WORKSPACE_CONFIG', ':4096:8')

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import torch
from torch import nn

try:
    import gold_recommendation as rec  # Generated standalone Kaggle notebook.
except ModuleNotFoundError as error:
    if error.name != 'gold_recommendation':
        raise
    import kaggle_recommendation as rec

FEATURE_SCHEMA = ['rating_count_valid', 'mean_rating', 'low_rating_fraction',
                  'high_rating_fraction', 'review_text_count', 'rating_missing',
                  'review_missing']
VARIANTS = ('NCF_ID', 'D0', 'D1', 'D2')
TOWER_VARIANTS = ('TT0', 'TT1', 'TT2')
DEFAULT_TRIALS = [dict(name='C8', dim=8, hidden=[32, 16], lr=.001, weight_decay=.00001)]
TRAINING_SIGNATURE_KEYS = ('training_profile', 'seed', 'trials', 'max_epochs', 'eval_epochs',
    'early_stopping', 'early_stop_start_epoch', 'early_stop_patience', 'early_stop_min_delta',
    'negatives', 'projection_dim', 'train_batch', 'train_device', 'score_devices', 'cpu_threads',
    'bootstrap_samples', 'score_shard_queries', 'adam_foreach', 'prefetch_train', 'score_feature_cache')
SAMPLER_VERSION = 'stateless_splitmix64_uniform_v1'


def default_config(algorithm):
    if algorithm == 'two_tower':
        config = default_config('deepfm')
        config.update(algorithm=algorithm, variants=list(TOWER_VARIANTS),
            output_root='/kaggle/working/two_tower_results', query_batch=512,
            item_block=16384, score_shard_queries=2048, train_batch=20480,
            tower_output_dim=32, publish_results=False,
            trials=[dict(name='T8', dim=8, hidden=[64], lr=.001, weight_decay=.00001)])
        return config
    if algorithm not in ('ncf', 'deepfm'):
        raise ValueError('algorithm must be ncf, deepfm or two_tower')
    return dict(algorithm=algorithm, training_profile='compact_full_users',
        variants=['NCF_ID'] if algorithm == 'ncf' else ['D0', 'D1', 'D2'],
        categories=list(rec.CATEGORIES), gold_root=None, embedding_root=None, embedding_bundle=None,
        output_root='/kaggle/working/neural_results', resume_root=None, seed=42, cpu_threads=2,
        train_device='cuda:0', score_devices=['cuda:0', 'cuda:1'], train_batch=4096,
        query_batch=32, item_block=8192, pair_batch=65536, score_shard_queries=128,
        adam_foreach=True, prefetch_train=True, score_feature_cache=True,
        max_epochs=20, eval_epochs=[1, 3, 6, 9, 12, 15, 18, 20],
        early_stopping=True, early_stop_start_epoch=6, early_stop_patience=2,
        early_stop_min_delta=.00001, negatives=4, trials=copy.deepcopy(DEFAULT_TRIALS),
        projection_dim=16, bootstrap_samples=1000, min_free_gib=5,
        publish_results=True, auto_resume=True, phases=['validation', 'test'],
        result_datasets={}, dataset_versions={'gold': None, 'embedding': None})


def validate_config(config):
    if config['algorithm'] not in ('ncf', 'deepfm', 'two_tower'):
        raise ValueError('Unknown neural algorithm')
    if config['training_profile'] != 'compact_full_users':
        raise ValueError('Only compact_full_users is implemented; keep the full Gold population')
    if type(config['early_stopping']) is not bool:
        raise ValueError('early_stopping must be boolean')
    for key in ('adam_foreach', 'prefetch_train', 'score_feature_cache'):
        if type(config[key]) is not bool:
            raise ValueError(key + ' must be boolean')
    for key in ('early_stop_start_epoch', 'early_stop_patience'):
        if type(config[key]) is not int or config[key] < 1:
            raise ValueError('Positive integer required: ' + key)
    if (isinstance(config['early_stop_min_delta'], bool)
            or not isinstance(config['early_stop_min_delta'], (int, float))
            or not np.isfinite(config['early_stop_min_delta']) or config['early_stop_min_delta'] < 0):
        raise ValueError('early_stop_min_delta must be finite and nonnegative')
    variants = config['variants']
    allowed = {'ncf': ('NCF_ID',), 'deepfm': ('D0', 'D1', 'D2'), 'two_tower': TOWER_VARIANTS}[config['algorithm']]
    if config['algorithm'] == 'two_tower' and (type(config.get('tower_output_dim')) is not int or config['tower_output_dim'] < 1):
        raise ValueError('Positive tower_output_dim required')
    if not variants or len(set(variants)) != len(variants) or any(v not in allowed for v in variants):
        raise ValueError('Invalid or duplicate variants')
    categories = config['categories']
    if not categories or len(set(categories)) != len(categories) or any(c not in rec.CATEGORIES for c in categories):
        raise ValueError('Invalid or duplicate categories')
    if config['phases'] not in (['validation'], ['validation', 'test'], ['test']):
        raise ValueError('phases must be [validation], [validation,test], or [test] with restored validation')
    for key in ('train_batch', 'query_batch', 'item_block', 'pair_batch', 'score_shard_queries',
                'max_epochs', 'cpu_threads', 'bootstrap_samples', 'projection_dim', 'negatives'):
        if type(config[key]) is not int or config[key] < 1:
            raise ValueError('Positive integer required: ' + key)
    if type(config['seed']) is not int or not 0 <= config['seed'] < 2**32:
        raise ValueError('seed must be uint32')
    epochs = config['eval_epochs']
    if not epochs or sorted(set(epochs)) != epochs or any(type(e) is not int or e < 1 or e > config['max_epochs'] for e in epochs) or epochs[-1] != config['max_epochs']:
        raise ValueError('Invalid evaluation epoch schedule')
    if config['early_stopping'] and config['early_stop_start_epoch'] > config['max_epochs']:
        raise ValueError('Early-stop start epoch exceeds max_epochs; adjust the budget or disable early stopping explicitly')
    trials = config['trials']
    if not trials or len({p['name'] for p in trials}) != len(trials):
        raise ValueError('Need unique trials')
    for trial in trials:
        if not trial['name'].isalnum() or type(trial['dim']) is not int or trial['dim'] < 1:
            raise ValueError('Invalid trial name/dimension')
        if not trial['hidden'] or any(type(h) is not int or h < 1 for h in trial['hidden']):
            raise ValueError('Invalid MLP dimensions')
        if not np.isfinite(trial['lr']) or trial['lr'] <= 0 or not np.isfinite(trial['weight_decay']) or trial['weight_decay'] < 0:
            raise ValueError('Invalid optimizer parameters')
    devices = config['score_devices']
    if not devices or len(set(devices)) != len(devices) or ('cpu' in devices and devices != ['cpu']):
        raise ValueError('Use unique CUDA score devices or [cpu]')
    torch.set_num_threads(config['cpu_threads'])
    torch.backends.cuda.matmul.allow_tf32 = False
    torch.backends.cudnn.allow_tf32 = False
    torch.use_deterministic_algorithms(True)
    for device in set(devices + [config['train_device']]):
        if device == 'cpu':
            continue
        if not device.startswith('cuda:') or not device[5:].isdigit():
            raise ValueError('Explicit device required: cpu or cuda:N')
        if not torch.cuda.is_available() or int(device[5:]) >= torch.cuda.device_count():
            raise RuntimeError('Requested ' + device + ' unavailable. Enable Kaggle GPU or explicitly select cpu; no silent fallback.')
    if any(d.startswith('cuda:') for d in devices + [config['train_device']]):
        os.environ.setdefault('CUBLAS_WORKSPACE_CONFIG', ':4096:8')


def seed_model(seed):
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)


class PairNetwork(nn.Module):
    """NeuMF uses separate GMF/MLP tables; DeepFM shares ID tables across FM/Deep."""
    def __init__(self, nusers, nitems, variant, parameters, projection_dim=16):
        super().__init__()
        if variant not in VARIANTS:
            raise ValueError('Unknown variant')
        self.variant = variant
        dim = parameters['dim']
        self.user = nn.Embedding(nusers, dim)
        self.item = nn.Embedding(nitems, dim)
        width = 2 * dim
        if variant == 'NCF_ID':
            self.gmf_user = nn.Embedding(nusers, dim)
            self.gmf_item = nn.Embedding(nitems, dim)
        else:
            self.user_bias = nn.Embedding(nusers, 1)
            self.item_bias = nn.Embedding(nitems, 1)
            self.bias = nn.Parameter(torch.zeros(1))
        if variant in ('D1', 'D2'):
            self.item_projection = nn.Linear(384, projection_dim)
            width += projection_dim + 1
        if variant == 'D2':
            self.review_projection = nn.Linear(384, projection_dim)
            width += projection_dim + len(FEATURE_SCHEMA)
        layers = []
        for hidden in parameters['hidden']:
            layers.extend([nn.Linear(width, hidden), nn.ReLU()])
            width = hidden
        self.deep = nn.Sequential(*layers)
        self.output = nn.Linear(width + (dim if variant == 'NCF_ID' else 0), 1)
        for module in self.modules():
            if isinstance(module, nn.Embedding):
                nn.init.normal_(module.weight, std=.01)
            elif isinstance(module, nn.Linear):
                nn.init.xavier_uniform_(module.weight)
                nn.init.zeros_(module.bias)
        if variant != 'NCF_ID':
            nn.init.zeros_(self.user_bias.weight)
            nn.init.zeros_(self.item_bias.weight)

    def forward(self, users, items, item_text=None, item_present=None, stats=None, reviews=None,
                item_projected=None, review_projected=None):
        u, i = self.user(users), self.item(items)
        fields = [u, i]
        if self.variant in ('D1', 'D2'):
            fields.extend([torch.relu(self.item_projection(item_text)) if item_projected is None else item_projected,
                           item_present[:, None]])
        if self.variant == 'D2':
            fields.extend([stats, torch.relu(self.review_projection(reviews)) if review_projected is None else review_projected])
        deep = self.deep(torch.cat(fields, dim=1))
        if self.variant == 'NCF_ID':
            product = self.gmf_user(users) * self.gmf_item(items)
            return self.output(torch.cat([product, deep], dim=1)).squeeze(1)
        return (self.output(deep).squeeze(1) + (u * i).sum(dim=1)
                + self.user_bias(users).squeeze(1) + self.item_bias(items).squeeze(1) + self.bias)


class TwoTowerNetwork(nn.Module):
    """Independent encoders; frozen MiniLM inputs, raw dot-product output."""
    def __init__(self, nusers, nitems, variant, parameters, projection_dim=16, output_dim=32):
        super().__init__()
        if variant not in TOWER_VARIANTS:
            raise ValueError('Unknown Two-Tower variant')
        self.variant, self.output_dim = variant, output_dim
        dim = parameters['dim']
        self.user = nn.Embedding(nusers, dim)
        self.item = nn.Embedding(nitems, dim)
        user_width = item_width = dim
        if variant in ('TT1', 'TT2'):
            self.item_projection = nn.Linear(384, projection_dim)
            item_width += projection_dim + 1
        if variant == 'TT2':
            self.review_projection = nn.Linear(384, projection_dim)
            user_width += projection_dim + len(FEATURE_SCHEMA)
        def tower(width):
            layers = []
            for hidden in parameters['hidden']:
                layers.extend([nn.Linear(width, hidden), nn.ReLU()])
                width = hidden
            return nn.Sequential(*layers, nn.Linear(width, output_dim))
        self.user_tower, self.item_tower = tower(user_width), tower(item_width)
        for module in self.modules():
            if isinstance(module, nn.Embedding):
                nn.init.normal_(module.weight, std=.01)
            elif isinstance(module, nn.Linear):
                nn.init.xavier_uniform_(module.weight)
                nn.init.zeros_(module.bias)

    def encode_user(self, users, stats=None, reviews=None):
        fields = [self.user(users)]
        if self.variant == 'TT2':
            fields.extend([self.review_projection(reviews), stats])
        return self.user_tower(torch.cat(fields, dim=1))

    def encode_item(self, items, item_text=None, item_present=None):
        fields = [self.item(items)]
        if self.variant in ('TT1', 'TT2'):
            fields.extend([self.item_projection(item_text), item_present[:, None]])
        return self.item_tower(torch.cat(fields, dim=1))

    def forward(self, users, items, item_text=None, item_present=None, stats=None, reviews=None):
        return (self.encode_user(users, stats, reviews) * self.encode_item(items, item_text, item_present)).sum(dim=1)


def make_model(nusers, nitems, variant, parameters, config):
    if variant in TOWER_VARIANTS:
        return TwoTowerNetwork(nusers, nitems, variant, parameters, config['projection_dim'], config['tower_output_dim'])
    return PairNetwork(nusers, nitems, variant, parameters, config['projection_dim'])


def load_examples(gold, fold):
    frame = gold.table(fold.name + '/train_examples',
        ['example_id', 'user_idx', 'item_idx', 'first_pair_time', 'prediction_time', 'strength'])
    if not len(frame) or frame.isna().any().any() or frame.example_id.duplicated().any() or len(frame) != fold.binary.nnz:
        raise ValueError('Missing/duplicate positive anchors')
    if frame.duplicated(['user_idx', 'item_idx']).any() or not (frame.strength == 1).all():
        raise ValueError('Need exactly one positive per train pair')
    if not (frame.prediction_time == frame.first_pair_time).all() or (frame.prediction_time >= pd.Timestamp(fold.start)).any():
        raise ValueError('Invalid positive anchor time')
    users = frame.user_idx.to_numpy(np.int64)
    items = frame.item_idx.to_numpy(np.int64)
    if (users < 0).any() or (users >= fold.nusers).any() or (items < 0).any() or (items >= fold.nitems).any():
        raise ValueError('Positive indices out of mapping')
    if not np.all(np.asarray(fold.binary[users, items]).ravel() == 1):
        raise ValueError('Positive anchors differ from train pairs')
    pairs = gold.table(fold.name + '/train_pairs', ['user_idx', 'item_idx', 'first_pair_time'])
    checked = frame.merge(pairs, on=['user_idx', 'item_idx'], validate='one_to_one', suffixes=('', '_pair'))
    if len(checked) != len(frame) or not (checked.first_pair_time == checked.first_pair_time_pair).all():
        raise ValueError('Positive first_pair_time mismatch')
    frame = frame.sort_values('example_id', kind='stable').reset_index(drop=True)
    frame['sampler_key'] = np.fromiter((int.from_bytes(hashlib.sha256(str(x).encode()).digest()[:8], 'little')
                                       for x in frame.example_id), dtype=np.uint64, count=len(frame))
    return frame


def sample_negatives(binary, user, key, epoch, seed, count):
    values = sample_negative_batch(binary, np.array([user]), np.array([key], np.uint64), epoch, seed, count)[0]
    return values[values >= 0]


def mix64(values):
    """SplitMix64 finalizer, vectorized over independent example counters."""
    with np.errstate(over='ignore'):
        values = np.asarray(values, np.uint64) + np.uint64(0x9E3779B97F4A7C15)
        values = (values ^ (values >> np.uint64(30))) * np.uint64(0xBF58476D1CE4E5B9)
        values = (values ^ (values >> np.uint64(27))) * np.uint64(0x94D049BB133111EB)
        return values ^ (values >> np.uint64(31))


def observed_pair_keys(binary):
    if binary.shape[0] * binary.shape[1] > np.iinfo(np.int64).max:
        raise ValueError('User/item product exceeds int64 sampling keys')
    rows = np.repeat(np.arange(binary.shape[0], dtype=np.int64), np.diff(binary.indptr))
    keys = rows * binary.shape[1] + binary.indices
    return keys if binary.has_sorted_indices else np.sort(keys)


def sample_negative_batch(binary, users, keys, epoch, seed, count, observed=None):
    """Uniform train-only negatives; deterministic per example, independent of batching/prefetch."""
    users = np.asarray(users, np.int64)
    keys = np.asarray(keys, np.uint64)
    if users.shape != keys.shape or users.ndim != 1 or (users < 0).any() or (users >= binary.shape[0]).any():
        raise ValueError('Invalid negative-sampling anchors')
    nitems = binary.shape[1]
    if nitems < 1 or count < 1:
        raise ValueError('Positive candidate and negative counts required')
    observed = observed_pair_keys(binary) if observed is None else observed
    support = binary.indptr[users + 1] - binary.indptr[users]
    take = np.minimum(count, nitems - support)
    output = np.full((len(users), count), -1, np.int64)
    dense = (nitems - support <= 2 * count) | (support > nitems // 2)
    # Exact complement fallback prevents rejection loops for small/saturated pools.
    for row in np.flatnonzero(dense & (take > 0)):
        user = users[row]
        seen = binary.indices[binary.indptr[user]:binary.indptr[user + 1]]
        pool = np.setdiff1d(np.arange(nitems), seen, assume_unique=True)
        rng = np.random.default_rng([seed, epoch, int(keys[row]) & 0xffffffff, int(keys[row]) >> 32])
        output[row, :take[row]] = rng.choice(pool, size=take[row], replace=False)
    base = keys ^ mix64(np.uint64(seed) ^ np.uint64(0xA0761D6478BD642F)) ^ mix64(np.uint64(epoch) ^ np.uint64(0xE7037ED1A0B428DB))
    counters = np.zeros(len(users), np.uint64)
    # Accept an exact multiple of nitems outcomes; reject the uint64 tail to avoid modulo bias.
    limit = np.uint64(((2**64 - 1) // nitems) * nitems)
    for slot in range(count):
        pending = np.flatnonzero(~dense & (take > slot))
        while len(pending):
            with np.errstate(over='ignore'):
                random = mix64(base[pending] + counters[pending] * np.uint64(0x9E3779B97F4A7C15))
            counters[pending] += np.uint64(1)
            candidate = (random % np.uint64(nitems)).astype(np.int64)
            positions = np.searchsorted(observed, users[pending] * nitems + candidate)
            blocked = np.zeros(len(pending), bool)
            valid = positions < len(observed)
            blocked[valid] = observed[positions[valid]] == users[pending[valid]] * nitems + candidate[valid]
            if slot:
                blocked |= (output[pending, :slot] == candidate[:, None]).any(axis=1)
            accepted = ~blocked & (random < limit)
            output[pending[accepted], slot] = candidate[accepted]
            pending = pending[~accepted]
    return output


def epoch_batches(examples, binary, epoch, config):
    rng = np.random.default_rng([config['seed'], epoch])
    order = rng.permutation(len(examples))
    us = examples.user_idx.to_numpy(np.int64)
    it = examples.item_idx.to_numpy(np.int64)
    keys = examples.sampler_key.to_numpy(np.uint64)
    observed = observed_pair_keys(binary)
    # Keep feature references, never repeat dense D2 profiles for all sampled negatives on disk.
    anchors_per_batch = max(1, config['train_batch'] // (config['negatives'] + 1))
    for start in range(0, len(order), anchors_per_batch):
        anchors = order[start:start + anchors_per_batch]
        negative = sample_negative_batch(binary, us[anchors], keys[anchors], epoch,
                                         config['seed'], config['negatives'], observed)
        items = np.column_stack([it[anchors], negative])
        valid = items >= 0
        labels = np.zeros(items.shape, np.float32); labels[:, 0] = 1
        yield (np.broadcast_to(us[anchors, None], items.shape)[valid], items[valid], labels[valid],
               np.broadcast_to(anchors[:, None], items.shape)[valid])


def load_review_vectors(gold, manifest_path, wanted, directory, config):
    """Load only required text IDs into disk-backed rows, validating every shard like content."""
    manifest = rec.read_json(manifest_path)
    signature = manifest['signature']
    encoder = gold.contract['encoder']
    if manifest.get('status') != 'complete' or signature.get('export_manifest_sha256') != rec.sha256(gold.directory / 'text_export_manifest.json'):
        raise ValueError('Wrong/incomplete review embedding source')
    for key in ('model', 'revision', 'dimension', 'max_seq_length', 'normalize_embeddings', 'output_dtype', 'long_text_policy'):
        if signature.get(key) != encoder[key]:
            raise ValueError('Review embedding encoder mismatch: ' + key)
    if encoder['dimension'] != 384 or signature.get('cleaning_version') != 'gold_text_nfc_html_whitespace_v1':
        raise ValueError('Wrong review encoder dimension/cleaning')
    ids = sorted(wanted)
    lookup = {key: row for row, key in enumerate(ids)}
    directory.mkdir(parents=True, exist_ok=True)
    rec.disk_guard(directory, max(1, len(ids)) * 384 * 4, config['min_free_gib'])
    vectors = np.lib.format.open_memmap(directory / 'review_vectors.npy', mode='w+', dtype=np.float32,
                                        shape=(max(1, len(ids)), 384))
    vectors[:] = 0
    found = np.zeros(len(ids), bool)
    shards = manifest['shards']
    if len({s['path'] for s in shards}) != len(shards) or len(shards) != manifest['total_shards'] or sum(s['rows'] for s in shards) != manifest['texts']:
        raise ValueError('Incomplete/duplicate review shard list')
    for number, state in enumerate(shards, 1):
        path = rec.child(manifest_path.parent, state['path'])
        if not state['path'].startswith('texts/') or path.suffix != '.parquet' or state.get('signature') != signature or state.get('status') != 'complete':
            raise ValueError('Unexpected review embedding shard')
        if rec.read_json(path.with_suffix('.json')) != {k: v for k, v in state.items() if k != 'path'} or rec.sha256(path) != state['output_sha256']:
            raise ValueError('Review embedding checksum/checkpoint mismatch')
        parquet = pq.ParquetFile(path)
        if parquet.metadata.num_rows != state['rows']:
            raise ValueError('Review embedding row count mismatch')
        for batch in parquet.iter_batches(batch_size=1024, columns=['text_id', 'embedding']):
            batch_ids = batch.column(0).to_pylist()
            selected = [i for i, key in enumerate(batch_ids) if key in lookup]
            if not selected:
                continue
            picked = batch.take(pa.array(selected, pa.int32()))
            column = picked.column(1)
            if not pa.types.is_fixed_size_list(column.type) or column.type.list_size != 384 or column.type.value_type != pa.float32() or column.null_count or column.flatten().null_count:
                raise ValueError('Invalid review vector schema')
            values = column.flatten().to_numpy(zero_copy_only=False).reshape(-1, 384)
            if not np.isfinite(values).all() or not np.allclose(np.linalg.norm(values, axis=1), 1, atol=.002):
                raise ValueError('Invalid review embedding norm')
            for key, vector in zip(picked.column(0).to_pylist(), values):
                row = lookup[key]
                if found[row]:
                    raise ValueError('Duplicate required review text_id')
                vectors[row] = vector
                found[row] = True
        if number == 1 or number % 20 == 0 or number == len(shards):
            print('Review embedding shards:', number, '/', len(shards), 'matched', int(found.sum()), '/', len(ids), flush=True)
    if not found.all():
        raise ValueError('Required review vector missing from complete bundle')
    vectors.flush()
    return vectors, lookup


def prefix_features(events, anchors, vectors, stats_out, reviews_out):
    """One user at a time; emit before consuming equal-time events. Repeated events retain weight."""
    events = events.sort_values(['user_idx', 'event_time', 'record_hash'], kind='stable').reset_index(drop=True)
    anchors = anchors.sort_values(['user_idx', 'prediction_time'], kind='stable')
    event_users = events.user_idx.to_numpy(np.int64)
    event_times = events.event_time.to_numpy(dtype='datetime64[ns]')
    rating = events.rating.to_numpy(np.float64, na_value=np.nan)
    vector_rows = events.vector_row.to_numpy(np.int64)
    anchor_users = anchors.user_idx.to_numpy(np.int64)
    times = anchors.prediction_time.to_numpy(dtype='datetime64[ns]')
    destinations = anchors.feature_row.to_numpy(np.int64)
    pos = 0
    while pos < len(anchors):
        user = anchor_users[pos]
        end = np.searchsorted(anchor_users, user, side='right')
        lo = np.searchsorted(event_users, user, side='left')
        hi = np.searchsorted(event_users, user, side='right')
        event = lo
        valid_count = text_count = low_count = high_count = 0
        rating_sum = 0.
        vector_sum = np.zeros(384, np.float64)
        for anchor in range(pos, end):
            while event < hi and event_times[event] < times[anchor]:
                value = rating[event]
                if np.isfinite(value):
                    if not 1 <= value <= 5:
                        raise ValueError('Historical rating outside 1..5')
                    valid_count += 1
                    rating_sum += value
                    low_count += int(value <= 2)
                    high_count += int(value >= 4)
                if vector_rows[event] >= 0:
                    vector_sum += vectors[vector_rows[event]]
                    text_count += 1
                event += 1
            row = destinations[anchor]
            norm = np.linalg.norm(vector_sum)
            usable = text_count > 0 and norm > 1e-12
            reviews_out[row] = vector_sum / norm if usable else 0
            stats_out[row] = [valid_count, rating_sum / valid_count if valid_count else 3.,
                low_count / valid_count if valid_count else 0., high_count / valid_count if valid_count else 0.,
                text_count, float(valid_count == 0), float(not usable)]
        pos = end


def feature_arrays(root):
    return {name: np.load(root / (name + '.npy'), mmap_mode='r')
            for name in ('stats', 'reviews', 'train_feature_rows', 'query_feature_rows', 'scaler_mean', 'scaler_scale')}


def build_d2(gold, fold, examples, manifest_path, root, config):
    root.mkdir(parents=True, exist_ok=True)
    signature = rec.fingerprint(dict(source=gold.signature, fold=fold.name, embedding=rec.sha256(manifest_path),
        examples=rec.array_hash(examples.sampler_key.to_numpy()), schema=FEATURE_SCHEMA, code=rec.sha256(__file__)))
    complete = root / 'features.json'
    if complete.is_file():
        state = rec.read_json(complete)
        if state.get('signature') != signature:
            raise ValueError('D2 feature cache source mismatch')
        if state.get('status') == 'complete' and not state.get('cache_compacted'):
            if any(not (root / name).is_file() or rec.sha256(root / name) != digest for name, digest in state['files'].items()):
                raise ValueError('D2 feature cache checksum mismatch')
            return feature_arrays(root)
    rec.save_json(complete, dict(status='building', signature=signature))
    for name in ('query_stats.npy', 'query_reviews.npy'):
        (root / name).unlink(missing_ok=True)
    events = gold.table(fold.name + '/history_events', ['record_hash', 'user_id', 'event_time'])
    if events.isna().any().any() or events.record_hash.duplicated().any() or (events.event_time >= pd.Timestamp(fold.start)).any():
        raise ValueError('Invalid/future historical events')
    users = gold.table(fold.name + '/users', ['user_id', 'user_idx'])
    events = events.merge(users, on='user_id', how='left', validate='many_to_one')
    if events.user_idx.isna().any():
        raise ValueError('Historical user absent from train mapping')
    refs = gold.table('review_text_refs', ['record_hash', 'user_id', 'event_time', 'text_id', 'has_text'])
    ratings = gold.table('history_ratings', ['record_hash', 'rating'])
    if refs.record_hash.duplicated().any() or ratings.record_hash.duplicated().any() or not np.array_equal(refs.text_id.notna(), refs.has_text):
        raise ValueError('Invalid review refs/rating lookup')
    events = events.merge(refs, on='record_hash', how='left', validate='one_to_one', suffixes=('', '_ref'))
    if events.has_text.isna().any() or not (events.user_id == events.user_id_ref).all() or not (events.event_time == events.event_time_ref).all():
        raise ValueError('Historical review refs mismatch')
    events = events.merge(ratings, on='record_hash', how='left', validate='one_to_one', indicator=True)
    if not (events['_merge'] == 'both').all():
        raise ValueError('Missing historical rating lookup row')
    anchors = pd.concat([examples[['user_idx', 'prediction_time']],
        pd.DataFrame(dict(user_idx=fold.user_indices, prediction_time=pd.Timestamp(fold.start)))], ignore_index=True)
    keys = pd.MultiIndex.from_frame(anchors)
    codes, unique = pd.factorize(keys, sort=True)
    unique_anchors = unique.to_frame(index=False)
    unique_anchors.columns = ['user_idx', 'prediction_time']
    # Row 0 is the shared canonical empty-prefix row; only anchors after a user's first event need distinct rows.
    first_event_time = events.groupby('user_idx', sort=False)['event_time'].min()
    has_prior = ((unique_anchors.user_idx >= 0)
                 & (unique_anchors.prediction_time > unique_anchors.user_idx.map(first_event_time))).to_numpy(bool)
    active_anchors = unique_anchors.loc[has_prior].copy()
    active_anchors['feature_row'] = np.arange(1, len(active_anchors) + 1, dtype=np.int64)
    anchor_rows = np.zeros(len(unique_anchors), dtype=np.int64)
    anchor_rows[has_prior] = active_anchors['feature_row'].to_numpy()
    feature_codes = anchor_rows[codes]
    n_feature_rows = len(active_anchors) + 1
    max_anchor_time = active_anchors.drop_duplicates('user_idx', keep='last').set_index('user_idx')['prediction_time']
    consumed_text = events.has_text & (events.event_time < events.user_idx.map(max_anchor_time))
    wanted = set(events.loc[consumed_text, 'text_id'])
    anchor_bytes = n_feature_rows * (384 + len(FEATURE_SCHEMA)) * 4
    vector_bytes = max(1, len(wanted)) * 384 * 4
    temp_base = Path(tempfile.gettempdir())
    use_temp_vectors = temp_base.exists() and shutil.disk_usage(temp_base).free > shutil.disk_usage(root).free
    rec.disk_guard(root, anchor_bytes + (0 if use_temp_vectors else vector_bytes), config['min_free_gib'])
    temp_context = tempfile.TemporaryDirectory(prefix='d2-vectors-', dir=temp_base) if use_temp_vectors else None
    try:
        vector_root = Path(temp_context.name) if temp_context is not None else root
        vectors, lookup = load_review_vectors(gold, manifest_path, wanted, vector_root, config)
        events['vector_row'] = events.text_id.map(lookup).fillna(-1).astype(np.int64)
        stats = np.lib.format.open_memmap(root / 'stats.npy', mode='w+', dtype=np.float32, shape=(n_feature_rows, len(FEATURE_SCHEMA)))
        reviews = np.lib.format.open_memmap(root / 'reviews.npy', mode='w+', dtype=np.float32, shape=(n_feature_rows, 384))
        stats[0] = [0., 3., 0., 0., 0., 1., 1.]
        reviews[0] = 0.
        prefix_features(events, active_anchors, vectors, stats, reviews)
        stats.flush(); reviews.flush()
        del vectors
        gc.collect()
        # Intermediate vector lookup is reproducible from Input and not part of the model Output.
        (vector_root / 'review_vectors.npy').unlink(missing_ok=True)
    finally:
        if temp_context is not None:
            temp_context.cleanup()
    train_rows, query_rows = feature_codes[:len(examples)], feature_codes[len(examples):]
    # Only continuous counts/rates are scaled. Missing indicators remain binary.
    count_columns = [0, 4]
    continuous = [0, 1, 2, 3, 4]
    sums = np.zeros(len(continuous), np.float64); squares = sums.copy()
    for start in range(0, len(train_rows), 8192):
        block = np.asarray(stats[train_rows[start:start + 8192]], np.float64)
        block[:, count_columns] = np.log1p(block[:, count_columns])
        values = block[:, continuous]
        sums += values.sum(axis=0); squares += (values * values).sum(axis=0)
    mean = np.zeros(len(FEATURE_SCHEMA), np.float32); scale = np.ones(len(FEATURE_SCHEMA), np.float32)
    mean[continuous] = sums / len(train_rows)
    scale[continuous] = np.sqrt(np.maximum(squares / len(train_rows) - (sums / len(train_rows)) ** 2, 0))
    scale[scale < 1e-6] = 1
    for name, array in [('train_feature_rows', train_rows), ('query_feature_rows', query_rows),
                        ('scaler_mean', mean), ('scaler_scale', scale)]:
        rec.save_array(root / (name + '.npy'), array)
    empty_anchors = len(unique_anchors) - len(active_anchors)
    state = dict(status='complete', signature=signature, schema=FEATURE_SCHEMA, anchors=len(unique_anchors),
        feature_rows=n_feature_rows, consumed_review_texts=len(wanted),
        events=len(events), events_with_text=int(events.has_text.sum()),
        empty_rating_anchors=int(empty_anchors + (stats[1:, 5] == 1).sum()),
        empty_review_anchors=int(empty_anchors + (stats[1:, 6] == 1).sum()),
        policy='strict-before whole timestamp; event-weighted review mean then L2; scaler fit positive train anchors only',
        files={p.name: rec.sha256(p) for p in root.glob('*.npy')})
    rec.save_json(complete, state)
    del stats, reviews
    return feature_arrays(root)


class Features:
    def __init__(self, variant, item_vectors=None, item_valid=None, d2=None):
        self.variant = {'TT0': 'D0', 'TT1': 'D1', 'TT2': 'D2'}.get(variant, variant)
        self.items, self.valid, self.d2 = item_vectors, item_valid, d2

    def arrays(self, items, rows):
        return dict(self.item_arrays(items), **self.user_arrays(rows))

    def item_arrays(self, items):
        output = {}
        if self.variant in ('D1', 'D2'):
            output['item_text'] = np.array(self.items[items], dtype=np.float32, copy=True)
            output['item_present'] = self.valid[items].astype(np.float32)
        return output

    def user_arrays(self, rows):
        output = {}
        if self.variant == 'D2':
            stats = np.array(self.d2['stats'][rows], dtype=np.float32, copy=True)
            stats[:, [0, 4]] = np.log1p(stats[:, [0, 4]])
            stats = (stats - self.d2['scaler_mean']) / self.d2['scaler_scale']
            output['stats'] = stats
            output['reviews'] = np.array(self.d2['reviews'][rows], copy=True)
        return output

    def tensors(self, items, rows, device):
        return {key: torch.as_tensor(value, device=device) for key, value in self.arrays(items, rows).items()}


class ScoringFeatures:
    """Checkpoint-local projections: no repeated 384-D transfer/projection for every user-item pair."""
    def __init__(self, model, features, feature_rows, device, batch=8192):
        self.variant = features.variant
        self.items = self.present = self.rows = self.stats = self.reviews = None
        if self.variant not in ('D1', 'D2'):
            return
        needed_rows = np.unique(feature_rows) if self.variant == 'D2' else np.empty(0, np.int64)
        estimate = len(features.items) * (model.item_projection.out_features + 1) * 4
        if self.variant == 'D2':
            estimate += len(needed_rows) * (model.review_projection.out_features + len(FEATURE_SCHEMA) + 2) * 4
        if str(device).startswith('cuda:') and estimate > torch.cuda.mem_get_info(device)[0] * .5:
            raise RuntimeError('Scoring feature cache exceeds half free VRAM; set SCORE_FEATURE_CACHE=False explicitly')
        with torch.inference_mode():
            self.items = torch.empty((len(features.items), model.item_projection.out_features), device=device)
            self.present = torch.as_tensor(features.valid.astype(np.float32), device=device)
            for start in range(0, len(features.items), batch):
                stop = min(start + batch, len(features.items))
                text = torch.as_tensor(np.array(features.items[start:stop], copy=True), device=device)
                self.items[start:stop] = torch.relu(model.item_projection(text))
            if self.variant == 'D2':
                self.rows = torch.as_tensor(needed_rows, device=device)
                self.stats = torch.empty((len(needed_rows), len(FEATURE_SCHEMA)), device=device)
                self.reviews = torch.empty((len(needed_rows), model.review_projection.out_features), device=device)
                for start in range(0, len(needed_rows), batch):
                    stop = min(start + batch, len(needed_rows)); rows = needed_rows[start:stop]
                    stats = np.array(features.d2['stats'][rows], dtype=np.float32, copy=True)
                    stats[:, [0, 4]] = np.log1p(stats[:, [0, 4]])
                    self.stats[start:stop] = torch.as_tensor((stats - features.d2['scaler_mean']) / features.d2['scaler_scale'], device=device)
                    reviews = torch.as_tensor(np.array(features.d2['reviews'][rows], copy=True), device=device)
                    self.reviews[start:stop] = torch.relu(model.review_projection(reviews))

    def tensors(self, items, rows, device):
        if self.variant not in ('D1', 'D2'):
            return {}
        items = torch.as_tensor(items, device=device, dtype=torch.long)
        output = dict(item_projected=self.items[items], item_present=self.present[items])
        if self.variant == 'D2':
            positions = torch.searchsorted(self.rows, torch.as_tensor(rows, device=device, dtype=torch.long))
            output.update(stats=self.stats[positions], review_projected=self.reviews[positions])
        return output


def training_batches(examples, binary, features, epoch, config):
    """At most one prepared CPU batch ahead; pinning runs off the GPU consumer thread."""
    pin = str(config['train_device']).startswith('cuda:')
    def prepare():
        iterator = iter(epoch_batches(examples, binary, epoch, config))
        while True:
            started = time.monotonic()
            raw = next(iterator, None)
            if raw is None:
                return
            users, items, labels, anchors = raw
            rows = features.d2['train_feature_rows'][anchors] if features.d2 else anchors
            arrays = dict(users=users, items=items, labels=labels, **features.arrays(items, rows))
            tensors = {key: torch.from_numpy(np.ascontiguousarray(value)) for key, value in arrays.items()}
            if pin:
                tensors = {key: value.pin_memory() for key, value in tensors.items()}
            yield tensors, int(labels.sum()), time.monotonic() - started
    iterator = iter(prepare())
    if not config.get('prefetch_train', True):
        yield from iterator
        return
    with ThreadPoolExecutor(max_workers=1) as executor:
        pending = executor.submit(next, iterator, None)
        while True:
            batch = pending.result()
            if batch is None:
                break
            pending = executor.submit(next, iterator, None)
            yield batch


def atomic_checkpoint(path, state, config):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    # Free space already excludes the old file; reserve the new payload before replacement.
    tensor_bytes = sum(t.numel() * t.element_size() for t in state['model'].values())
    rec.disk_guard(path.parent, tensor_bytes * (3 if 'optimizer' in state else 1) + 1024**2, config['min_free_gib'])
    temporary = path.with_suffix('.pt.tmp')
    torch.save(state, temporary)
    temporary.replace(path)
    rec.save_json(path.with_suffix('.json'), dict(sha256=rec.sha256(path), signature=state['signature'], epoch=state['epoch']))


def read_checkpoint(path, signature, device='cpu'):
    meta = rec.read_json(Path(path).with_suffix('.json'))
    if meta.get('signature') != signature or rec.sha256(path) != meta['sha256']:
        raise ValueError('Model checkpoint source/checksum mismatch')
    state = torch.load(path, map_location=device, weights_only=True)
    if state['signature'] != signature or state['epoch'] != meta['epoch']:
        raise ValueError('Model checkpoint metadata mismatch')
    return state


def score_block_tensor(model, users, items, feature_rows, features, device, pair_batch):
    """Keep pair logits on device until Top-K reduction, instead of copying each microbatch."""
    scores = torch.empty((len(users), len(items)), device=device, dtype=torch.float32)
    width = len(items)
    device_indices = isinstance(features, ScoringFeatures) or features.variant not in ('D1', 'D2')
    if device_indices:
        user_tensor = torch.as_tensor(users, device=device, dtype=torch.long)
        item_tensor = torch.as_tensor(items, device=device, dtype=torch.long)
        row_tensor = torch.as_tensor(feature_rows, device=device, dtype=torch.long)
    with torch.inference_mode():
        for start in range(0, len(users) * width, pair_batch):
            end = min(start + pair_batch, len(users) * width)
            if device_indices:
                flat = torch.arange(start, end, device=device)
                q, i = flat // width, item_tensor[flat % width]
                values = model(user_tensor[q], i, **features.tensors(i, row_tensor[q], device))
            else:
                flat = np.arange(start, end)
                q, i = flat // width, items[flat % width]
                values = model(torch.as_tensor(users[q], device=device), torch.as_tensor(i, device=device),
                               **features.tensors(i, feature_rows[q], device))
            scores.ravel()[start:end] = values
    return scores


def score_block(model, users, items, feature_rows, features, device, pair_batch):
    scores = score_block_tensor(model, users, items, feature_rows, features, device, pair_batch).cpu().numpy()
    if not np.isfinite(scores).all():
        raise ValueError('Neural scorer produced NaN/Inf')
    return scores


def device_topk(scores, ids, k=20):
    """Exact FP32 score-desc/item-ID-asc ordering in one sortable int64 key, including ties."""
    if scores.dtype != torch.float32 or scores.ndim != 2:
        raise ValueError('FP32 matrix required for exact device Top-K')
    ids = torch.broadcast_to(torch.as_tensor(ids, device=scores.device, dtype=torch.int64), scores.shape)
    # Canonicalize +/-0 so numeric ties use only item IDs. IDs are nonnegative Int32 Gold indices.
    bits = torch.where(scores == 0, torch.zeros_like(scores), scores).contiguous().view(torch.int32).to(torch.int64)
    ordered = torch.where(bits < 0, ~bits, bits ^ 0x80000000) & 0xffffffff
    keys = ((ordered - 0x80000000) << 32) | (0xffffffff - ids)
    selected = torch.topk(keys, min(k, scores.shape[1]), dim=1, sorted=True).indices
    return torch.gather(ids, 1, selected), torch.gather(scores, 1, selected)


class TowerScoringFeatures:
    """One item-output matrix per checkpoint/device, shared across scoring shards."""
    def __init__(self, model, features, nitems, device, batch=8192):
        self.features = features
        self.items = torch.empty((nitems, model.output_dim), dtype=torch.float32, device=device)
        with torch.inference_mode():
            for start in range(0, nitems, batch):
                items = np.arange(start, min(start + batch, nitems), dtype=np.int64)
                inputs = {key: torch.as_tensor(value, device=device) for key, value in features.item_arrays(items).items()}
                self.items[start:start + len(items)] = model.encode_item(torch.as_tensor(items, device=device), **inputs)
        if not bool(torch.isfinite(self.items).all()):
            raise ValueError('Non-finite item tower output')


def rank_tower(model, users, feature_rows, cache, config, device):
    output = np.full((len(users), 20), -1, np.int32)
    values = np.full((len(users), 20), -np.inf, np.float32)
    with torch.inference_mode():
        for start in range(0, len(users), config['query_batch']):
            stop = min(start + config['query_batch'], len(users))
            inputs = {key: torch.as_tensor(value, device=device)
                      for key, value in cache.features.user_arrays(feature_rows[start:stop]).items()}
            query = model.encode_user(torch.as_tensor(users[start:stop], device=device), **inputs)
            best_ids = torch.empty((stop - start, 0), device=device, dtype=torch.long)
            best_scores = torch.empty((stop - start, 0), device=device)
            finite = torch.isfinite(query).all()
            for lo in range(0, len(cache.items), config['item_block']):
                items = cache.items[lo:lo + config['item_block']]
                scores = query @ items.T
                finite &= torch.isfinite(scores).all()
                ids, scores = device_topk(scores, np.arange(lo, lo + len(items)))
                best_ids, best_scores = device_topk(torch.cat([best_scores, scores], dim=1), torch.cat([best_ids, ids], dim=1))
            if not bool(finite):
                raise ValueError('Non-finite Two-Tower scores')
            width = best_ids.shape[1]
            output[start:stop, :width] = best_ids.cpu().numpy()
            values[start:stop, :width] = best_scores.cpu().numpy()
    return output, values


def rank_neural(model, users, feature_rows, features, nitems, config, device):
    model.eval()
    if isinstance(model, TwoTowerNetwork):
        cache = features if isinstance(features, TowerScoringFeatures) else TowerScoringFeatures(model, features, nitems, device)
        return rank_tower(model, users, feature_rows, cache, config, device)
    if config.get('score_feature_cache', True) and not isinstance(features, ScoringFeatures):
        features = ScoringFeatures(model, features, feature_rows, device)
    output = np.full((len(users), 20), -1, np.int32)
    values = np.full((len(users), 20), -np.inf, np.float32)
    for start in range(0, len(users), config['query_batch']):
        end = min(len(users), start + config['query_batch'])
        best_ids = torch.empty((end - start, 0), device=device, dtype=torch.long)
        best_scores = torch.empty((end - start, 0), device=device, dtype=torch.float32)
        finite = torch.tensor(True, device=device)
        for lo in range(0, nitems, config['item_block']):
            items = np.arange(lo, min(nitems, lo + config['item_block']), dtype=np.int64)
            scores = score_block_tensor(model, users[start:end], items, feature_rows[start:end], features, device, config['pair_batch'])
            finite &= torch.isfinite(scores).all()
            ids, score = device_topk(scores, items)
            best_ids, best_scores = device_topk(torch.cat([best_scores, score], dim=1), torch.cat([best_ids, ids], dim=1))
        if not bool(finite):
            raise ValueError('Neural scorer produced NaN/Inf')
        best_ids, best_scores = best_ids.cpu().numpy(), best_scores.cpu().numpy()
        output[start:end, :best_ids.shape[1]] = best_ids
        values[start:end, :best_scores.shape[1]] = best_scores
    return output, values


def predictions(model, fold, features, root, signature, config, only_warm=False):
    root = Path(root); root.mkdir(parents=True, exist_ok=True)
    positions = np.flatnonzero((fold.user_indices >= 0) & (fold.warm if only_warm else True))
    output = np.full((fold.nqueries, 20), -1, np.int32)
    feature_rows = features.d2['query_feature_rows'] if features.d2 else np.zeros(fold.nqueries, np.int64)
    digest = rec.fingerprint(dict(signature=signature, positions=rec.array_hash(positions), nitems=fold.nitems,
                                  dtype='float32', devices=config['score_devices'],
                                  feature_cache=config.get('score_feature_cache', True),
                                  gpu_topk='float32_int64_lexicographic_v1'))
    shards = [(number, start, min(len(positions), start + config['score_shard_queries']))
              for number, start in enumerate(range(0, len(positions), config['score_shard_queries']))]
    # CPU template avoids retaining a second full train model/optimizer on a scoring GPU.
    devices = config['score_devices']
    def worker(rank):
        device = devices[rank]
        local = local_features = None
        try:
            for number, start, end in shards[rank::len(devices)]:
                query_positions = positions[start:end]
                path = root / f'part-{number:06d}.npz'
                meta_path = path.with_suffix('.json')
                if path.exists() and meta_path.exists():
                    meta = rec.read_json(meta_path)
                    if meta.get('signature') != digest or rec.sha256(path) != meta['sha256']:
                        raise ValueError('Neural scoring checkpoint mismatch')
                    with np.load(path, allow_pickle=False) as saved:
                        if not np.array_equal(saved['query_positions'], query_positions) or saved['items'].shape != (len(query_positions), 20):
                            raise ValueError('Wrong scoring query shard')
                        selected = saved['items'].copy()
                else:
                    if local is None:
                        local = copy.deepcopy(model).to(device).eval()
                        if isinstance(local, TwoTowerNetwork):
                            local_features = TowerScoringFeatures(local, features, fold.nitems, device)
                        else:
                            local_features = (ScoringFeatures(local, features, feature_rows[positions], device)
                                if config.get('score_feature_cache', True) else features)
                    started = time.monotonic()
                    selected, scores = rank_neural(local, fold.user_indices[query_positions].astype(np.int64),
                        feature_rows[query_positions], local_features, fold.nitems, config, device)
                    rec.disk_guard(root, selected.nbytes + scores.nbytes, config['min_free_gib'])
                    temporary = path.with_suffix('.npz.tmp')
                    with temporary.open('wb') as stream:
                        np.savez(stream, query_positions=query_positions, items=selected, scores=scores)
                    temporary.replace(path)
                    seconds = time.monotonic() - started
                    rec.save_json(meta_path, dict(signature=digest, sha256=rec.sha256(path), queries=len(query_positions),
                        seconds=seconds, pairs_per_second=len(query_positions) * fold.nitems / max(seconds, 1e-9), device=device))
                output[query_positions] = selected
                print('Scored/reused', fold.name, end, '/', len(positions), 'queries', flush=True)
        finally:
            del local, local_features
    with ThreadPoolExecutor(max_workers=len(devices)) as executor:
        list(executor.map(worker, range(len(devices))))
    rec.save_json(root / 'manifest.json', dict(status='complete', signature=digest, queries=len(positions), shards=len(shards)))
    return output


def checkpoint_state(model, optimizer, epoch, signature, trial_state):
    # Tensor/primitive-only format permits weights_only=True on all restore paths.
    return dict(signature=signature, epoch=epoch, model=model.state_dict(), optimizer=optimizer.state_dict(),
                torch_rng=torch.get_rng_state(), cuda_rng=torch.cuda.get_rng_state_all() if torch.cuda.is_available() else [],
                trial_state=copy.deepcopy(trial_state))


def training_plan(config):
    plan = dict({key: config[key] for key in TRAINING_SIGNATURE_KEYS}, negative_sampler=SAMPLER_VERSION)
    if config['algorithm'] == 'two_tower':
        plan.update(tower_output_dim=config['tower_output_dim'], architecture='independent_mlp_dot_v1', loss='bce_mean')
    return plan


def early_stop_state(config):
    return dict(enabled=config['early_stopping'], monitor='warm_full_ndcg_at10',
        start_epoch=config['early_stop_start_epoch'], patience=config['early_stop_patience'],
        min_delta=float(config['early_stop_min_delta']), significant_best=None,
        bad_checks=0, last_checked_epoch=None, last_metric=None, stop_requested=False)


def update_early_stopping(control, epoch, ndcg):
    """Count completed validation checks, independently of strict best-checkpoint selection."""
    if not np.isfinite(ndcg):
        raise ValueError('Non-finite early-stopping metric')
    if control['last_checked_epoch'] is not None and epoch <= control['last_checked_epoch']:
        if epoch == control['last_checked_epoch'] and ndcg == control['last_metric']:
            return  # Idempotent after a completed-check resume.
        raise ValueError('Early-stopping checks must advance in epoch order')
    if control['stop_requested']:
        raise ValueError('Cannot update a stopped trial with a later validation')
    reference = control['significant_best']
    improved = reference is None or (ndcg > reference and ndcg >= reference + control['min_delta'])
    if improved:
        control['significant_best'] = float(ndcg)
        control['bad_checks'] = 0
    elif not control['enabled'] or epoch < control['start_epoch']:
        control['bad_checks'] = 0
    else:
        control['bad_checks'] += 1
    control.update(last_checked_epoch=epoch, last_metric=float(ndcg))
    control['stop_requested'] = bool(control['enabled'] and epoch >= control['start_epoch']
                                     and control['bad_checks'] >= control['patience'])


def train_epoch(model, optimizer, fold, examples, features, epoch, config):
    model.train()
    total = positive = 0
    loss_fn = nn.BCEWithLogitsLoss()
    device = config['train_device']
    cuda = str(device).startswith('cuda:')
    if cuda:
        torch.cuda.synchronize(device)
    started = time.monotonic()
    loss_sum = torch.zeros((), device=device)
    prepare_seconds = wait_seconds = step_seconds = 0.
    batch_number = 0
    with closing(training_batches(examples, fold.binary, features, epoch, config)) as batches:
        while True:
            waiting = time.monotonic()
            batch = next(batches, None)
            wait_seconds += time.monotonic() - waiting
            if batch is None:
                break
            batch_number += 1
            tensors, positives, preparation = batch
            prepare_seconds += preparation
            stepping = time.monotonic()
            tensors = {key: value.to(device, non_blocking=cuda) for key, value in tensors.items()}
            users, items, labels = (tensors.pop(key) for key in ('users', 'items', 'labels'))
            optimizer.zero_grad(set_to_none=True)
            logits = model(users, items, **tensors)
            loss = loss_fn(logits, labels)
            loss.backward()
            # One host synchronization before update, instead of one per parameter plus loss.item().
            finite = [torch.isfinite(loss)] + [torch.isfinite(p.grad).all() for p in model.parameters() if p.grad is not None]
            if not bool(torch.stack(finite).all()):
                raise ValueError('Non-finite train loss or neural gradient')
            optimizer.step()
            loss_sum += loss.detach() * len(labels)
            total += len(labels); positive += positives
            step_seconds += time.monotonic() - stepping
            if batch_number == 1 or batch_number % 100 == 0:
                print('Train epoch', epoch, 'positives', positive, '/', len(examples), 'BCE', round(float(loss_sum) / total, 6), flush=True)
    if positive != len(examples):
        raise ValueError('Training dropped positive anchors')
    if cuda:
        torch.cuda.synchronize(device)
    elapsed = time.monotonic() - started
    return dict(loss=float(loss_sum) / total, pairs=total, positives=positive, negatives=total - positive,
        optimizer_steps=batch_number, seconds=elapsed, pairs_per_second=total / max(elapsed, 1e-9),
        sample_prepare_cpu_seconds=prepare_seconds, waiting_for_batch_seconds=wait_seconds,
        step_host_seconds=step_seconds,
        timing_policy='CPU prepare overlaps GPU; step_host includes enqueue/finite-check waits, not isolated GPU kernel time')


def parameter_budget(nusers, nitems, variant, parameters, config):
    dim = parameters['dim']
    count = (nusers + nitems) * dim * (2 if variant == 'NCF_ID' else 1)
    width = 2 * dim
    if variant != 'NCF_ID':
        count += nusers + nitems + 1
    if variant in ('D1', 'D2'):
        count += 385 * config['projection_dim']; width += config['projection_dim'] + 1
    if variant == 'D2':
        count += 385 * config['projection_dim']; width += config['projection_dim'] + len(FEATURE_SCHEMA)
    for hidden in parameters['hidden']:
        count += (width + 1) * hidden; width = hidden
    count += width + (dim if variant == 'NCF_ID' else 0) + 1
    if variant in TOWER_VARIANTS:
        count = (nusers + nitems) * dim
        widths = [dim, dim]
        if variant in ('TT1', 'TT2'):
            count += 385 * config['projection_dim']
            widths[1] += config['projection_dim'] + 1
        if variant == 'TT2':
            count += 385 * config['projection_dim']
            widths[0] += config['projection_dim'] + len(FEATURE_SCHEMA)
        for width in widths:
            for hidden in parameters['hidden'] + [config['tower_output_dim']]:
                count += (width + 1) * hidden
                width = hidden
    import psutil
    free = psutil.virtual_memory().available if config['train_device'] == 'cpu' else torch.cuda.mem_get_info(config['train_device'])[0]
    minimum = count * 16
    estimated = minimum + (count * 4 if config['adam_foreach'] else 0)
    print('Parameter budget:', count, '; minimum FP32 Adam bytes:', estimated, '; available:', free, flush=True)
    if estimated > free * .8:
        raise RuntimeError('Model/Adam budget exceeds 80% free memory before activations; choose a smaller locked trial dimension/device.')
    return dict(parameters=count, minimum_adam_bytes=minimum, estimated_train_bytes=estimated,
                adam_foreach=config['adam_foreach'], available_bytes=free)


def fit_trial(fold, examples, features, variant, parameters, root, source, config, epochs=None):
    root.mkdir(parents=True, exist_ok=True)
    if epochs is not None and (type(epochs) is not int or not 1 <= epochs <= config['max_epochs']):
        raise ValueError('Refit epochs must be a locked validation selection within the training budget')
    signature = rec.fingerprint(dict(source=source, fold=fold.name, parameters=parameters, variant=variant,
                                     refit_epochs=epochs, training_plan=training_plan(config)))
    metadata = root / 'trial.json'
    state = rec.read_json(metadata) if metadata.exists() else dict(signature=signature, status='running',
        epochs=[], evaluations=[], best=None, early_stopping=early_stop_state(config))
    if state['signature'] != signature:
        raise ValueError('Trial metadata mismatch')
    if state.get('status') == 'complete' and state.get('model_pruned'):
        return state, signature
    last = root / 'last.pt'
    restored = read_checkpoint(last, signature) if last.exists() else None
    if restored is not None:
        # The atomic model checkpoint commits epoch/evaluation/control together. The JSON
        # mirror can lag if the process stopped after committing the checkpoint.
        state = restored.get('trial_state')
        if not state or state.get('signature') != signature or 'early_stopping' not in state:
            raise ValueError('Checkpoint lacks matching compact training state')
    if state.get('status') == 'complete':
        if restored is None or restored['epoch'] != state['stopped_epoch']:
            raise ValueError('Completed trial missing its matching final checkpoint')
        rec.save_json(metadata, state)
        return state, signature
    budget = parameter_budget(fold.nusers, fold.nitems, variant, parameters, config)
    seed_model(config['seed'])
    model = make_model(fold.nusers, fold.nitems, variant, parameters, config).to(config['train_device'])
    if sum(p.numel() for p in model.parameters()) != budget['parameters']:
        raise AssertionError('Parameter estimator out of sync')
    optimizer = torch.optim.Adam(model.parameters(), lr=parameters['lr'], weight_decay=parameters['weight_decay'],
                                 foreach=config['adam_foreach'])
    completed = 0
    if restored is not None:
        model.load_state_dict(restored['model']); optimizer.load_state_dict(restored['optimizer'])
        completed = restored['epoch']
        if not state['epochs'] or state['epochs'][-1]['epoch'] != completed:
            raise ValueError('Checkpoint epoch and loss history disagree')
        torch.set_rng_state(restored['torch_rng'])
        if restored['cuda_rng']:
            torch.cuda.set_rng_state_all(restored['cuda_rng'])
        del restored
    limit = epochs if epochs is not None else config['max_epochs']
    if completed > limit:
        raise ValueError('Checkpoint exceeds requested epoch budget')
    for epoch in range(max(1, completed), limit + 1):
        if epochs is None and state['early_stopping']['stop_requested']:
            break
        if epoch > completed:
            with rec.Monitor(list(set(config['score_devices'] + [config['train_device']]))) as monitor:
                measures = train_epoch(model, optimizer, fold, examples, features, epoch, config)
            state['epochs'] = [e for e in state['epochs'] if e['epoch'] != epoch]
            state['epochs'].append(dict(epoch=epoch, **measures, resources=monitor.result))
            atomic_checkpoint(last, checkpoint_state(model, optimizer, epoch, signature, state), config)
            completed = epoch
            rec.save_json(metadata, state)
            print(variant, parameters['name'], 'epoch', epoch, measures, flush=True)
        if epochs is None and epoch in config['eval_epochs'] and not any(e['epoch'] == epoch for e in state['evaluations']):
            model.zero_grad(set_to_none=True)
            model.cpu()
            with rec.Monitor(config['score_devices']) as scoring:
                native = predictions(model, fold, features, root / f'epoch-{epoch}-ranking',
                    dict(trial=signature, model_sha256=rec.sha256(last)), config, only_warm=True)
            measures = rec.query_metrics(fold, native)
            if not fold.warm.any():
                raise ValueError('No warm-full validation queries')
            result = dict(epoch=epoch, trial=parameters['name'], parameters=parameters,
                ndcg_at10=float(measures['ndcg@10'][fold.warm].mean()), recall_at10=float(measures['recall@10'][fold.warm].mean()),
                parameter_count=budget['parameters'], scoring_resources=scoring.result)
            if state['best'] is None or selection_score(result) > selection_score(state['best']):
                # Best is inference-only; the last checkpoint retains optimizer/RNG for epoch resume.
                atomic_checkpoint(root / 'best.pt', dict(signature=signature, epoch=epoch, model=model.state_dict()), config)
                state['best'] = result
            update_early_stopping(state['early_stopping'], epoch, result['ndcg_at10'])
            result['bad_checks'] = state['early_stopping']['bad_checks']
            result['significant_best_ndcg'] = state['early_stopping']['significant_best']
            state['evaluations'].append(result)
            # Commit before updating the JSON mirror so resume never double-counts patience.
            atomic_checkpoint(last, checkpoint_state(model, optimizer, epoch, signature, state), config)
            rec.save_json(metadata, state)
            print(variant, parameters['name'], 'validation epoch', epoch, 'NDCG@10', result['ndcg_at10'],
                  'bad checks', state['early_stopping']['bad_checks'], '/', config['early_stop_patience'], flush=True)
            if state['early_stopping']['stop_requested']:
                break
            model.to(config['train_device'])
    if not completed or (epochs is None and state['best'] is None):
        raise ValueError('Trial completed without training or validation selection')
    rec.save_json(root / 'fit_budget.json', budget)
    pd.DataFrame([{k: v for k, v in e.items() if k != 'resources'} for e in state['epochs']]).to_csv(root / 'loss.csv', index=False)
    if state['evaluations']:
        pd.DataFrame([{k: v for k, v in e.items() if k not in ('parameters', 'scoring_resources')}
                      for e in state['evaluations']]).to_csv(root / 'validation.csv', index=False)
    reason = ('refit_selected_epoch' if epochs is not None else
              'ranking_plateau' if state['early_stopping']['stop_requested'] else 'max_epochs')
    recent = state['evaluations'][-2:]
    improving_at_cap = bool(reason == 'max_epochs' and len(recent) == 2
                            and recent[-1]['ndcg_at10'] > recent[-2]['ndcg_at10'])
    state.update(status='complete', optimizer_retained=False, stop_reason=reason, stopped_epoch=completed,
                 selected_epoch=epochs if epochs is not None else state['best']['epoch'],
                 budget_exhausted_while_improving=improving_at_cap)
    # A finished, locked trial/refit never trains further. Release Adam moments before upload;
    # interrupted epochs retain full optimizer/RNG in last.pt until this completion boundary.
    model.zero_grad(set_to_none=True)
    model.cpu()
    atomic_checkpoint(last, dict(signature=signature, epoch=completed, model=model.state_dict(),
                                 trial_state=copy.deepcopy(state)), config)
    rec.save_json(metadata, state)
    print(variant, parameters['name'], 'finished:', reason, '; trained epoch', completed,
          '; selected epoch', state['selected_epoch'], flush=True)
    if improving_at_cap:
        print('Epoch budget exhausted while validation is still improving; convergence is not established.', flush=True)
    del model, optimizer
    gc.collect()
    if torch.cuda.is_available():
        torch.cuda.empty_cache()
    return state, signature


def prune_losing_trials(root, results):
    """Delete only generated model payloads of completed, strictly non-selected trials."""
    winner = max(results, key=selection_score)['trial']
    trials_root = (root / 'trials').resolve()
    for result in results:
        if result['trial'] == winner:
            continue
        directory = rec.child(trials_root, result['trial'])
        state = rec.read_json(directory / 'trial.json')
        if state.get('status') != 'complete':
            raise ValueError('Cannot prune an incomplete trial')
        for name in ('last.pt', 'last.json', 'best.pt', 'best.json'):
            (directory / name).unlink(missing_ok=True)
        state['model_pruned'] = True
        rec.save_json(directory / 'trial.json', state)


def compact_d2(root, config, queries):
    """Retain query features/scaler for inspection; training prefix arrays are transient after scoring."""
    state = rec.read_json(root / 'features.json')
    rec.save_json(root / 'features.json', dict(state, status='compacting'))
    rows = np.load(root / 'query_feature_rows.npy')
    rec.disk_guard(root, len(rows) * (384 + len(FEATURE_SCHEMA)) * 4, config['min_free_gib'])
    for name in ('stats', 'reviews'):
        original = np.load(root / (name + '.npy'), mmap_mode='r')
        target = np.lib.format.open_memmap(root / ('query_' + name + '.npy'), mode='w+',
                                           dtype=np.float32, shape=(len(rows), original.shape[1]))
        for start in range(0, len(rows), 8192):
            target[start:start + 8192] = original[rows[start:start + 8192]]
        target.flush()
        del original, target
    gc.collect()
    for name in ('stats.npy', 'reviews.npy', 'train_feature_rows.npy', 'query_feature_rows.npy'):
        (root / name).unlink()
    state['cache_compacted'] = True
    state['status'] = 'complete'
    queries[['query_id', 'user_idx', 'prediction_time']].to_parquet(root / 'query_keys.parquet', index=False)
    state['files'] = {p.name: rec.sha256(p) for p in root.iterdir() if p.suffix in ('.npy', '.parquet')}
    rec.save_json(root / 'features.json', state)


def selection_score(result):
    return (result['ndcg_at10'], result['recall_at10'], -result['parameter_count'], -result['epoch'])


def phase_valid(root, phase, source):
    path = root / phase / 'phase_complete.json'
    if not path.exists():
        return False
    state = rec.read_json(path)
    return (state.get('status') == 'complete' and state.get('source') == source and bool(state.get('artifacts'))
            and all(rec.child(root, p).is_file() and rec.sha256(rec.child(root, p)) == digest for p, digest in state['artifacts'].items()))


def run(config):
    validate_config(config)
    phase, variant = config['phase'], config['variant']
    if phase not in rec.BOUNDS or variant not in config['variants'] or config['category'] not in config['categories']:
        raise ValueError('Invalid phase/variant/category')
    gold = rec.Gold(rec.discover_gold(config.get('gold_root')), config['category'])
    embedding = rec.discover_embedding(gold, config.get('embedding_root'), config.get('embedding_bundle')) if variant in ('D1', 'D2', 'TT1', 'TT2') else None
    source = dict(gold=gold.signature, variant=variant, algorithm=config['algorithm'], protocol=rec.PROTOCOL,
        shared_code=rec.sha256(rec.__file__), neural_code=rec.sha256(__file__), versions=rec.versions(),
        embedding=rec.sha256(embedding) if embedding else None,
        training=training_plan(config),
        dataset_versions=config.get('dataset_versions'))
    parent = Path(config['output_root']).resolve(); parent.mkdir(parents=True, exist_ok=True)
    root = parent / (variant.lower() + '_' + config['category'] + '_' + rec.fingerprint(source)[:16])
    rec.resume_output(root, Path(config['resume_root']) / root.name if config.get('resume_root') else None, config['min_free_gib'])
    root.mkdir(parents=True, exist_ok=True)
    if (root / 'run.json').exists() and rec.read_json(root / 'run.json')['signature'] != source:
        raise ValueError('Neural run source mismatch')
    if phase == 'test' and not phase_valid(root, 'validation', source):
        raise ValueError('Run/restore matching complete validation first; test cannot select configuration')
    if phase_valid(root, phase, source):
        print('Reused complete neural phase:', variant, config['category'], phase, flush=True)
        return root
    phase_root = root / phase; phase_root.mkdir(exist_ok=True)
    rec.save_json(root / 'run.json', dict(status='running', signature=source, phase=phase, config=config))
    try:
        devices = list(set(config['score_devices'] + [config['train_device']]))
        rec.save_json(phase_root / 'runtime.json', dict(python=os.sys.version, torch=torch.__version__,
            torch_cuda=torch.version.cuda, cpu_threads=config['cpu_threads'], train_device=config['train_device'],
            score_devices=config['score_devices'], dtype='float32', deterministic_algorithms=True,
            gpus=[dict(device=d, name=torch.cuda.get_device_name(d), total_memory=torch.cuda.get_device_properties(d).total_memory)
                  for d in devices if d.startswith('cuda:')]))
        with rec.Monitor(devices) as loading:
            fold = gold.fold(phase)
            examples = load_examples(gold, fold)
        rec.save_json(phase_root / 'load_resources.json', loading.result)
        fold.copy_mappings(phase_root)
        with rec.Monitor(devices) as preparing:
            item_vectors = item_valid = d2 = None
            if embedding:
                item_vectors, item_valid, info = rec.load_item_vectors(gold, fold, embedding.parent, embedding.parent.name)
                rec.disk_guard(root, item_vectors.nbytes, config['min_free_gib'])
                rec.save_array(phase_root / 'item_vectors.npy', item_vectors)
                rec.save_array(phase_root / 'item_valid.npy', item_valid)
                rec.save_json(phase_root / 'item_features.json', info)
            if variant in ('D2', 'TT2'):
                d2 = build_d2(gold, fold, examples, embedding, phase_root / 'features', config)
            features = Features(variant, item_vectors, item_valid, d2)
        rec.save_json(phase_root / 'feature_resources.json', preparing.result)
        best_path = root / 'best_config.json'
        if phase == 'validation':
            results = []
            for parameters in config['trials']:
                trial_root = root / 'trials' / parameters['name']
                trial, trial_signature = fit_trial(fold, examples, features, variant, parameters, trial_root, source, config)
                results.append(dict(trial['best'], stopped_epoch=trial['stopped_epoch'], selected_epoch=trial['selected_epoch'],
                                    stop_reason=trial['stop_reason'],
                                    checkpoint=str((trial_root / 'best.pt').relative_to(root).as_posix()),
                                    checkpoint_signature=trial_signature))
                pd.DataFrame([{**r, 'parameters': json.dumps(r['parameters'], sort_keys=True)} for r in results]).to_csv(root / 'trials.csv', index=False)
                prune_losing_trials(root, results)
            best = max(results, key=selection_score)
            best['checkpoint_sha256'] = rec.sha256(root / best['checkpoint'])
            rec.save_json(best_path, dict(status='locked', source=source, best=best))
        else:
            selection = rec.read_json(best_path)
            if selection.get('status') != 'locked' or selection.get('source') != source:
                raise ValueError('No matching locked validation configuration')
            best = selection['best']
            if rec.sha256(root / best['checkpoint']) != best['checkpoint_sha256']:
                raise ValueError('Best validation model changed')
            _, refit_signature = fit_trial(fold, examples, features, variant, best['parameters'],
                phase_root / 'model', source, config, epochs=best['epoch'])
        model_path = root / best['checkpoint'] if phase == 'validation' else phase_root / 'model/last.pt'
        checkpoint_signature = best['checkpoint_signature'] if phase == 'validation' else refit_signature
        restored = read_checkpoint(model_path, checkpoint_signature)
        model = make_model(fold.nusers, fold.nitems, variant, best['parameters'], config)
        model.load_state_dict(restored['model']); del restored
        with rec.Monitor(config['score_devices']) as scoring:
            native = predictions(model, fold, features, phase_root / 'ranking',
                dict(source=source, model=rec.sha256(model_path)), config)
        rec.save_json(phase_root / 'score_resources.json', scoring.result)
        if isinstance(model, TwoTowerNetwork):
            with rec.Monitor(['cpu']) as exporting:
                rec.disk_guard(phase_root, fold.nitems * model.output_dim * 4, config['min_free_gib'])
                cache = TowerScoringFeatures(model.eval(), features, fold.nitems, 'cpu')
                path = phase_root / 'item_output_vectors.npy'
                rec.save_array(path, cache.items.numpy())
                rec.save_json(phase_root / 'tower_export.json', dict(status='complete',
                    model_sha256=rec.sha256(model_path), vector_sha256=rec.sha256(path),
                    rows=fold.nitems, dimension=model.output_dim, order='fold item_idx',
                    score='raw dot product; no L2 output normalization'))
                del cache
            rec.save_json(phase_root / 'tower_export_resources.json', exporting.result)
        with rec.Monitor(['cpu']) as evaluating:
            rows = rec.evaluate(fold, native, phase_root / 'evaluation', variant.lower(), config)
        rec.save_json(phase_root / 'metric_resources.json', evaluating.result)
        del model, features, d2
        gc.collect()
        if variant in ('D2', 'TT2'):
            compact_d2(phase_root / 'features', config, fold.queries)
        artifacts = {p.relative_to(root).as_posix(): rec.sha256(p) for p in phase_root.rglob('*')
                     if p.is_file() and p.name != 'phase_complete.json'}
        if phase == 'validation':
            for p in [best_path, root / best['checkpoint'], (root / best['checkpoint']).with_suffix('.json')]:
                artifacts[p.relative_to(root).as_posix()] = rec.sha256(p)
            rec.save_json(root / 'validation_complete.json', dict(status='complete', source=source))
        rec.save_json(phase_root / 'phase_complete.json', dict(status='complete', source=source, fold=phase, artifacts=artifacts))
        rec.save_json(root / 'run.json', dict(status='complete', signature=source, phase=phase, config=config))
        print(pd.DataFrame(rows).query("cohort in ['all_queries','warm_full']").to_string(index=False), flush=True)
        return root
    except BaseException as error:
        rec.save_json(root / 'run.json', dict(status='failed', signature=source, phase=phase, config=config,
                                            error_type=type(error).__name__, error=str(error)))
        raise


def run_suite(config):
    validate_config(config)
    publish = config.get('publish_results', True)
    if publish and config['phases'] != ['validation', 'test']:
        raise ValueError('Private Dataset publishing requires both phases; use Save Version/Output for validation-only runs')
    jobs = [(category, variant) for category in config['categories'] for variant in config['variants']]
    handles, remote = config.get('result_datasets', {}), {}
    if publish:
        selected = [handles.get(variant + '/' + category) for category, variant in jobs]
        if any(not h for h in selected) or len(set(selected)) != len(jobs):
            raise ValueError('Use separate result Dataset handles keyed by VARIANT/Category')
        rec.configure_kaggle_auth()
        remote = {handle: rec.result_dataset_private(handle) for handle in selected}
    # Fail missing inputs before a long first training run.
    for category in config['categories']:
        gold = rec.Gold(rec.discover_gold(config.get('gold_root')), category)
        for variant in config['variants']:
            needed = ['validation/train_examples', 'test/train_examples']
            if variant in ('D1', 'D2', 'TT1', 'TT2'):
                rec.discover_embedding(gold, config.get('embedding_root'), config.get('embedding_bundle'))
            if variant in ('D2', 'TT2'):
                needed += ['review_text_refs', 'history_ratings'] + [phase + '/history_events' for phase in config['phases']]
            missing = [table for table in needed if table not in gold.info['tables']]
            if missing:
                raise ValueError('Missing Gold tables: ' + ', '.join(missing))
    output = Path(config['output_root']); output.mkdir(parents=True, exist_ok=True)
    summary, prepared = [], []
    for category, variant in jobs:
        current = dict(config, category=category, variant=variant)
        handle = handles.get(variant + '/' + category)
        previous = None
        if publish and remote[handle] and config.get('auto_resume', True) and not current.get('resume_root'):
            import kagglehub
            restored = Path(kagglehub.dataset_download(handle))
            previous = rec.read_json(restored / 'result_bundle.json')
            if previous.get('format') != 'gold_recommendation_results_v1' or previous.get('handle') != handle:
                raise ValueError('Unexpected neural result Dataset layout')
            current['resume_root'] = str(restored / 'recommendation_results')
        prepared.append((category, variant, current, handle, previous))
    # Lock every selected Two-Tower validation before exposing any test scores.
    if config['algorithm'] == 'two_tower' and config['phases'] == ['validation', 'test']:
        for category, variant, current, handle, previous in prepared:
            run(dict(current, phase='validation'))
    for category, variant, current, handle, previous in prepared:
        for phase in config['phases']:
            print('\n===', category, variant, phase, '===', flush=True)
            root = run(dict(current, phase=phase))
        receipt = rec.publish_results(root, handle, config['min_free_gib'], previous) if publish else None
        summary.append(dict(category=category, variant=variant, run_root=str(root), dataset=handle, upload=receipt))
        rec.save_json(output / (config['algorithm'] + '_suite_results.json'), summary)
    return summary


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    args = parser.parse_args()
    run_suite(rec.read_json(args.config))
