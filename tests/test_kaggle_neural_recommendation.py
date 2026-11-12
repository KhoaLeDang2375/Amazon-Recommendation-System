"""CPU training and restore on actual Parquet: neural architecture, time leakage and scoring."""
import json
from pathlib import Path
import shutil
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import torch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'notebooks'))
sys.path.insert(0, str(ROOT / 'tests'))
import kaggle_recommendation as rec
import kaggle_neural_recommendation as neural
from test_kaggle_recommendation import fixture


def neural_fixture(root):
    gold, bundle = fixture(root)
    category = gold / 'All_Beauty'
    manifest = rec.read_json(category / 'category_manifest.json')
    def write(name, frame):
        path = category / name / 'part.parquet'; path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pandas(frame, preserve_index=False), path)
        manifest['tables'][name] = dict(rows=len(frame), files=[dict(path=path.relative_to(category).as_posix(),
            rows=len(frame), bytes=path.stat().st_size, sha256=rec.sha256(path))])
    ids = pq.read_table(bundle / 'texts/part.parquet', columns=['text_id']).column(0).to_pylist()
    times = pd.to_datetime(['2020-01-01', '2020-01-02', '2020-01-01', '2020-01-02', '2020-01-01'])
    for fold in rec.BOUNDS:
        pairs = pq.read_table(category / fold / 'train_pairs').to_pandas()
        pairs['first_pair_time'] = times
        write(fold + '/train_pairs', pairs)
        examples = pairs.copy()
        examples['example_id'] = ['e' + str(i) for i in range(len(pairs))]
        examples['prediction_time'] = times
        write(fold + '/train_examples', examples)
        write(fold + '/history_events', pd.DataFrame(dict(record_hash=['r' + str(i) for i in range(5)],
            user_id=['u0', 'u0', 'u1', 'u1', 'u2'], event_time=times)))
    write('review_text_refs', pd.DataFrame(dict(record_hash=['r' + str(i) for i in range(5)],
        user_id=['u0', 'u0', 'u1', 'u1', 'u2'], event_time=times,
        text_id=[ids[0], ids[0], None, ids[1], ids[2]], has_text=[True, True, False, True, True])))
    write('history_ratings', pd.DataFrame(dict(record_hash=['r' + str(i) for i in range(5)], rating=[1., 5., np.nan, 4., 2.])))
    rec.save_json(category / 'category_manifest.json', manifest)
    dataset = rec.read_json(gold / 'dataset_manifest.json')
    dataset['categories']['All_Beauty'] = rec.sha256(category / 'category_manifest.json')
    rec.save_json(gold / 'dataset_manifest.json', dataset)
    return gold, bundle


def configuration(root, gold, bundle, variant='NCF_ID'):
    config = neural.default_config('ncf' if variant == 'NCF_ID' else 'deepfm')
    config.update(variants=[variant], categories=['All_Beauty'], category='All_Beauty', variant=variant,
        phase='validation', gold_root=str(gold), embedding_root=str(bundle), output_root=str(root / variant),
        train_device='cpu', score_devices=['cpu'], cpu_threads=1, train_batch=10,
        query_batch=2, item_block=2, pair_batch=3, score_shard_queries=2, min_free_gib=0,
        bootstrap_samples=10, max_epochs=2, eval_epochs=[1, 2], publish_results=False, early_stopping=False,
        trials=[dict(name='N0', dim=2, hidden=[4], lr=.01, weight_decay=.00001)])
    return config


class NeuralChecks(unittest.TestCase):
    def workspace(self):
        work = ROOT / 'outputs/validation/neural-fixtures'; work.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(dir=work, prefix='case-')
        self.addCleanup(temporary.cleanup)
        return Path(temporary.name).resolve()

    def test_compact_defaults_parameter_counts_and_control_validation(self):
        config = neural.default_config('deepfm')
        config.update(train_device='cpu', score_devices=['cpu'], cpu_threads=1)
        neural.validate_config(config)
        self.assertEqual(config['training_profile'], 'compact_full_users')
        self.assertEqual(config['max_epochs'], 20)
        self.assertEqual(config['eval_epochs'], [1, 3, 6, 9, 12, 15, 18, 20])
        self.assertEqual(len(config['trials']), 1)
        self.assertEqual(config['trials'][0]['dim'], 8)
        self.assertEqual(config['trials'][0]['hidden'], [32, 16])
        self.assertEqual(config['projection_dim'], 16)
        expected = dict(NCF_ID=1209, D0=1153, D1=7857, D2=14753)
        for variant, count in expected.items():
            model = neural.PairNetwork(3, 4, variant, config['trials'][0], config['projection_dim'])
            self.assertEqual(sum(p.numel() for p in model.parameters()), count)
            self.assertEqual(neural.parameter_budget(3, 4, variant, config['trials'][0], config)['parameters'], count)
        for changes in [dict(early_stop_patience=0), dict(early_stop_min_delta=float('nan')),
                        dict(early_stop_min_delta='x'), dict(early_stopping=1),
                        dict(early_stop_start_epoch=21), dict(eval_epochs=[1, 3])]:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                neural.validate_config(dict(config, **changes))

    def test_early_stopping_warmup_tiny_gains_ties_and_duplicate_checks(self):
        config = neural.default_config('ncf')
        control = neural.early_stop_state(config)
        for epoch, value in [(1, .1), (3, .11)]:
            neural.update_early_stopping(control, epoch, value)
            self.assertFalse(control['stop_requested'])
            self.assertEqual(control['bad_checks'], 0)
        neural.update_early_stopping(control, 6, .109)
        self.assertEqual(control['bad_checks'], 1)
        neural.update_early_stopping(control, 9, .108)
        self.assertTrue(control['stop_requested'])
        self.assertEqual(control['bad_checks'], 2)
        neural.update_early_stopping(control, 9, .108)
        self.assertEqual(control['bad_checks'], 2)
        with self.assertRaisesRegex(ValueError, 'advance'):
            neural.update_early_stopping(control, 6, .109)
        with self.assertRaisesRegex(ValueError, 'Non-finite'):
            neural.update_early_stopping(control, 12, float('nan'))
        small = neural.early_stop_state(dict(config, early_stop_start_epoch=1,
                                            early_stop_patience=3, early_stop_min_delta=.01))
        for epoch, value in [(1, .1), (2, .104), (3, .108), (4, .111)]:
            neural.update_early_stopping(small, epoch, value)
        self.assertEqual(small['bad_checks'], 0)  # Tiny gains accumulate past the significant reference.
        self.assertEqual(small['significant_best'], .111)
        for epoch in (5, 6, 7): neural.update_early_stopping(small, epoch, .111)
        self.assertTrue(small['stop_requested'])
        disabled = neural.early_stop_state(dict(config, early_stopping=False))
        for epoch in (1, 3, 6, 9, 12, 15, 18, 20):
            neural.update_early_stopping(disabled, epoch, .1)
        self.assertFalse(disabled['stop_requested'])
        self.assertEqual(disabled['bad_checks'], 0)

    def test_early_stop_real_training_preserves_best_and_refit_epoch(self):
        root = self.workspace(); gold_path, bundle = neural_fixture(root)
        config = configuration(root, gold_path, bundle)
        config.update(early_stopping=True, early_stop_start_epoch=2, early_stop_patience=2,
                      max_epochs=6, eval_epochs=[1, 2, 3, 4, 5, 6])
        neural.validate_config(config)
        gold = rec.Gold(gold_path, 'All_Beauty'); fold = gold.fold('validation')
        examples = neural.load_examples(gold, fold); features = neural.Features('NCF_ID')
        params = config['trials'][0]
        values = iter([.6, .5, .4])
        def metrics(*args):
            value = next(values)
            return {'ndcg@10': np.full(4, value), 'recall@10': np.ones(4)}
        with mock.patch.object(neural, 'predictions', return_value=np.zeros((4, 20), np.int32)), \
             mock.patch.object(rec, 'query_metrics', side_effect=metrics):
            state, signature = neural.fit_trial(fold, examples, features, 'NCF_ID', params, root / 'trial', {}, config)
        self.assertEqual(state['stop_reason'], 'ranking_plateau')
        self.assertEqual(state['stopped_epoch'], 3)
        self.assertEqual(state['selected_epoch'], 1)
        self.assertEqual(len(state['epochs']), 3)
        self.assertEqual(neural.read_checkpoint(root / 'trial/last.pt', signature)['epoch'], 3)
        self.assertEqual(neural.read_checkpoint(root / 'trial/best.pt', signature)['epoch'], 1)
        refit = gold.fold('test'); refit_examples = neural.load_examples(gold, refit)
        with mock.patch.object(neural, 'predictions', side_effect=AssertionError('Refit must not validate')), \
             mock.patch.object(rec, 'query_metrics', side_effect=AssertionError('Refit must not select')):
            final, refit_sig = neural.fit_trial(refit, refit_examples, features, 'NCF_ID', params,
                root / 'refit', {}, config, epochs=state['selected_epoch'])
        self.assertEqual(final['stop_reason'], 'refit_selected_epoch')
        self.assertEqual(final['stopped_epoch'], 1)
        self.assertFalse(final['evaluations'])
        first_epoch = neural.read_checkpoint(root / 'trial/best.pt', signature)['model']
        replay = neural.read_checkpoint(root / 'refit/last.pt', refit_sig)['model']
        for name in first_epoch:
            torch.testing.assert_close(first_epoch[name], replay[name], rtol=0, atol=0)
        # min_delta governs stopping, not whether an actually better checkpoint is kept.
        tiny_config = dict(config, early_stop_min_delta=.01)
        values = iter([.6, .600001, .600002])
        with mock.patch.object(neural, 'predictions', return_value=np.zeros((4, 20), np.int32)), \
             mock.patch.object(rec, 'query_metrics', side_effect=metrics):
            tiny, _ = neural.fit_trial(fold, examples, features, 'NCF_ID', params, root / 'tiny', {}, tiny_config)
        self.assertEqual(tiny['stop_reason'], 'ranking_plateau')
        self.assertEqual(tiny['selected_epoch'], 3)
        self.assertAlmostEqual(tiny['early_stopping']['significant_best'], .6)
        with self.assertRaisesRegex(ValueError, 'mismatch'):
            neural.fit_trial(fold, examples, features, 'NCF_ID', params, root / 'trial', {}, tiny_config)

    def test_resume_early_stop_before_and_after_validation_commit(self):
        root = self.workspace(); gold_path, bundle = neural_fixture(root)
        config = configuration(root, gold_path, bundle)
        config.update(early_stopping=True, early_stop_start_epoch=2, early_stop_patience=2,
                      max_epochs=6, eval_epochs=[1, 2, 3, 4, 5, 6])
        neural.validate_config(config)
        gold = rec.Gold(gold_path, 'All_Beauty'); fold = gold.fold('validation')
        examples = neural.load_examples(gold, fold); features = neural.Features('NCF_ID'); params = config['trials'][0]
        def scores(*args, **kwargs):
            epoch = int(Path(args[3]).name.split('-')[1])
            return np.full((4, 20), epoch, np.int32)
        checked = []
        def metrics(fold, predictions):
            epoch = int(predictions[0, 0]); checked.append(epoch)
            return {'ndcg@10': np.full(4, .7 - .1 * epoch), 'recall@10': np.ones(4)}
        with mock.patch.object(neural, 'predictions', side_effect=scores), mock.patch.object(rec, 'query_metrics', side_effect=metrics):
            full, full_sig = neural.fit_trial(fold, examples, features, 'NCF_ID', params, root / 'full', {}, config)
        reference = neural.read_checkpoint(root / 'full/last.pt', full_sig)
        original_train, original_save = neural.train_epoch, rec.save_json
        for point in ('before_epoch', 'before_validation', 'after_validation_commit', 'after_completion_commit'):
            with self.subTest(point=point):
                directory = root / point; fired = [False]; checked.clear()
                def training(*args):
                    if point == 'before_epoch' and args[5] == 3 and not fired[0]:
                        fired[0] = True; raise RuntimeError('simulated interruption')
                    return original_train(*args)
                def scoring(*args, **kwargs):
                    epoch = int(Path(args[3]).name.split('-')[1])
                    if point == 'before_validation' and epoch == 2 and not fired[0]:
                        fired[0] = True; raise RuntimeError('simulated interruption')
                    return scores(*args, **kwargs)
                def saving(path, value):
                    if Path(path) == directory / 'trial.json' and not fired[0]:
                        last_checked = value['early_stopping']['last_checked_epoch']
                        if ((point == 'after_validation_commit' and last_checked == 2)
                                or (point == 'after_completion_commit' and value.get('status') == 'complete')):
                            fired[0] = True; raise RuntimeError('simulated interruption')
                    return original_save(path, value)
                with mock.patch.object(neural, 'train_epoch', side_effect=training), \
                     mock.patch.object(neural, 'predictions', side_effect=scoring), \
                     mock.patch.object(rec, 'query_metrics', side_effect=metrics), mock.patch.object(rec, 'save_json', side_effect=saving):
                    with self.assertRaisesRegex(RuntimeError, 'interruption'):
                        neural.fit_trial(fold, examples, features, 'NCF_ID', params, directory, {}, config)
                    self.assertTrue(fired[0])
                    resumed, sig = neural.fit_trial(fold, examples, features, 'NCF_ID', params, directory, {}, config)
                self.assertEqual(checked, [1, 2, 3])
                self.assertEqual(resumed['early_stopping'], full['early_stopping'])
                self.assertEqual((resumed['stopped_epoch'], resumed['selected_epoch']), (3, 1))
                payload = neural.read_checkpoint(directory / 'last.pt', sig)
                for name in reference['model']:
                    torch.testing.assert_close(reference['model'][name], payload['model'][name], rtol=0, atol=0)

    def test_end_to_end_early_stop_and_max_budget_reporting(self):
        root = self.workspace(); gold_path, bundle = neural_fixture(root)
        config = configuration(root, gold_path, bundle)
        config.update(early_stopping=True, early_stop_start_epoch=2, early_stop_patience=1,
                      early_stop_min_delta=1., max_epochs=4, eval_epochs=[1, 2, 3, 4])
        run = neural.run(config)
        trial = rec.read_json(run / 'trials/N0/trial.json')
        self.assertEqual(trial['stop_reason'], 'ranking_plateau')
        self.assertEqual(trial['stopped_epoch'], 2)
        self.assertEqual(rec.read_json(run / 'trials/N0/last.json')['epoch'], 2)
        selected = rec.read_json(run / 'best_config.json')['best']
        self.assertEqual(selected['selected_epoch'], selected['epoch'])
        self.assertEqual(selected['stopped_epoch'], 2)
        neural.run(dict(config, phase='test'))
        final = rec.read_json(run / 'test/model/trial.json')
        self.assertEqual(final['stop_reason'], 'refit_selected_epoch')
        self.assertEqual(final['stopped_epoch'], selected['epoch'])
        short = configuration(root, gold_path, bundle)
        neural.validate_config(short)
        gold = rec.Gold(gold_path, 'All_Beauty'); fold = gold.fold('validation')
        values = iter([.2, .3])
        def metric(*args): return {'ndcg@10': np.full(4, next(values)), 'recall@10': np.ones(4)}
        with mock.patch.object(neural, 'predictions', return_value=np.zeros((4, 20), np.int32)), \
             mock.patch.object(rec, 'query_metrics', side_effect=metric):
            state, _ = neural.fit_trial(fold, neural.load_examples(gold, fold), neural.Features('NCF_ID'),
                'NCF_ID', short['trials'][0], root / 'cap', {}, short)
        self.assertEqual(state['stop_reason'], 'max_epochs')
        self.assertTrue(state['budget_exhausted_while_improving'])

    def test_prefix_ties_missing_event_multiplicity_and_mutation(self):
        vectors = np.zeros((2, 384), np.float32); vectors[0, 0] = 1; vectors[1, 1] = 1
        events = pd.DataFrame(dict(user_idx=[0, 0, 0, 0], record_hash=['a', 'b', 'c', 'd'],
            event_time=pd.to_datetime(['2020-01-01', '2020-01-01', '2020-01-02', '2020-01-03']),
            rating=[1., 5., 4., np.nan], vector_row=[0, 0, 1, -1]))
        anchors = pd.DataFrame(dict(user_idx=[0, 0, 0, -1],
            prediction_time=pd.to_datetime(['2020-01-01', '2020-01-02', '2020-01-04', '2020-01-04']),
            feature_row=[0, 1, 2, 3]))
        stats = np.zeros((4, 7), np.float32); reviews = np.zeros((4, 384), np.float32)
        neural.prefix_features(events, anchors, vectors, stats, reviews)
        np.testing.assert_allclose(stats[0], [0, 3, 0, 0, 0, 1, 1])
        np.testing.assert_allclose(stats[1], [2, 3, .5, .5, 2, 0, 0])
        np.testing.assert_allclose(reviews[2, :2], np.array([2., 1.]) / np.sqrt(5), atol=1e-6)
        self.assertEqual(stats[2, 0], 3)
        np.testing.assert_array_equal(stats[3], stats[0])
        changed = events.copy(); changed.loc[2:, 'rating'] = 1; changed.loc[2:, 'vector_row'] = 0
        other_stats = np.zeros_like(stats); other_reviews = np.zeros_like(reviews)
        neural.prefix_features(changed, anchors, vectors, other_stats, other_reviews)
        np.testing.assert_array_equal(stats[:2], other_stats[:2])
        np.testing.assert_array_equal(reviews[:2], other_reviews[:2])

    def test_architecture_hand_computed_fm_and_true_pair_scoring(self):
        parameters = dict(dim=2, hidden=[3])
        for variant in neural.VARIANTS:
            neural.seed_model(42)
            model = neural.PairNetwork(3, 4, variant, parameters)
            features = neural.Features(variant, np.eye(4, 384, dtype=np.float32), np.array([True, True, True, False]),
                dict(stats=np.zeros((3, 7), np.float32), reviews=np.zeros((3, 384), np.float32),
                     scaler_mean=np.zeros(7, np.float32), scaler_scale=np.ones(7, np.float32)) if variant == 'D2' else None)
            users = np.array([0, 1, 2], np.int64); rows = np.arange(3)
            config = dict(query_batch=2, item_block=1, pair_batch=2)
            expected = neural.score_block(model, users, np.arange(4), rows, features, 'cpu', 12)
            expected_ids, _ = rec.numpy_topk(expected, np.arange(4), 20)
            for block in (1, 3, 4):
                config['item_block'] = block
                ranked, _ = neural.rank_neural(model, users, rows, features, 4, config, 'cpu')
                np.testing.assert_array_equal(ranked[:, :4], expected_ids)
                np.testing.assert_array_equal(ranked[:, 4:], -1)
        model = neural.PairNetwork(1, 2, 'D0', parameters)
        with torch.no_grad():
            model.user.weight[:] = torch.tensor([[1., 2.]])
            model.item.weight[:] = torch.tensor([[3., 4.], [0., 0.]])
            model.user_bias.weight[:] = 1
            model.item_bias.weight[:] = 2
            model.bias[:] = 3
            model.output.weight[:] = 0; model.output.bias[:] = 0
        np.testing.assert_allclose(model(torch.tensor([0, 0]), torch.tensor([0, 1])).detach().numpy(), [17, 6])
        with torch.no_grad():
            for parameter in model.parameters(): parameter.zero_()
        tied, _ = neural.rank_neural(model, np.array([0]), np.array([0]), neural.Features('D0'), 2,
                                     dict(query_batch=1, item_block=1, pair_batch=1), 'cpu')
        self.assertEqual(tied[0, :2].tolist(), [0, 1])

    def test_sampler_variants_batches_saturated_pool(self):
        root = self.workspace(); gold_path, bundle = neural_fixture(root)
        gold = rec.Gold(gold_path, 'All_Beauty'); fold = gold.fold('validation')
        examples = neural.load_examples(gold, fold)
        first = {}
        for row in examples.itertuples():
            sampled = neural.sample_negatives(fold.binary, row.user_idx, row.sampler_key, 1, 42, 4)
            seen = fold.binary[row.user_idx].indices
            self.assertFalse(set(sampled) & set(seen))
            self.assertEqual(len(sampled), min(4, 4 - len(seen)))
            self.assertEqual(len(set(sampled)), len(sampled))
            first[row.example_id] = sampled
        for row in examples.iloc[::-1].itertuples():
            np.testing.assert_array_equal(first[row.example_id], neural.sample_negatives(fold.binary, row.user_idx, row.sampler_key, 1, 42, 4))
        dense = rec.sparse.csr_matrix(np.ones((1, 4), np.float32))
        self.assertEqual(len(neural.sample_negatives(dense, 0, 1, 1, 42, 4)), 0)
        config = configuration(root, gold_path, bundle)
        batches = list(neural.epoch_batches(examples, fold.binary, 1, config))
        self.assertEqual(sum(int(b[2].sum()) for b in batches), len(examples))

    def test_vectorized_sampler_sparse_pools_order_batching_and_uniformity(self):
        rows = np.repeat(np.arange(40), 7)
        cols = (rows * 11 + np.tile(np.arange(7) * 3, 40)) % 127
        binary = rec.sparse.csr_matrix((np.ones(len(rows)), (rows, cols)), shape=(40, 127))
        users = np.arange(3000, dtype=np.int64) % 40
        keys = np.arange(3000, dtype=np.uint64) + np.uint64(0xf012ab3456)
        expected = neural.sample_negative_batch(binary, users, keys, 3, 42, 4)
        self.assertTrue((expected >= 0).all())
        self.assertTrue((np.diff(np.sort(expected, axis=1), axis=1) > 0).all())
        self.assertTrue((np.asarray(binary[np.repeat(users, 4), expected.ravel()]).ravel() == 0).all())
        for batch in (31, 257):
            actual = np.concatenate([neural.sample_negative_batch(binary, users[s:s+batch], keys[s:s+batch], 3, 42, 4)
                                     for s in range(0, len(users), batch)])
            np.testing.assert_array_equal(actual, expected)
        np.testing.assert_array_equal(neural.sample_negative_batch(binary, users[::-1], keys[::-1], 3, 42, 4)[::-1], expected)
        empty = rec.sparse.csr_matrix((1, 17), dtype=np.float32)
        draw = neural.sample_negative_batch(empty, np.zeros(20000, np.int64), np.arange(20000, dtype=np.uint64), 1, 42, 1)
        frequencies = np.bincount(draw.ravel(), minlength=17)
        self.assertLess(np.max(np.abs(frequencies - 20000/17)) / (20000/17), .2)
        tiny = rec.sparse.csr_matrix(np.ones((1, 1), np.float32))
        np.testing.assert_array_equal(neural.sample_negative_batch(tiny, np.array([0]), np.array([1]), 1, 42, 4), -1)

    def test_device_topk_exact_float_order_and_merging(self):
        rng = np.random.default_rng(7)
        scores = rng.normal(size=(5, 257)).astype(np.float32)
        scores[:, :7] = np.array([-np.inf, -0., 0., np.nextafter(np.float32(0), np.float32(1)),
                                  1., np.nextafter(np.float32(1), np.float32(2)), np.inf], np.float32)
        scores[1, 20:200] = 1.
        ids = np.stack([rng.permutation(257) for _ in range(5)]).astype(np.int64)
        for k in (1, 20, 257):
            expected_ids, expected_scores = rec.numpy_topk(scores, ids, k)
            actual_ids, actual_scores = neural.device_topk(torch.from_numpy(scores), torch.from_numpy(ids), k)
            np.testing.assert_array_equal(actual_ids.numpy(), expected_ids)
            np.testing.assert_array_equal(actual_scores.numpy(), expected_scores)
        best_ids = torch.empty((5, 0), dtype=torch.long); best_scores = torch.empty((5, 0))
        for start in range(0, 257, 23):
            part_ids, part_scores = neural.device_topk(torch.from_numpy(scores[:, start:start+23]), torch.from_numpy(ids[:, start:start+23]))
            best_ids, best_scores = neural.device_topk(torch.cat([best_scores, part_scores], 1), torch.cat([best_ids, part_ids], 1))
        np.testing.assert_array_equal(best_ids.numpy(), rec.numpy_topk(scores, ids, 20)[0])

    def test_scoring_projection_cache_matches_direct_pairs_and_new_checkpoints(self):
        rng = np.random.default_rng(11)
        neural.seed_model(42)
        for variant in neural.VARIANTS:
            model = neural.PairNetwork(3, 37, variant, dict(dim=8, hidden=[32, 16])).eval()
            item_vectors = rng.normal(size=(37, 384)).astype(np.float32)
            item_vectors, _ = rec.normalize_rows(item_vectors)
            valid = np.ones(37, bool); valid[-1] = False; item_vectors[-1] = 0
            d2 = dict(stats=np.abs(rng.normal(size=(6, 7))).astype(np.float32),
                reviews=rng.normal(size=(6, 384)).astype(np.float32),
                scaler_mean=np.zeros(7, np.float32), scaler_scale=np.ones(7, np.float32)) if variant == 'D2' else None
            features = neural.Features(variant, item_vectors, valid, d2)
            users = np.arange(3); rows = np.array([1, 3, 5])
            direct = neural.score_block(model, users, np.arange(37), rows, features, 'cpu', 19)
            cache = neural.ScoringFeatures(model, features, rows, 'cpu', batch=7)
            cached = neural.score_block(model, users, np.arange(37), rows, cache, 'cpu', 64)
            np.testing.assert_allclose(cached, direct, rtol=2e-6, atol=2e-6)
            expected = rec.numpy_topk(direct, np.arange(37), 20)[0]
            for enabled in (True, False):
                ranked, _ = neural.rank_neural(model, users, rows, features, 37,
                    dict(query_batch=2, item_block=13, pair_batch=19, score_feature_cache=enabled), 'cpu')
                np.testing.assert_array_equal(ranked, expected)
            if variant in ('D1', 'D2'):
                with torch.no_grad(): model.item_projection.bias.add_(.1)
                refreshed = neural.ScoringFeatures(model, features, rows, 'cpu', batch=7)
                reference = neural.score_block(model, users, np.arange(37), rows, features, 'cpu', 19)
                actual = neural.score_block(model, users, np.arange(37), rows, refreshed, 'cpu', 64)
                np.testing.assert_allclose(actual, reference, rtol=2e-6, atol=2e-6)

    def test_prefetch_matches_sync_training_and_rejects_nonfinite_before_update(self):
        root = self.workspace(); gold_path, bundle = neural_fixture(root)
        config = configuration(root, gold_path, bundle)
        neural.validate_config(config)
        gold = rec.Gold(gold_path, 'All_Beauty'); fold = gold.fold('validation')
        examples = neural.load_examples(gold, fold); features = neural.Features('NCF_ID')
        states, losses = [], []
        for enabled in (False, True):
            neural.seed_model(42)
            model = neural.PairNetwork(3, 4, 'NCF_ID', config['trials'][0])
            optimizer = torch.optim.Adam(model.parameters(), lr=.01, foreach=True)
            result = neural.train_epoch(model, optimizer, fold, examples, features, 1, dict(config, prefetch_train=enabled))
            self.assertGreater(result['pairs_per_second'], 0)
            states.append({k: v.detach().clone() for k, v in model.state_dict().items()}); losses.append(result['loss'])
        self.assertEqual(losses[0], losses[1])
        for name in states[0]: torch.testing.assert_close(states[0][name], states[1][name], rtol=0, atol=0)
        with torch.no_grad(): model.output.weight.fill_(float('nan'))
        with mock.patch.object(optimizer, 'step') as step:
            with self.assertRaisesRegex(ValueError, 'Non-finite'):
                neural.train_epoch(model, optimizer, fold, examples, features, 1, config)
            step.assert_not_called()
        with self.assertRaisesRegex(ValueError, 'NaN/Inf'):
            neural.rank_neural(model, np.array([0]), np.array([0]), features, 4,
                              dict(query_batch=1, item_block=2, pair_batch=3), 'cpu')

    @unittest.skipUnless(torch.cuda.is_available(), 'CUDA integration requires Kaggle GPU')
    def test_cuda_ranking_matches_cpu_reference(self):
        neural.seed_model(42)
        model = neural.PairNetwork(3, 37, 'NCF_ID', dict(dim=8, hidden=[32, 16])).eval()
        features = neural.Features('NCF_ID'); users = np.arange(3); rows = np.zeros(3, np.int64)
        direct = neural.score_block(model, users, np.arange(37), rows, features, 'cpu', 111)
        actual, _ = neural.rank_neural(model.cuda(), users, rows, features, 37,
            dict(query_batch=2, item_block=13, pair_batch=64), 'cuda:0')
        np.testing.assert_array_equal(actual, rec.numpy_topk(direct, np.arange(37), 20)[0])

    def test_all_variants_real_train_refit_resume_and_feature_cache(self):
        root = self.workspace(); gold, bundle = neural_fixture(root)
        for variant in neural.VARIANTS:
            config = configuration(root, gold, bundle, variant)
            run = neural.run(config)
            selected = rec.read_json(run / 'best_config.json')['best']
            self.assertIn(selected['epoch'], [1, 2])
            self.assertTrue((run / selected['checkpoint']).is_file())
            self.assertTrue((run / 'trials/N0/last.pt').is_file())
            with mock.patch.object(neural, 'fit_trial', side_effect=AssertionError('Completed phase should be reused')):
                self.assertEqual(neural.run(config), run)
            config.update(phase='test', output_root=str(root / (variant + '-restored')), resume_root=str(root / variant))
            restored = neural.run(config)
            self.assertTrue((restored / 'test/model/last.pt').is_file())
            self.assertEqual(rec.read_json(restored / 'test/model/last.json')['epoch'], selected['epoch'])
            self.assertEqual(restored.name, run.name)
            metrics = pd.read_csv(restored / 'test/evaluation/metrics.csv')
            row = metrics[metrics.model == variant.lower() + '_native']
            self.assertEqual(int(row[row.cohort == 'all_queries'].iloc[0].queries), 4)
            reasons = pd.read_parquet(restored / 'test/evaluation' / (variant.lower() + '_popularity_per_query.parquet'))
            self.assertEqual(reasons.loc[reasons.query_id == 'q2', 'fallback_reason'].iloc[0], 'cold_user')
            if variant == 'D2':
                stats = np.load(restored / 'validation/features/query_stats.npy')
                self.assertEqual(stats[0, 4], 2)
                self.assertTrue(rec.read_json(restored / 'validation/features/features.json')['cache_compacted'])
                self.assertFalse((restored / 'validation/features/review_vectors.npy').exists())
                local_gold = rec.Gold(gold, 'All_Beauty'); fold = local_gold.fold('validation')
                cache = root / 'feature-check'
                features = neural.build_d2(local_gold, fold, neural.load_examples(local_gold, fold), bundle / 'embedding_manifest.json', cache, config)
                self.assertEqual(features['stats'][features['train_feature_rows'][1], 0], 1)
                del features
                path = cache / 'stats.npy'
                with path.open('ab') as stream: stream.write(b'corrupted')
                with self.assertRaisesRegex(ValueError, 'checksum'):
                    neural.build_d2(local_gold, fold, neural.load_examples(local_gold, fold), bundle / 'embedding_manifest.json', cache, config)

    def test_epoch_restore_matches_uninterrupted_and_test_gate(self):
        root = self.workspace(); gold_path, bundle = neural_fixture(root)
        config = configuration(root, gold_path, bundle)
        with self.assertRaisesRegex(ValueError, 'validation first'):
            neural.run(dict(config, phase='test'))
        neural.validate_config(config)
        gold = rec.Gold(gold_path, 'All_Beauty'); fold = gold.fold('validation')
        examples = neural.load_examples(gold, fold); features = neural.Features('NCF_ID')
        params = config['trials'][0]
        complete, sig = neural.fit_trial(fold, examples, features, 'NCF_ID', params, root / 'full', {}, config)
        original = neural.train_epoch
        def interrupted(model, optimizer, fold, examples, features, epoch, config):
            if epoch == 2: raise RuntimeError('simulated session interruption')
            return original(model, optimizer, fold, examples, features, epoch, config)
        with mock.patch.object(neural, 'train_epoch', side_effect=interrupted):
            with self.assertRaisesRegex(RuntimeError, 'session interruption'):
                neural.fit_trial(fold, examples, features, 'NCF_ID', params, root / 'resumed', {}, config)
        neural.fit_trial(fold, examples, features, 'NCF_ID', params, root / 'resumed', {}, config)
        a = neural.read_checkpoint(root / 'full/last.pt', sig)
        b = neural.read_checkpoint(root / 'resumed/last.pt', sig)
        for name in a['model']:
            torch.testing.assert_close(a['model'][name], b['model'][name], rtol=0, atol=0)
        path = root / 'resumed/last.pt'
        with path.open('ab') as stream: stream.write(b'corrupted')
        with self.assertRaisesRegex(ValueError, 'checksum'): neural.read_checkpoint(path, sig)

    def test_private_upload_contains_torch_checkpoints_and_auto_restore(self):
        root = self.workspace(); gold, bundle = neural_fixture(root)
        config = configuration(root, gold, bundle)
        config.update(publish_results=True, auto_resume=True, result_datasets={'NCF_ID/All_Beauty': 'fixture/ncf-results'})
        snapshots = []
        def upload(handle, directory, version_notes):
            source = Path(directory)
            metadata = rec.read_json(source / 'result_bundle.json')
            paths = [name for name in metadata['files'] if name.endswith('.pt')]
            self.assertTrue(paths)
            for name in paths:
                self.assertEqual(rec.sha256(source / 'recommendation_results' / metadata['run'] / name), metadata['files'][name]['sha256'])
            shutil.copytree(source, root / 'remote', dirs_exist_ok=True)
            snapshots.append(metadata)
        hub = SimpleNamespace(dataset_upload=mock.Mock(side_effect=upload), dataset_download=mock.Mock(return_value=str(root / 'remote')))
        with mock.patch.dict(sys.modules, kagglehub=hub), mock.patch.object(rec, 'configure_kaggle_auth'), mock.patch.object(rec, 'result_dataset_private', return_value=False):
            first = neural.run_suite(config)
            self.assertEqual(first[0]['upload']['status'], 'submitted_processing')
        config['output_root'] = str(root / 'new-session')
        with mock.patch.dict(sys.modules, kagglehub=hub), mock.patch.object(rec, 'configure_kaggle_auth'), mock.patch.object(rec, 'result_dataset_private', return_value=True):
            with mock.patch.object(neural, 'fit_trial', side_effect=AssertionError('Remote complete phases should be reused')):
                second = neural.run_suite(config)
            self.assertTrue(second[0]['upload']['reused_remote'])
            self.assertEqual(len(snapshots), 1)

    def test_trial_selection_pruning_and_ranking_corruption(self):
        root = self.workspace(); gold, bundle = neural_fixture(root)
        config = configuration(root, gold, bundle)
        config.update(max_epochs=1, eval_epochs=[1])
        config['trials'].append(dict(name='N1', dim=3, hidden=[4], lr=.01, weight_decay=.00001))
        run = neural.run(config)
        winner = rec.read_json(run / 'best_config.json')['best']['trial']
        for name in ('N0', 'N1'):
            state = rec.read_json(run / 'trials' / name / 'trial.json')
            self.assertEqual((run / 'trials' / name / 'best.pt').is_file(), name == winner)
            if name != winner: self.assertTrue(state['model_pruned'])
        rows = pd.read_csv(run / 'trials.csv')
        self.assertEqual(len(rows), 2)
        # Re-running an incomplete final phase retains selection and reuses trial evidence.
        (run / 'validation/phase_complete.json').unlink()
        neural.run(config)
        gold_reader = rec.Gold(gold, 'All_Beauty'); fold = gold_reader.fold('validation')
        model = neural.PairNetwork(3, 4, 'NCF_ID', config['trials'][0])
        features = neural.Features('NCF_ID')
        predictions = neural.predictions(model, fold, features, root / 'scoring-check', {}, config)
        part = next((root / 'scoring-check').glob('part-*.npz'))
        with part.open('ab') as stream: stream.write(b'corrupted')
        with self.assertRaisesRegex(ValueError, 'checkpoint mismatch'):
            neural.predictions(model, fold, features, root / 'scoring-check', {}, config)

    def test_explicit_output_resume_takes_precedence_over_remote_dataset(self):
        root = self.workspace()
        config = configuration(root, root / 'gold', root / 'embedding')
        config.update(publish_results=True, auto_resume=True, resume_root=str(root / 'explicit-output'),
                      result_datasets={'NCF_ID/All_Beauty': 'fixture/ncf-results'})
        fake_gold = SimpleNamespace(info={'tables': {'validation/train_examples': {}, 'test/train_examples': {}}})
        hub = SimpleNamespace(dataset_download=mock.Mock(side_effect=AssertionError('Explicit resume must win')))
        calls = []
        def execute(current):
            calls.append(current)
            return root / 'run'
        with mock.patch.dict(sys.modules, kagglehub=hub), mock.patch.object(rec, 'Gold', return_value=fake_gold), \
             mock.patch.object(rec, 'configure_kaggle_auth'), mock.patch.object(rec, 'result_dataset_private', return_value=True), \
             mock.patch.object(neural, 'run', side_effect=execute), mock.patch.object(rec, 'publish_results', return_value={}):
            neural.run_suite(config)
        self.assertEqual([c['phase'] for c in calls], ['validation', 'test'])
        self.assertTrue(all(c['resume_root'] == config['resume_root'] for c in calls))
        hub.dataset_download.assert_not_called()

    def test_generated_notebooks_embed_current_source_and_valid_config(self):
        for number, algorithm in [('09', 'ncf'), ('10', 'deepfm')]:
            path = ROOT / 'notebooks' / (number + '_kaggle_' + algorithm + '.ipynb')
            notebook = json.loads(path.read_text(encoding='utf-8'))
            self.assertEqual(notebook['nbformat'], 4)
            sources = [''.join(cell['source']) for cell in notebook['cells'] if cell['cell_type'] == 'code']
            for module, source in [('gold_recommendation.py', 'kaggle_recommendation.py'), ('gold_neural_recommendation.py', 'kaggle_neural_recommendation.py')]:
                self.assertIn('%%writefile /kaggle/working/' + module + '\n' + (ROOT / 'notebooks' / source).read_text(encoding='utf-8'), sources)
            for source in sources:
                if not source.startswith('%%'): compile(source, str(path), 'exec')
            controls = next(source for source in sources if source.startswith('CATEGORIES_TO_RUN ='))
            namespace = {'Path': Path}
            exec(controls, namespace)
            self.assertEqual(neural.training_plan(namespace['CONFIG']), neural.training_plan(neural.default_config(algorithm)))
            self.assertTrue(all(not c['outputs'] and c['execution_count'] is None for c in notebook['cells'] if c['cell_type'] == 'code'))


if __name__ == '__main__':
    unittest.main()
