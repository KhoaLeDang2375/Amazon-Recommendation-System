"""Two-Tower CPU checks on real Parquet fixtures, including resume and cached retrieval."""
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

import numpy as np
import pandas as pd
import torch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'notebooks'))
sys.path.insert(0, str(ROOT / 'tests'))
import kaggle_neural_recommendation as neural
import kaggle_recommendation as rec
from test_kaggle_neural_recommendation import neural_fixture


def configuration(root, gold, bundle, variant='TT0'):
    config = neural.default_config('two_tower')
    config.update(categories=['All_Beauty'], category='All_Beauty', variants=[variant], variant=variant,
        phase='validation', gold_root=str(gold), embedding_root=str(bundle), output_root=str(root / variant),
        train_device='cpu', score_devices=['cpu'], cpu_threads=1, train_batch=10, query_batch=2,
        item_block=2, score_shard_queries=2, min_free_gib=0, bootstrap_samples=10,
        max_epochs=2, eval_epochs=[1, 2], early_stopping=False, publish_results=False,
        trials=[dict(name='T2', dim=2, hidden=[4], lr=.01, weight_decay=.00001)])
    return config


class TowerChecks(unittest.TestCase):
    def workspace(self):
        work = ROOT / 'outputs/validation/two-tower-fixtures'
        work.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(dir=work, prefix='case-')
        self.addCleanup(temporary.cleanup)
        return Path(temporary.name).resolve()

    def test_architecture_independence_gradients_cached_scoring_and_ties(self):
        torch.set_num_threads(1)
        config = neural.default_config('two_tower')
        config.update(train_device='cpu', score_devices=['cpu'])
        neural.validate_config(config)
        rng = np.random.default_rng(42)
        text = rng.normal(size=(37, 384)).astype(np.float32)
        present = np.ones(37, bool); present[3] = False; text[3] = 0
        d2 = dict(stats=np.zeros((3, 7), np.float32), reviews=rng.normal(size=(3, 384)).astype(np.float32),
            scaler_mean=np.zeros(7, np.float32), scaler_scale=np.ones(7, np.float32))
        for variant in neural.TOWER_VARIANTS:
            with self.subTest(variant=variant):
                neural.seed_model(42)
                model = neural.make_model(3, 37, variant, config['trials'][0], config)
                self.assertEqual(model.user_tower[0].out_features, 64)
                self.assertEqual(model.user_tower[-1].out_features, 32)
                self.assertEqual(sum(p.numel() for p in model.parameters()),
                    neural.parameter_budget(3, 37, variant, config['trials'][0], config)['parameters'])
                features = neural.Features(variant, text, present, d2 if variant == 'TT2' else None)
                users = np.array([0, 1, 2]); rows = np.array([0, 1, 2])
                items = np.array([0, 3, 5])
                logits = model(torch.as_tensor(users), torch.as_tensor(items), **features.tensors(items, rows, 'cpu'))
                torch.nn.functional.binary_cross_entropy_with_logits(logits, torch.tensor([1., 0., 1.])).backward()
                for module in [model.user, model.item, model.user_tower[-1], model.item_tower[-1]]:
                    self.assertTrue(torch.isfinite(module.weight.grad).all())
                    self.assertGreater(float(module.weight.grad.abs().sum()), 0)
                if variant == 'TT2':
                    self.assertGreater(float(model.review_projection.weight.grad.abs().sum()), 0)
                if variant != 'TT0':
                    self.assertGreater(float(model.item_projection.weight.grad.abs().sum()), 0)
                model.eval()
                cache = neural.TowerScoringFeatures(model, features, 37, 'cpu', batch=7)
                with mock.patch.object(model, 'encode_item', side_effect=AssertionError('Item tower must be cached')):
                    actual, scores = neural.rank_neural(model, users, rows, cache, 37, config, 'cpu')
                with torch.inference_mode():
                    direct = np.stack([model(torch.full((37,), int(user)), torch.arange(37),
                        **features.tensors(np.arange(37), np.full(37, row), 'cpu')).numpy() for user, row in zip(users, rows)])
                expected, values = rec.numpy_topk(direct, np.arange(37), 20)
                np.testing.assert_array_equal(actual, expected)
                np.testing.assert_allclose(scores, values, rtol=1e-4, atol=1e-6)
                self.assertTrue(torch.isfinite(cache.items[3]).all())  # no text is still an eligible ID item
                with torch.no_grad():
                    for parameter in model.parameters():
                        parameter.zero_()
                tied, _ = neural.rank_neural(model, users, rows, features, 37, config, 'cpu')
                np.testing.assert_array_equal(tied, np.tile(np.arange(20), (3, 1)))

    def test_all_variants_training_refit_export_phase_restore_and_ablations(self):
        root = self.workspace(); gold, bundle = neural_fixture(root)
        outputs = {}
        for variant in neural.TOWER_VARIANTS:
            config = configuration(root, gold, bundle, variant)
            run = neural.run(config)
            best = rec.read_json(run / 'best_config.json')['best']
            self.assertIn(best['epoch'], [1, 2])
            self.assertEqual(np.load(run / 'validation/item_output_vectors.npy').shape, (4, 32))
            with mock.patch.object(neural, 'fit_trial', side_effect=AssertionError('Reuse completed phase')):
                self.assertEqual(neural.run(config), run)
            config.update(phase='test', output_root=str(root / (variant + '-restored')), resume_root=str(root / variant))
            restored = neural.run(config)
            self.assertEqual(rec.read_json(restored / 'test/model/last.json')['epoch'], best['epoch'])
            metrics = pd.read_csv(restored / 'test/evaluation/metrics.csv')
            self.assertTrue((metrics.query("cohort == 'all_queries'").queries == 4).all())
            reasons = pd.read_parquet(restored / 'test/evaluation' / (variant.lower() + '_popularity_per_query.parquet'))
            self.assertEqual(reasons.loc[reasons.query_id == 'q2', 'fallback_reason'].iloc[0], 'cold_user')
            self.assertEqual(rec.read_json(restored / 'test/tower_export.json')['dimension'], 32)
            if variant == 'TT2':
                self.assertTrue(rec.read_json(restored / 'validation/features/features.json')['cache_compacted'])
                local = rec.Gold(gold, 'All_Beauty'); fold = local.fold('validation')
                arrays = neural.build_d2(local, fold, neural.load_examples(local, fold),
                    bundle / 'embedding_manifest.json', root / 'temporal', config)
                self.assertEqual(arrays['stats'][arrays['train_feature_rows'][0], 0], 0)
                self.assertEqual(arrays['stats'][arrays['train_feature_rows'][1], 0], 1)
                del arrays
            outputs[variant] = restored / 'test/evaluation'
        result = rec.compare_results(outputs['TT0'], outputs['TT1'], root / 'ci.json', samples=10)
        self.assertEqual(result['all_queries']['queries'], 4)
        # Execute the notebook's actual results cell: paths, plots, and both ablation CIs.
        config['output_root'] = str(root / 'summary')
        rec.save_json(Path(config['output_root']) / 'two_tower_suite_results.json',
            [dict(category='All_Beauty', variant=v, run_root=str(p.parent.parent)) for v, p in outputs.items()])
        notebook = rec.read_json(ROOT / 'notebooks/11_kaggle_two_tower.ipynb')
        source = next(''.join(c['source']) for c in notebook['cells'] if ''.join(c['source']).startswith('import pandas as pd'))
        import matplotlib
        matplotlib.use('Agg')
        with mock.patch.dict(sys.modules, gold_recommendation=rec):
            exec(source, dict(CONFIG=config, PHASES=config['phases'], CATEGORIES_TO_RUN=['All_Beauty'],
                sys=sys, json=json, Path=Path, display=lambda value: None, print=lambda *args, **kwargs: None))
        self.assertTrue((Path(config['output_root']) / 'comparisons/All_Beauty/test/TT2-vs-TT1-native.json').is_file())

    def test_interrupted_epoch_resume_and_test_gate(self):
        root = self.workspace(); gold_path, bundle = neural_fixture(root)
        config = configuration(root, gold_path, bundle)
        with self.assertRaisesRegex(ValueError, 'validation first'):
            neural.run(dict(config, phase='test'))
        neural.validate_config(config)
        gold = rec.Gold(gold_path, 'All_Beauty'); fold = gold.fold('validation')
        examples = neural.load_examples(gold, fold); features = neural.Features('TT0')
        parameters = config['trials'][0]
        _, signature = neural.fit_trial(fold, examples, features, 'TT0', parameters, root / 'full', {}, config)
        original = neural.train_epoch
        def interrupt(*args):
            if args[5] == 2:
                raise RuntimeError('interrupted')
            return original(*args)
        with mock.patch.object(neural, 'train_epoch', side_effect=interrupt):
            with self.assertRaisesRegex(RuntimeError, 'interrupted'):
                neural.fit_trial(fold, examples, features, 'TT0', parameters, root / 'resumed', {}, config)
        neural.fit_trial(fold, examples, features, 'TT0', parameters, root / 'resumed', {}, config)
        a = neural.read_checkpoint(root / 'full/last.pt', signature)
        b = neural.read_checkpoint(root / 'resumed/last.pt', signature)
        for key in a['model']:
            torch.testing.assert_close(a['model'][key], b['model'][key], rtol=0, atol=0)
        plan = neural.training_plan(config)
        self.assertNotEqual(plan, neural.training_plan(dict(config, tower_output_dim=64)))

    def test_suite_locks_all_validation_before_test(self):
        root = self.workspace(); gold, bundle = neural_fixture(root)
        config = configuration(root, gold, bundle)
        config['variants'] = ['TT0', 'TT1', 'TT2']
        calls = []
        def execute(current):
            calls.append((current['variant'], current['phase']))
            return root / current['variant']
        with mock.patch.object(neural, 'run', side_effect=execute):
            neural.run_suite(config)
        first_test = next(i for i, (_, phase) in enumerate(calls) if phase == 'test')
        self.assertEqual({variant for variant, phase in calls[:first_test] if phase == 'validation'}, set(config['variants']))

    def test_notebook_sources_controls_and_empty_outputs(self):
        path = ROOT / 'notebooks/11_kaggle_two_tower.ipynb'
        notebook = json.loads(path.read_text(encoding='utf-8'))
        sources = [''.join(c['source']) for c in notebook['cells'] if c['cell_type'] == 'code']
        for module, source in [('gold_recommendation.py', 'kaggle_recommendation.py'),
                               ('gold_neural_recommendation.py', 'kaggle_neural_recommendation.py')]:
            self.assertIn('%%writefile /kaggle/working/' + module + '\n' + (ROOT / 'notebooks' / source).read_text(encoding='utf-8'), sources)
        for source in sources:
            if not source.startswith('%%'):
                compile(source, str(path), 'exec')
        namespace = {'Path': Path}
        exec(next(s for s in sources if 'CATEGORIES_TO_RUN =' in s), namespace)
        config = namespace['CONFIG']
        self.assertEqual(config['algorithm'], 'two_tower')
        self.assertEqual(config['categories'], ['All_Beauty', 'Video_Games', 'Industrial_and_Scientific'])
        self.assertEqual(neural.training_plan(config), neural.training_plan(neural.default_config('two_tower')))
        self.assertTrue(all(c['execution_count'] is None and c['outputs'] == [] for c in notebook['cells'] if c['cell_type'] == 'code'))


if __name__ == '__main__':
    unittest.main()
