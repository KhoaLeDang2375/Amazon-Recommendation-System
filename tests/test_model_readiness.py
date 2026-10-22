"""Small Spark checks for Silver-only diagnostics; never read production data."""
import csv
import json
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'notebooks'))
import eda_model_readiness as eda


class ReadinessChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = eda.create_spark(2, 2)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frames(self):
        events = self.spark.createDataFrame([
            ('u', 'a', datetime(2020, 1, 1), False, 2., True, True),
            ('u', 'a', datetime(2020, 2, 1), False, 5., True, False),
            ('u', 'a', datetime(2021, 2, 1), True, 1., False, True),
            ('u', 'b', datetime(2021, 2, 1), True, 5., True, True),
            ('u', 'd', datetime(2021, 2, 1), False, 3., False, True),
            ('u', 'c', datetime(2021, 3, 1), False, 5., True, True),
            ('cold', 'c', datetime(2021, 4, 1), False, 3., True, False),
            ('train_only', 'b', datetime(2020, 1, 1), False, 4., True, False),
        ], 'user_id string, parent_asin string, event_time timestamp, event_ambiguous boolean, rating double, verified_purchase boolean, has_review_text boolean')
        catalog = self.spark.createDataFrame([
            ('a', 'Máy chơi game', ['fast', 'fast'], ['<p>Hello</p> https://a.test'], 'Games', 'Shop'),
            ('b', None, [], [], None, None),
            ('c', 'x' * 40000, [], ['!!!'], 'Other', 'Shop'),
        ], 'parent_asin string, product_title string, features array<string>, description array<string>, main_category string, store string')
        return events, catalog

    def test_first_next_ties_repeat_and_empty_history(self):
        events, catalog = self.frames()
        cf = eda.content(catalog).select('parent_asin', 'has_title', 'has_content', 'main_category', 'store')
        _, pairs, candidates, queries, targets = eda.diagnostic_fold(events, cf, '2021-01-01', '2022-01-01')
        rows = {r.user_id: r.asDict() for r in queries.collect()}
        self.assertEqual((rows['u']['targets'], rows['u']['targets_in'], rows['u']['repeat_targets']), (3, 2, 1))
        self.assertEqual(rows['u']['candidate_group'], 'partial')
        self.assertEqual(rows['u']['history_events'], 2)
        self.assertEqual(rows['u']['history_items'], 1)
        self.assertEqual(rows['cold']['history_events'], 0)
        self.assertEqual(rows['cold']['candidate_group'], 'outside')
        self.assertEqual(rows['train_only']['targets'], 0)
        verified = events.filter('verified_purchase = true')
        _, _, _, bq, bt = eda.diagnostic_fold(verified, cf, '2021-01-01', '2022-01-01')
        self.assertEqual(bt.filter("user_id = 'u'").select('parent_asin').first()[0], 'b')
        self.assertEqual(pairs.filter("user_id = 'u'").first().rating_versions, 2)
        # No target text/rating columns can leak through the target frame.
        self.assertNotIn('rating', targets.columns)
        self.assertNotIn('review_text', targets.columns)
        changed = events.withColumn('rating', eda.F.when(eda.F.col('event_time') >= eda.F.lit('2021-01-01').cast('timestamp'), 1.).otherwise(eda.F.col('rating')))
        cq = eda.diagnostic_fold(changed, cf, '2021-01-01', '2022-01-01')[3]
        self.assertEqual(sorted(queries.collect()), sorted(cq.collect()))

    def test_profiles_full_diagnostic_and_empty(self):
        events, catalog = self.frames()
        with tempfile.TemporaryDirectory() as directory:
            # Match production's multi-partition Parquet/cache path, not just LocalRelation.
            events.write.parquet(str(Path(directory) / 'events'))
            events = self.spark.read.parquet(str(Path(directory) / 'events')).persist(eda.StorageLevel.DISK_ONLY)
            self.addCleanup(lambda: events.unpersist(blocking=True))
            events.count()
            report = eda.Report(Path(directory) / 'report')
            eda.profile_text(catalog, report)
            samples = json.loads((report.directory / 'text_samples.local.json').read_text())
            self.assertLessEqual(len(samples), 200)
            self.assertTrue(all(len(r['content_excerpt']) <= 1000 for r in samples))
            flags = list(csv.DictReader((report.directory / 'text_flags.csv').open(encoding='utf-8-sig')))[0]
            self.assertEqual(flags['html_like'], '1')
            self.assertEqual(flags['repeated_parts'], '1')
            cf = eda.content(catalog).select('parent_asin', 'has_title', 'has_content', 'main_category', 'store')
            result, sketch = eda.analyze_fold(events, cf, 'validation', 'A', '2021-01-01', '2022-01-01', report, lambda _: None)
            sketch.unpersist(blocking=True)
            self.assertEqual(result['queries_with_target'], 2)
            neg = list(csv.DictReader((report.directory / 'validation_A_negative_sampling_feasibility.csv').open(encoding='utf-8-sig')))
            self.assertEqual(neg[0]['positive_pairs'], '2')
            self.assertEqual(neg[0]['nominal_rows'], '4')
            empty, sketch = eda.analyze_fold(events.limit(0), cf, 'empty', 'A', '2021-01-01', '2022-01-01', report, lambda _: None)
            sketch.unpersist(blocking=True)
            self.assertEqual(empty['queries_with_target'], 0)
            self.assertEqual(empty['target_pairs'], 0)
            # A user who has seen all candidates cannot supply a distinct negative.
            all_seen = events.filter("user_id = 'u' AND event_time < timestamp'2021-01-01'")
            _, sketch = eda.analyze_fold(all_seen, cf, 'all_seen', 'A', '2021-01-01', '2022-01-01', report, lambda _: None)
            sketch.unpersist(blocking=True)
            full = list(csv.DictReader((report.directory / 'all_seen_A_negative_sampling_feasibility.csv').open(encoding='utf-8-sig')))
            self.assertEqual(full[0]['minimum_unseen'], '0')
            self.assertEqual(full[0]['users_below_1'], '1')

    def test_output_and_run_boundaries(self):
        events, _ = self.frames()
        expected = events.schema.jsonValue()
        reordered = events.select(*reversed(events.columns)).schema.jsonValue()
        self.assertTrue(eda.schema_matches(reordered, expected))
        wrong_type = events.withColumn('rating', eda.F.col('rating').cast('string')).schema.jsonValue()
        self.assertFalse(eda.schema_matches(wrong_type, expected))
        self.assertFalse(eda.schema_matches(events.drop('rating').schema.jsonValue(), expected))
        for run in ('../raw', 'run_../../escape', 'latest'):
            with self.assertRaises(ValueError):
                eda.validate_paths('All_Beauty', run, '/workspace/outputs/eda_model_readiness', 'run_20261018T000000Z')
        with self.assertRaises(ValueError):
            eda.validate_paths('All_Beauty', 'run_20261016T021632209982Z', '/project/raw', 'run_20261018T000000Z')


if __name__ == '__main__':
    unittest.main()
