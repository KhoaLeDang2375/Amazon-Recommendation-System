"""Runnable temporal/metric/ALS persistence check, using synthetic events only."""
import math
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "notebooks"))
import als_baseline as baseline
from pyspark.ml.recommendation import ALSModel


class BaselineCheck(unittest.TestCase):
    def test_temporal_labels_metrics_and_saved_model(self):
        spark = baseline.create_spark(2, 2)
        try:
            events = spark.createDataFrame([
                ("u", "a", datetime(2020, 1, 1)), ("u", "a", datetime(2020, 2, 1)),
                ("v", "b", datetime(2020, 1, 1)), ("v", "a", datetime(2020, 2, 1)),
                ("w", "c", datetime(2020, 1, 1)), ("w", "b", datetime(2020, 2, 1)),
                ("u", "a", datetime(2021, 1, 1)), ("u", "d", datetime(2021, 1, 1)),
                ("u", "b", datetime(2021, 3, 1)), ("cold", "b", datetime(2021, 4, 1)),
                ("v", "c", datetime(2021, 2, 1)), ("w", "z", datetime(2022, 1, 1)),
            ], "user_id string, parent_asin string, event_time timestamp")
            pairs, targets = baseline.split_fold(events, "2021-01-01", "2022-01-01")
            self.assertEqual(pairs.count(), 5)
            labels = {(r.user_id, r.parent_asin) for r in targets.collect()}
            self.assertEqual(labels, {("u", "a"), ("u", "d"), ("cold", "b"), ("v", "c")})
            queries = baseline.query_population(pairs, targets)
            population = {r.user_id: r for r in queries.collect()}
            self.assertEqual(population["cold"].history_items, 0)
            self.assertEqual(population["u"].targets_in, 1)
            self.assertFalse(population["u"].warm)
            self.assertTrue(population["v"].warm)
            recs = spark.createDataFrame([("u", ["b", "a"]), ("v", ["c"])],
                                         "user_id string, predictions array<string>")
            scores = {r.user_id: r for r in baseline.query_scores(queries, recs, 10).collect()}
            self.assertEqual(scores["u"].recall, 0.5)
            self.assertAlmostEqual(scores["u"].ndcg, (1 / math.log2(3)) / (1 + 1 / math.log2(3)))
            self.assertEqual(scores["cold"].ndcg, 0)
            self.assertEqual(scores["cold"].recall, 0)
            self.assertEqual(scores["v"].ndcg, 1)
            self.assertEqual(baseline.evaluate(queries, recs, "toy")[0]["queries"], 3)
            sample = baseline.sample_users(events, 2, 42).collect()
            self.assertEqual(sample, baseline.sample_users(events.repartition(2), 2, 42).collect())
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp) / "fold"
                result = baseline.fit_fold(spark, events, root, "2021-01-01", "2022-01-01",
                                           2, 2, 0.1, 1.0, 42, lambda phase: None)
                self.assertEqual(result["fallback_queries"], 1)
                self.assertEqual(result["train_pairs"], 5)
                saved = ALSModel.load((root / "model").as_uri())
                self.assertEqual(saved.userFactors.count(), 3)
                self.assertEqual(saved.itemFactors.count(), 3)
                saved.userFactors.unpersist()
                saved.itemFactors.unpersist()
                system = spark.read.parquet((root / "recommendations").as_uri())
                self.assertEqual(system.count(), 3)
                self.assertEqual(len(result["metrics"]), 12)
        finally:
            spark.stop()


if __name__ == "__main__":
    unittest.main()
