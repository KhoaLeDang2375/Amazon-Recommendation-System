"""Cutoff boundaries, ties, denominators, history bins and deterministic selection."""
import sys
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "notebooks"))
import eda_cutoff as study


class SelectionChecks(unittest.TestCase):
    def test_selection_is_complete_latest_then_shortest(self):
        rows = [dict(category=c, start=s, months=m, warm_full_queries=3000 if m >= 6 else 10)
                for c in ("A", "B") for s in study.CUTOFFS for m in study.HORIZONS]
        chosen = study.choose_protocol(rows, ["A", "B"])
        self.assertEqual((chosen["validation_start"], chosen["validation_end"], chosen["test_end"]),
                         ("2021-01-01", "2021-07-01", "2022-07-01"))
        rows[-2]["warm_full_queries"] = 1
        self.assertEqual(study.choose_protocol(rows, ["A", "B"])["months"], 12)
        with self.assertRaises(ValueError):
            study.choose_protocol(rows[:-1], ["A", "B"])
        with self.assertRaises(ValueError):
            study.choose_protocol(rows + [rows[0]], ["A", "B"])
        for row in rows:
            row["warm_full_queries"] = 0
        self.assertEqual(study.choose_protocol(rows, ["A", "B"])["status"], "insufficient_support")


class SparkChecks(unittest.TestCase):
    def test_ties_horizon_boundaries_and_empty(self):
        from eda_recommendation import create_spark
        spark = create_spark(2, 2)
        try:
            events = spark.createDataFrame([
                ("u", "a", datetime(2020, 1, 1)), ("u", "a", datetime(2020, 2, 1)),
                ("other", "b", datetime(2020, 1, 1)),
                ("u", "a", datetime(2021, 1, 1)), ("u", "z", datetime(2021, 1, 1)),
                ("cold", "b", datetime(2021, 2, 1)),
                ("other", "a", datetime(2021, 4, 1)),
                ("end", "a", datetime(2022, 1, 1)),
            ], "user_id string, parent_asin string, event_time timestamp")
            rows, bins = study.measure(events, "2021-01-01", (3, 6, 12))
            short, medium, long = rows
            self.assertEqual((short["train_users"], short["train_items"], short["train_pairs"], short["train_events"]), (2, 2, 2, 3))
            self.assertEqual((short["queries"], short["warm_full_queries"], short["tied_queries"]), (2, 0, 1))
            self.assertEqual(short["cold_user_rate"], 0.5)
            self.assertEqual(short["target_lag_days_p50_p90"], [0.0, 31.0])
            self.assertAlmostEqual(short["cold_item_target_rate"], 1/3)
            self.assertEqual(short["any_cold_item_queries"], 1)
            self.assertEqual(short["all_cold_item_queries"], 0)
            self.assertEqual((medium["queries"], long["queries"], long["warm_full_queries"]), (3, 3, 1))
            self.assertEqual(short["train_users_without_target"], 1)
            self.assertTrue(any(r["measure"] == "history_events" and r["bin"] == "2" for r in bins))
            empty, _ = study.measure(events.limit(0), "2021-01-01", (3,))
            self.assertEqual(empty[0]["queries"], 0)
            self.assertIsNone(empty[0]["cold_item_target_rate"])
        finally:
            spark.stop()


if __name__ == "__main__":
    unittest.main()
