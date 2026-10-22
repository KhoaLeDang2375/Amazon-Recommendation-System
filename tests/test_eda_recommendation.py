"""Small adversarial JSONL fixture, executed in the existing Spark container."""
import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "notebooks"))
from eda_recommendation import (SCHEMA, analyze_all, numeric, parse_partition,
                                resolve_timestamp, state, nested_issues, create_spark)
from pyspark import SparkContext


def read_table(root, name):
    with (root / (name + ".csv")).open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


class EdaChecks(unittest.TestCase):
    def test_full_fixture(self):
        import nbformat
        notebook = nbformat.read(Path(__file__).resolve().parents[1] / "notebooks" / "02_eda_recommendation.ipynb", as_version=4)
        nbformat.validate(notebook)
        for cell in notebook.cells:
            if cell.cell_type == "code":
                compile(cell.source, "02_eda_recommendation.ipynb", "exec")
        self.assertIsNone(numeric(True))
        self.assertEqual(state({}, "price"), "absent")
        self.assertEqual(state({"price": None}, "price"), "null")
        self.assertEqual(state({"price": " "}, "price"), "whitespace")
        self.assertEqual(resolve_timestamp(1609459200), (1609459200, "seconds"))
        self.assertEqual(resolve_timestamp(1609459200000), (1609459200, "milliseconds"))
        self.assertEqual(resolve_timestamp(0)[1], "outside_dataset_bounds")
        self.assertEqual(resolve_timestamp(True)[1], "invalid")
        self.assertEqual(resolve_timestamp(1609459200.5)[1], "invalid")
        review_images = [{"small_image_url": "https://example.com/s.jpg", "medium_image_url": "https://example.com/m.jpg",
                          "large_image_url": "https://example.com/l.jpg", "attachment_type": "IMAGE"}]
        self.assertEqual(nested_issues("images", review_images, "p1", "reviews"), set())
        self.assertEqual(nested_issues("images", [{"thumb": "https://example.com/t.jpg", "hi_res": None}], "p1"), set())
        self.assertIn("invalid_url_format", nested_issues("images", [{"large_image_url": "bad"}], "p1", "reviews"))
        malformed = [dict(source="fixture.jsonl", value=x) for x in ['{', '[]', '{"rating":1,"rating":2}']]
        summary = json.loads(list(parse_partition(malformed, "reviews"))[-1][SCHEMA.fieldNames().index("profile_json")])
        self.assertEqual(sum(x[-1] for x in summary["counts"] if x[1] == "parse_error"), 3)
        spark = create_spark(shuffle_partitions=2)
        spark.sparkContext.setLogLevel("ERROR")
        try:
            with tempfile.TemporaryDirectory(prefix="eda-check-", dir="/workspace/outputs") as temporary:
                base = Path(temporary) / "raw"
                reviews = [
                    dict(user_id=" u1 ", asin="a1", parent_asin="p1", rating=5,
                         timestamp=1609459200000, text="same", verified_purchase=True, images=review_images),
                    dict(user_id=" u1 ", asin="a1", parent_asin="p1", rating=5,
                         timestamp=1609459200000, text="same", verified_purchase=True, images=review_images),
                    dict(user_id="u1", asin="a2", parent_asin="p2", rating="4.0",
                         timestamp=1640995200, text="", verified_purchase="true"),
                    dict(user_id="u2", asin="a3", parent_asin="p3", rating=True,
                         timestamp=1640995200000, text="same", helpful_vote=-1),
                    dict(user_id="u3", asin="a4", parent_asin="missing", rating=1,
                         timestamp=1641081600000, text="ok"),
                    dict(user_id=None, parent_asin="p4", timestamp=0, rating=10),
                ]
                metadata = [
                    dict(parent_asin="p1", title="First", main_category="Beauty", price=0,
                         features=[], description=[], images=[], details={}),
                    dict(parent_asin="p1", title="First", main_category="Beauty", price=0,
                         features=[], description=[], images=[], details={}),
                    dict(parent_asin="p2", title="Second", price=10),
                    dict(parent_asin="p2", title="Conflict", price=20),
                    dict(parent_asin="p3", title="Third"),
                    dict(parent_asin="p3", description=["Complement"]),
                    dict(parent_asin="p4", title="Fourth", price="N/A", categories=[["A"]],
                         images=[3], bought_together=["p4", "p4"], details={"Brand": "B"}),
                ]
                for kind, records in [("reviews", reviews), ("metadata", metadata)]:
                    directory = base / ("all_beauty_" + kind) / "v1"
                    directory.mkdir(parents=True)
                    lines = [json.dumps(x) for x in records]
                    if kind == "reviews":
                        lines.extend(['{broken', 'null'])
                    (directory / "fixture.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")
                root = analyze_all(spark, ["All_Beauty"], base.as_uri(), output=str(Path(temporary) / "eda"), run_core=True)
                category = root / "All_Beauty"
                summary = json.loads((category / "summary.json").read_text())
                self.assertEqual((summary["review_rows"], summary["metadata_rows"]), (6, 7))
                self.assertEqual((summary["users"], summary["items"], summary["distinct_pairs"]), (3, 4, 4))
                self.assertAlmostEqual(summary["density"], 1 / 3)
                self.assertEqual(summary["left_join_rows"], 6)
                self.assertEqual(summary["matched_reviews"], 5)
                self.assertEqual(summary["safe_reviews"], 3)
                self.assertEqual(summary["repeated_pairs"], 1)
                overview = read_table(category, "reviews_overview")[0]
                self.assertEqual(overview["parse_errors"], "2")
                ids = read_table(category, "reviews_ids")[0]
                self.assertEqual((ids["user_id_distinct"], ids["asin_distinct"], ids["parent_asin_distinct"]), ("3", "4", "5"))
                missing = {x["field"]: x for x in read_table(category, "reviews_missing")}
                self.assertAlmostEqual(float(missing["rating"]["structurally_valid_or_convertible_rate"]), 4 / 6)
                classes = read_table(category, "metadata_conflict_classes")
                self.assertEqual({x["classification"] for x in classes}, {"identical", "complementary", "conflicting"})
                distribution = read_table(category, "rating_distribution")
                self.assertEqual(sum(int(x["count"]) for x in distribution if x["rating"] == "5.0"), 2)
                self.assertEqual(read_table(category, "price_summary")[0]["valid_values"], "1")
                split = read_table(category, "temporal_split_diagnostic")[0]
                self.assertEqual((split["train_reviews"], split["test_reviews"]), ("2", "3"))
                cold = read_table(category, "temporal_cold_start")
                self.assertEqual(sum(int(x["test_reviews"]) for x in cold), 3)
                self.assertEqual(read_table(category, "five_core_impact")[0]["pairs"], "0")
                run = json.loads((root / "run.json").read_text())
                self.assertEqual(run["status"], "complete")
                self.assertEqual(run["scope"], "full_category")
                self.assertEqual(run["cross_category_measurement"], "not_measured")
                self.assertFalse((root / "_work").exists())
                self.assertFalse((root / "comparison/global_sparsity.csv").exists())
                self.assertTrue((root / "comparison/category_sparsity.csv").exists())
                resources = json.loads((root / "resources.json").read_text())
                self.assertEqual(resources["samples"][-1]["phase"], "category_complete")
                self.assertEqual(spark.sparkContext._jsc.sc().getPersistentRDDs().size(), 0)
        finally:
            spark.stop()
        self.assertIsNone(SparkContext._active_spark_context)


if __name__ == "__main__":
    unittest.main()
