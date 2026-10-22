"""Small adversarial fixtures; HDFS integration only under /project/testing."""
import json
import os
from pathlib import Path
import unittest
import uuid

from pyspark import SparkContext
from pyspark.sql import functions as F
from processing.silver import Hdfs, create_spark, parse_line, run_silver, validate_paths


class SilverChecks(unittest.TestCase):
    def test_rules_and_boundaries(self):
        review = dict(user_id=" u1 ", parent_asin="p1", asin="a1", rating=1,
                      timestamp=1609459200123, text="  This is NOT good  ", helpful_vote=-1)
        row = parse_line(json.dumps(review), "fixture", "reviews", "All_Beauty")
        self.assertEqual(row["_outcome"], "accepted")
        self.assertEqual(row["review_text"], "This is NOT good")
        self.assertEqual(row["timestamp_raw"], 1609459200123)
        self.assertIsNone(row["helpful_vote"])
        review["rating"] = True
        review["timestamp"] = 900000000123  # 12 digits; still milliseconds.
        row = parse_line(json.dumps(review), "fixture", "reviews", "All_Beauty")
        self.assertIsNone(row["rating"])
        self.assertEqual(row["_outcome"], "accepted")
        for value in (True, 1609459200, "1609459200123", 1609459200123.0):
            row = parse_line(json.dumps(dict(review, timestamp=value)), "fixture", "reviews", "All_Beauty")
            self.assertEqual(row["_outcome"], "core_quarantine")
        for text in ('{', '[]', '{"a":1,"a":2}', '{"details":{"x":1,"x":2}}'):
            self.assertEqual(parse_line(text, "fixture", "metadata", "All_Beauty")["_outcome"], "parse_quarantine")
        one = parse_line(json.dumps(review), "fixture", "reviews", "All_Beauty")
        two = parse_line(json.dumps(dict(reversed(list(review.items())))), "fixture", "reviews", "All_Beauty")
        self.assertEqual(one["record_hash"], two["record_hash"])
        counter = parse_line(json.dumps(dict(parent_asin="p", rating_number="9007199254740993")), "fixture", "metadata", "All_Beauty")
        self.assertEqual(counter["rating_number"], 9007199254740993)
        for value in (True, "NaN", "1.5", str(2 ** 63)):
            counter = parse_line(json.dumps(dict(parent_asin="p", rating_number=value)), "fixture", "metadata", "All_Beauty")
            self.assertIsNone(counter["rating_number"])
        with self.assertRaisesRegex(ValueError, "runtime configuration"):
            run_silver("All_Beauty", min_free_gib=float("nan"))
        with self.assertRaises(ValueError):
            validate_paths("All_Beauty", "hdfs://namenode:8020/project/raw", "hdfs://namenode:8020/project/raw", False, "run_fixture_12345678")
        with self.assertRaises(ValueError):
            validate_paths("All_Beauty", "hdfs://namenode:8020/project/testing/raw/silver/../raw", "hdfs://namenode:8020/project/testing/processed/silver/a", True, "run_fixture_12345678")

    @unittest.skipUnless(os.environ.get("SILVER_TEST_HDFS") == "1", "Set SILVER_TEST_HDFS=1 inside the Spark test container")
    def test_hdfs_snapshot_and_failed_publication(self):
        token = "fixture_" + uuid.uuid4().hex
        source = "hdfs://namenode:8020/project/testing/raw/silver/" + token
        output = "hdfs://namenode:8020/project/testing/processed/silver/" + token
        report = "/workspace/outputs/validation/silver-fixture-" + token
        scratch = Path("/tmp") / token
        scratch.mkdir()
        first = dict(user_id=" u1 ", parent_asin="p1", asin="a1", rating=5,
                     timestamp=1609459200123, text="not bad", helpful_vote=2, Title="extra case")
        reviews = [json.dumps(first), json.dumps(dict(reversed(list(first.items())))),
                   json.dumps(dict(first, rating=1, text="not good", asin="variant")),
                   json.dumps(dict(user_id="u2", parent_asin="p2", timestamp=1640995200001,
                                   rating=True, text="   ", helpful_vote=-1, verified_purchase="true")),
                   json.dumps(dict(parent_asin="p2", timestamp=1640995200000, rating=4)),
                   json.dumps(dict(user_id="u3", parent_asin="unmatched", timestamp=1640995200000, rating=4)), '{']
        p1 = dict(parent_asin="p1", title="Product", price=0, details={"Brand": "A", "brand": "B"})
        metadata = [json.dumps(p1), json.dumps(p1), json.dumps(dict(parent_asin="p2", title="Second", price="not a price",
                    features=["useful", 7], author="extra")), json.dumps(dict(parent_asin="p3", title="No review"))]
        spark = create_spark(scratch, 1, 2)
        try:
            hdfs = Hdfs(spark)
            for kind, lines in (("reviews", reviews), ("metadata", metadata)):
                path = f"{source}/all_beauty_{kind}/v1/input.jsonl"
                hdfs.fs.mkdirs(hdfs.Path(path).getParent())
                stream = hdfs.fs.create(hdfs.Path(path), False)
                stream.write(bytearray(("\n".join(lines) + "\n").encode()))
                stream.close()
        finally:
            spark.stop()
        args = dict(category="All_Beauty", input_base=source, output_base=output, testing=True,
                    report_root=report, cores=1, shuffle_partitions=2, min_free_gib=0)
        result = run_silver(**args, run_id="run_success_12345678")
        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["quality"]["datasets"]["reviews"]["clean_rows"], 4)
        self.assertEqual(result["quality"]["datasets"]["reviews"]["exact_copy_removed"], 1)
        self.assertEqual(result["quality"]["datasets"]["metadata"]["clean_rows"], 3)
        self.assertEqual(result["quality"]["coverage"], {"left_join_rows": 4, "matched_reviews": 3})
        self.assertIsNone(SparkContext._active_spark_context)
        spark = create_spark(scratch, 1, 2)
        try:
            frame = spark.read.parquet(result["final_path"] + "/reviews_clean")
            self.assertEqual(frame.filter("event_ambiguous").count(), 2)
            self.assertEqual(frame.filter("user_id = 'u2'").first().rating, None)
            self.assertEqual(frame.filter("unix_millis(event_time) != timestamp_raw").count(), 0)
            self.assertIn("not good", [x.review_text for x in frame.collect()])
            catalog = spark.read.parquet(result["final_path"] + "/catalog")
            self.assertEqual(json.loads(catalog.filter("parent_asin = 'p1'").first().details_json), {"Brand": "A", "brand": "B"})
            self.assertEqual(catalog.filter("parent_asin = 'p2'").first().price, None)
            self.assertEqual(spark.read.parquet(result["final_path"] + "/quarantine").count(), 2)
            self.assertEqual(spark.read.parquet(result["final_path"] + "/duplicate_audit").count(), 2)
        finally:
            spark.stop()
        with self.assertRaisesRegex(FileExistsError, "manifest already exists"):
            run_silver(**args, run_id="run_success_12345678")
        with self.assertRaisesRegex(RuntimeError, "Injected failure"):
            run_silver(**args, run_id="run_failure_12345678", fail_before_publish=True)
        failure = json.loads((Path(report) / "run_failure_12345678/run.json").read_text())
        self.assertEqual(failure["status"], "failed")
        with self.assertRaisesRegex(RuntimeError, "budget exceeded"):
            run_silver(**args, run_id="run_budget_12345678", output_budget_gib=1e-9)
        self.assertIsNone(SparkContext._active_spark_context)
        # No deletion: all fixture data/evidence remains in the testing namespace.
        spark = create_spark(scratch, 1, 2)
        try:
            hdfs = Hdfs(spark)
            self.assertFalse(hdfs.exists(failure["final_path"]))
            self.assertTrue(hdfs.exists(failure["staging_path"]))
            path = f"{source}/all_beauty_metadata/v1/conflict.jsonl"
            stream = hdfs.fs.create(hdfs.Path(path), False)
            stream.write(bytearray((json.dumps(dict(p1, title="Conflicting title")) + "\n").encode()))
            stream.close()
        finally:
            spark.stop()
        with self.assertRaisesRegex(ValueError, "metadata parent conflicts"):
            run_silver(**args, run_id="run_conflict_12345678")
        diagnostic = json.loads((Path(report) / "run_conflict_12345678/metadata_conflicts.json").read_text())
        self.assertEqual(diagnostic["conflicting_parent_groups"], 1)
        self.assertEqual(len(diagnostic["samples"]), 2)
        self.assertNotIn("parent_asin", diagnostic["samples"][0])
        self.assertIsNone(SparkContext._active_spark_context)


if __name__ == "__main__":
    unittest.main()
