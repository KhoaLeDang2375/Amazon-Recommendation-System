"""Versioned Silver snapshots. Raw is read-only; publication follows read-back gates."""
import argparse
import hashlib
import json
import math
import os
import platform
import re
import shutil
import signal
import threading
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from functools import partial
from pathlib import Path
from urllib.parse import urlsplit

from pyspark import SparkContext, StorageLevel
from pyspark.sql import SparkSession, Window, functions as F, types as T
from notebooks.eda_recommendation import (
    FIELDS, MIN_SECONDS, MAX_SECONDS, canonical, digest, inventory, key_value,
    nested_issues, numeric, state, text_value,
)

POLICY_VERSION = "silver_v1"
GIB = 2 ** 30
EXPECTED = {
    "All_Beauty": (701528, 7275, 694253, 112590),
    "Video_Games": (4624615, 53646, 4570969, 137269),
    "Industrial_and_Scientific": (5183005, 52988, 5130017, 427564),
}
S, L, D, B = T.StringType(), T.LongType(), T.DoubleType(), T.BooleanType()
COMMON = [("source_category", S), ("parent_asin", S), ("record_hash", S),
          ("source_path", S), ("line_hash", S), ("quality_flags", T.ArrayType(S)),
          ("field_states", T.MapType(S, S)), ("extras_json", S)]
REVIEW = COMMON + [
    ("user_id", S), ("asin", S), ("timestamp_raw", L), ("rating", D),
    ("verified_purchase", B), ("helpful_vote", L), ("review_title", S),
    ("review_text", S), ("has_review_text", B), ("images_json", S),
]
CATALOG = COMMON + [
    ("product_title", S), ("features", T.ArrayType(S)), ("description", T.ArrayType(S)),
    ("main_category", S), ("store", S), ("price", D), ("price_status", S),
    ("average_rating", D), ("rating_number", L), ("categories_json", S),
    ("images_json", S), ("videos_json", S), ("details_json", S), ("bought_together_json", S),
]
INTERNAL = [("_outcome", S), ("_reasons", T.ArrayType(S)), ("_raw_line", S),
            ("_user_original", S), ("_parent_original", S)]
QUARANTINE = [("dataset_kind", S), ("source_category", S), ("source_path", S),
              ("line_hash", S), ("record_hash", S), ("primary_reason", S),
              ("all_reason_codes", T.ArrayType(S)), ("raw_line", S)]


def schema(fields):
    return T.StructType([T.StructField(name, kind, True) for name, kind in fields])


def scope():
    from ingestion.scripts.dataset_scope import load_scope
    return load_scope()


def parse_line(line, source, kind, category):
    """Pure parser: optional field failures retain a keyed, timed interaction."""
    row = {name: None for name, _ in (REVIEW if kind == "reviews" else CATALOG) + INTERNAL}
    row.update(source_category=category, source_path=source, line_hash=digest(line),
               quality_flags=[], field_states={}, _reasons=[])
    duplicate_keys = []

    def pairs_hook(pairs):
        obj = {}
        for key, value in pairs:
            if key in obj:
                duplicate_keys.append(key)
            obj[key] = value
        return obj

    try:
        obj = json.loads(line, object_pairs_hook=pairs_hook)
        if not isinstance(obj, dict):
            raise ValueError("NON_OBJECT_JSON")
        if duplicate_keys:
            raise ValueError("DUPLICATE_JSON_KEY")
        row["record_hash"] = digest(canonical(obj))
    except (ValueError, RecursionError, UnicodeError) as exc:
        reason = str(exc) if str(exc) in {"NON_OBJECT_JSON", "DUPLICATE_JSON_KEY"} else "MALFORMED_JSON"
        row.update(_outcome="parse_quarantine", _reasons=[reason], _raw_line=line)
        return row

    flags = row["quality_flags"]
    for name, expected in FIELDS[kind].items():
        row["field_states"][name] = state(obj, name)
        value = obj.get(name)
        if value is not None:
            valid_type = type(value) in (int, float) if expected == "number" else type(value).__name__ == expected
            if not valid_type:
                flags.append(name + ":wrong_original_type")
        if isinstance(value, str) and value != value.strip():
            flags.append(name + ":trimmed")
        if name in {"features", "description", "categories", "images", "videos", "bought_together"}:
            flags.extend(name + ":" + issue for issue in sorted(nested_issues(name, value, key_value(obj.get("parent_asin")), kind)))

    def text(name):
        value = obj.get(name)
        return value.strip() if text_value(value) else None

    def number(name, minimum=0, maximum=None, integer=False):
        value = obj.get(name)
        if integer:
            try:
                n = Decimal(str(value)) if type(value) in (int, float, str) else None
            except InvalidOperation:
                n = None
            valid = n is not None and n.is_finite() and minimum <= n <= 2 ** 63 - 1 and n == n.to_integral_value()
            if not valid:
                if value is not None:
                    flags.append(name + ":invalid")
                return None
            return int(n)
        n = numeric(value)
        valid = n is not None and n >= minimum and (maximum is None or n <= maximum)
        if not valid:
            if value is not None:
                flags.append(name + ":invalid")
            return None
        return float(n)

    def nested(name):
        return canonical(obj[name]) if name in obj and obj[name] is not None else None

    row["parent_asin"] = key_value(obj.get("parent_asin"))
    if row["parent_asin"] is None:
        row["_reasons"].append("INVALID_PARENT")
    else:
        row["_parent_original"] = digest(obj["parent_asin"])
    extra = {k: v for k, v in obj.items() if k not in FIELDS[kind]}
    row["extras_json"] = canonical(extra) if extra else None
    if extra:
        flags.append("schema:extra_fields")
    if kind == "reviews":
        row["user_id"] = key_value(obj.get("user_id"))
        if row["user_id"] is None:
            row["_reasons"].append("INVALID_USER")
        else:
            row["_user_original"] = digest(obj["user_id"])
        ts = obj.get("timestamp")
        if type(ts) is not int or not int(MIN_SECONDS * 1000) <= ts < int(MAX_SECONDS * 1000):
            row["_reasons"].append("INVALID_MILLISECOND_TIMESTAMP")
        else:
            row["timestamp_raw"] = ts
        row.update(asin=key_value(obj.get("asin")), rating=number("rating", 1, 5),
                   helpful_vote=number("helpful_vote", integer=True),
                   verified_purchase=obj.get("verified_purchase") if type(obj.get("verified_purchase")) is bool else None,
                   review_title=text("title"), review_text=text("text"),
                   has_review_text=text_value(obj.get("text")), images_json=nested("images"))
        if row["asin"] is None:
            flags.append("asin:invalid")
    else:
        for name in ("features", "description"):
            value = obj.get(name)
            row[name] = [x.strip() for x in value if text_value(x)] if isinstance(value, list) else None
        row.update(product_title=text("title"), main_category=text("main_category"), store=text("store"),
                   price=number("price"), average_rating=number("average_rating", 1, 5),
                   rating_number=number("rating_number", integer=True))
        row["price_status"] = ("zero" if row["price"] == 0 else "valid") if row["price"] is not None else (
            state(obj, "price") if state(obj, "price") != "present" else "invalid")
        for name in ("categories", "images", "videos", "details", "bought_together"):
            row[name + "_json"] = nested(name)
    row["quality_flags"] = sorted(set(flags))
    row["_outcome"] = "core_quarantine" if row["_reasons"] else "accepted"
    row["_raw_line"] = line if row["_reasons"] else None
    return row


def parse_rows(rows, kind, category):
    for row in rows:
        yield parse_line(row.value, row.source, kind, category)


class Hdfs:
    def __init__(self, spark):
        self.spark = spark
        self.Path = spark._jvm.org.apache.hadoop.fs.Path
        self.fs = self.Path("hdfs://namenode:8020/").getFileSystem(spark._jsc.hadoopConfiguration())

    def exists(self, path):
        return self.fs.exists(self.Path(path))

    def bytes(self, path):
        return self.fs.getContentSummary(self.Path(path)).getLength() if self.exists(path) else 0

    def write_json(self, path, value, overwrite=True):
        stream = self.fs.create(self.Path(path), overwrite)
        try:
            stream.write(bytearray((json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode("utf-8")))
        finally:
            stream.close()

    def files(self, path):
        it = self.fs.listFiles(self.Path(path), True)
        rows = []
        while it.hasNext():
            entry = it.next()
            rows.append({"name": entry.getPath().getName(), "bytes": entry.getLen()})
        return rows


def local_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


class Resources:
    """Polling guards are budgets, not hard filesystem quotas."""
    def __init__(self, spark, hdfs, staging, scratch, report, output_budget, scratch_budget, min_free_gib):
        self.spark, self.hdfs, self.staging, self.scratch = spark, hdfs, staging, scratch
        self.report, self.output_budget, self.scratch_budget = report, output_budget, scratch_budget
        self.min_free = int(min_free_gib * GIB)
        self.error = None
        self.phase = "inventory"
        self.samples = []
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self.monitor, daemon=True)

    def sample(self):
        scratch_bytes = 0
        for base, _, files in os.walk(self.scratch):
            for name in files:
                try:
                    scratch_bytes += (Path(base) / name).stat().st_size
                except FileNotFoundError:
                    pass
        row = dict(utc=datetime.now(timezone.utc).isoformat(), phase=self.phase,
                   scratch_bytes=scratch_bytes, output_bytes=self.hdfs.bytes(self.staging),
                   temporary_free_bytes=shutil.disk_usage(self.scratch).free,
                   report_free_bytes=shutil.disk_usage(self.report).free)
        for name in ("current", "peak", "max"):
            path = Path("/sys/fs/cgroup") / ("memory." + name)
            if path.exists():
                value = path.read_text().strip()
                row["memory_" + name + "_bytes"] = int(value) if value.isdigit() else value
        self.samples.append(row)
        with (self.report / "resources.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(row) + "\n")
        if min(row["temporary_free_bytes"], row["report_free_bytes"]) < self.min_free:
            raise RuntimeError("Filesystem free space below reserve")
        if row["scratch_bytes"] > self.scratch_budget or row["output_bytes"] > self.output_budget:
            raise RuntimeError("Silver output/scratch budget exceeded")

    def monitor(self):
        while not self.stop_event.wait(5):
            try:
                self.sample()
            except Exception as exc:
                self.error = exc
                self.spark.sparkContext.cancelAllJobs()
                return

    def checkpoint(self, phase):
        self.phase = phase
        if self.error:
            raise self.error
        print("[SILVER] " + phase, flush=True)
        self.sample()

    def close(self):
        self.stop_event.set()
        self.thread.join(timeout=15)
        if self.thread.is_alive():
            raise RuntimeError("Resource monitor did not stop")
        local_json(self.report / "resources.json", {"samples": self.samples,
                   "measurement": "5-second polling and phase snapshots; not a hard quota"})


def validate_paths(category, input_base, output_base, testing, run_id):
    spec = scope()
    if category not in spec["categories"] or not re.fullmatch(r"run_[A-Za-z0-9_]{8,64}", run_id):
        raise ValueError("Invalid category/run ID")
    for value, layer in ((input_base, "raw"), (output_base, "processed")):
        url = urlsplit(value)
        if url.scheme != "hdfs" or url.netloc != "namenode:8020" or url.query or url.fragment:
            raise ValueError("Expected the existing namenode HDFS namespace")
        if any(part in {".", ".."} for part in url.path.split("/")) or "%" in url.path:
            raise ValueError("Path traversal is not allowed")
        expected = "/project/raw" if layer == "raw" else "/project/processed/silver"
        if testing:
            if not re.fullmatch(r"/project/testing/" + layer + r"/silver/[A-Za-z0-9_-]+", url.path):
                raise ValueError("Fixture paths must stay under /project/testing/{raw,processed}/silver/<id>")
        elif url.path != expected:
            raise ValueError("Production input/output roots are fixed")
    return spec


def source_lineage(files, category, testing):
    if testing:
        return {"source": "testing_fixture"}
    path = Path("/workspace/ingestion/source-manifest.json")
    manifest = json.loads(path.read_text(encoding="utf-8"))
    records = []
    for kind in ("reviews", "metadata"):
        name = ("meta_" if kind == "metadata" else "") + category + ".jsonl"
        entries = [x for x in manifest["files"] if x["filename"].rsplit("/", 1)[-1] == name]
        if len(entries) != 1:
            raise ValueError("Source manifest entry missing/ambiguous")
        entry = entries[0]
        if entry.get("status") not in {"downloaded_verified", "skipped_verified"} or not re.fullmatch(r"[a-f0-9]{64}", entry.get("sha256", "")):
            raise ValueError("Source manifest is not verified")
        if entry["size"] != sum(x["bytes"] for x in files[kind]):
            raise ValueError("Source manifest bytes differ from HDFS inventory")
        records.append({key: entry[key] for key in ("filename", "size", "sha256", "status")})
    return {"manifest_sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "revision": manifest.get("revision"),
            "files": records, "checksum_evidence": "existing source manifest; payload not rehashed by this job"}


def create_spark(scratch, cores, shuffle_partitions):
    if SparkContext._active_spark_context is not None or SparkSession._instantiatedSession is not None:
        raise RuntimeError("Silver must own its Spark session")
    os.environ["PYSPARK_SUBMIT_ARGS"] = "--driver-memory 1g pyspark-shell"
    return (SparkSession.builder.master(f"local[{cores}]").appName("recommendation-silver")
            .config("spark.driver.memory", "1g").config("spark.local.dir", str(scratch))
            .config("spark.sql.caseSensitive", "true").config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
            .config("spark.sql.files.maxPartitionBytes", str(32 * 2 ** 20))
            .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
            .config("spark.ui.retainedJobs", "20").config("spark.ui.retainedStages", "40")
            .config("spark.sql.ui.retainedExecutions", "20").getOrCreate())


def run_silver(category, input_base="hdfs://namenode:8020/project/raw",
               output_base="hdfs://namenode:8020/project/processed/silver", testing=False,
               run_id=None, report_root="/workspace/outputs/processing/silver", cores=2,
               shuffle_partitions=64, min_free_gib=20, output_budget_gib=None, scratch_budget_gib=None,
               fail_before_publish=False):
    run_id = run_id or datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%S%fZ")
    spec = validate_paths(category, input_base, output_base, testing, run_id)
    if cores not in (1, 2) or shuffle_partitions < 1 or not math.isfinite(min_free_gib) or min_free_gib < 0:
        raise ValueError("Invalid runtime configuration")
    if fail_before_publish and not testing:
        raise ValueError("Fault injection is testing-only")
    for budget in (output_budget_gib, scratch_budget_gib):
        if budget is not None and (not math.isfinite(budget) or budget <= 0):
            raise ValueError("Budgets must be positive and finite")
    report = Path(report_root) / run_id
    report.mkdir(parents=True, exist_ok=True)
    if (report / "run.json").exists():
        raise FileExistsError("Local run manifest already exists; never overwrite")
    scratch = Path("/tmp/silver-spark") / run_id
    scratch.mkdir(parents=True, exist_ok=False)
    base = f"{output_base}/{category.lower()}/{spec['version']}"
    staging, final = f"{base}/_staging/{run_id}", f"{base}/{run_id}"
    manifest = dict(status="running", run_id=run_id, category=category, testing=testing,
                    policy_version=POLICY_VERSION, started_at=datetime.now(timezone.utc).isoformat(),
                    final_path=final, staging_path=staging, timezone="UTC", scope=spec,
                    schema={"reviews": schema(REVIEW + [("event_time", T.TimestampType()), ("event_ambiguous", B)]).jsonValue(),
                            "catalog": schema(CATALOG).jsonValue()},
                    code_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                    eda_helpers_sha256=hashlib.sha256((Path(__file__).parents[1] / "notebooks/eda_recommendation.py").read_bytes()).hexdigest(),
                    spark_config={"master": f"local[{cores}]", "driver_memory": "1g", "shuffle_partitions": shuffle_partitions})
    local_json(report / "run.json", manifest)
    spark, hdfs, guard = None, None, None
    cached = []
    published = False
    owns_staging = False
    previous_handler = signal.getsignal(signal.SIGTERM)

    def interrupted(signum, frame):
        raise InterruptedError("Silver interrupted by SIGTERM")

    signal.signal(signal.SIGTERM, interrupted)
    try:
        spark = create_spark(scratch, cores, shuffle_partitions)
        spark.sparkContext.setLogLevel("WARN")
        manifest["schema_sha256"] = digest(canonical(manifest["schema"]))
        manifest["runtime"] = {"python": platform.python_version(), "spark": spark.version,
                               "hadoop_client": spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion()}
        hdfs = Hdfs(spark)
        if hdfs.exists(final) or hdfs.exists(staging):
            raise FileExistsError("Run output already exists; never overwrite")
        files = {kind: inventory(spark, f"{input_base}/{category.lower()}_{kind}/{spec['version']}")
                 for kind in ("reviews", "metadata")}
        manifest["input_inventory"] = files
        manifest["source_lineage"] = source_lineage(files, category, testing)
        raw_bytes = sum(x["bytes"] for entries in files.values() for x in entries)
        output_budget = int(output_budget_gib * GIB) if output_budget_gib is not None else max(64 * 2 ** 20, 2 * raw_bytes)
        scratch_budget = int(scratch_budget_gib * GIB) if scratch_budget_gib is not None else max(128 * 2 ** 20, 4 * raw_bytes)
        manifest["budgets"] = dict(output_bytes=output_budget, scratch_bytes=scratch_budget, min_free_gib=min_free_gib)
        hdfs.fs.mkdirs(hdfs.Path(staging))
        hdfs.write_json(staging + "/_owner.json", {"run_id": run_id}, overwrite=False)
        owns_staging = True
        hdfs.write_json(staging + "/manifest.json", manifest)
        guard = Resources(spark, hdfs, staging, scratch, report, output_budget, scratch_budget, min_free_gib)
        guard.checkpoint("inventory")
        guard.thread.start()
        quality = {"datasets": {}}
        for kind, fields, target in (("reviews", REVIEW, "reviews_clean"), ("metadata", CATALOG, "catalog")):
            guard.checkpoint("parse_" + kind)
            raw = spark.read.text([x["path"] for x in files[kind]]).withColumn("source", F.input_file_name())
            parsed = spark.createDataFrame(raw.rdd.mapPartitions(partial(parse_rows, kind=kind, category=category)), schema(fields + INTERNAL))
            parsed.persist(StorageLevel.DISK_ONLY)
            cached.append(parsed)
            outcomes = {x["_outcome"]: x["count"] for x in parsed.groupBy("_outcome").count().collect()}
            counts = {"input_rows": sum(outcomes.values()), "parse_quarantine": outcomes.get("parse_quarantine", 0),
                      "core_quarantine": outcomes.get("core_quarantine", 0), "accepted_before_dedup": outcomes.get("accepted", 0)}
            flags = parsed.select(F.explode("quality_flags").alias("flag")).groupBy("flag").count().collect()
            counts["quality_flags"] = {x["flag"]: x["count"] for x in flags}
            accepted = parsed.filter("_outcome = 'accepted'")
            for key, original in (("parent_asin", "_parent_original"), *(([("user_id", "_user_original")]) if kind == "reviews" else [])):
                collisions = accepted.groupBy(key).agg(F.countDistinct(original).alias("forms")).filter("forms > 1").count()
                if collisions:
                    raise ValueError("ID trim collision; policy review required")
            q = parsed.filter("_outcome != 'accepted'").select(
                F.lit(kind).alias("dataset_kind"), "source_category", "source_path", "line_hash", "record_hash",
                F.col("_reasons")[0].alias("primary_reason"), F.col("_reasons").alias("all_reason_codes"), F.col("_raw_line").alias("raw_line"))
            q.coalesce(1).write.mode("errorifexists" if kind == "reviews" else "append").parquet(staging + "/quarantine")
            groups = accepted.groupBy("record_hash").agg(F.count("*").alias("occurrences"),
                      F.min(F.struct("source_path", "line_hash")).alias("trace")).filter("occurrences > 1")
            audit = groups.select(F.lit(kind).alias("dataset_kind"), F.lit(category).alias("source_category"),
                                  "record_hash", "occurrences", (F.col("occurrences") - 1).alias("removed_count"),
                                  F.col("trace.source_path").alias("source_path"), F.col("trace.line_hash").alias("line_hash"))
            audit.coalesce(1).write.mode("errorifexists" if kind == "reviews" else "append").parquet(staging + "/duplicate_audit")
            order = Window.partitionBy("record_hash").orderBy("source_path", "line_hash")
            clean = accepted.withColumn("_rank", F.row_number().over(order)).filter("_rank = 1").select(*[n for n, _ in fields])
            clean.persist(StorageLevel.DISK_ONLY)
            cached.append(clean)
            guard.checkpoint("dedup_" + kind)
            if kind == "reviews":
                ties = clean.groupBy("user_id", "parent_asin", "timestamp_raw").count().filter("count > 1").select(
                    "user_id", "parent_asin", "timestamp_raw").withColumn("event_ambiguous", F.lit(True))
                result = clean.join(ties, ["user_id", "parent_asin", "timestamp_raw"], "left").fillna(False, subset=["event_ambiguous"])
                result = result.withColumn("event_time", F.expr("timestamp_millis(timestamp_raw)"))
            else:
                conflicts = clean.groupBy("parent_asin").count().filter("count > 1")
                conflict_count = conflicts.count()
                if conflict_count:
                    samples = clean.join(conflicts.select("parent_asin"), "parent_asin").select(
                        F.sha2("parent_asin", 256).alias("parent_sha256"), "record_hash", "source_path", "line_hash"
                    ).orderBy("parent_sha256", "record_hash").limit(20).collect()
                    diagnostic = {"conflicting_parent_groups": conflict_count, "samples": [x.asDict() for x in samples],
                                  "sample_limit": 20, "note": "Hashes/trace only; no raw item IDs or content"}
                    local_json(report / "metadata_conflicts.json", diagnostic)
                    hdfs.write_json(staging + "/metadata_conflicts.json", diagnostic)
                    raise ValueError("Unresolved metadata parent conflicts; catalog not published")
                result = clean
            partitions = min(shuffle_partitions, max(1, math.ceil(sum(x["bytes"] for x in files[kind]) / (128 * 2 ** 20))))
            guard.checkpoint("write_" + target)
            result.coalesce(partitions).write.mode("errorifexists").option("compression", "snappy").parquet(staging + "/" + target)
            for frame in (clean, parsed):
                frame.unpersist(blocking=True)
                cached.remove(frame)
            guard.checkpoint("readback_" + target)
            readback = spark.read.parquet(staging + "/" + target)
            expected_fields = fields + ([("event_time", T.TimestampType()), ("event_ambiguous", B)] if kind == "reviews" else [])
            if {field.name: field.dataType for field in readback.schema} != dict(expected_fields):
                raise ValueError("Parquet read-back schema types differ from the declared contract")
            stats = readback.agg(F.count("*").alias("rows"), F.countDistinct("record_hash").alias("hashes"),
                                F.count(F.when(F.col("parent_asin").isNull(), 1)).alias("null_parents")).first().asDict()
            if stats["rows"] != stats["hashes"] or stats["null_parents"]:
                raise ValueError("Read-back identity gate failed")
            if kind == "reviews" and readback.filter("user_id IS NULL OR event_time IS NULL OR unix_millis(event_time) != timestamp_raw").limit(1).count():
                raise ValueError("Read-back key/time gate failed")
            removed = spark.read.parquet(staging + "/duplicate_audit").filter(F.col("dataset_kind") == kind).agg(F.sum("removed_count")).first()[0] or 0
            counts.update(clean_rows=stats["rows"], exact_copy_removed=removed)
            if counts["input_rows"] != counts["parse_quarantine"] + counts["core_quarantine"] + removed + stats["rows"]:
                raise ValueError("Row conservation gate failed")
            quality["datasets"][kind] = counts
        guard.checkpoint("coverage")
        reviews = spark.read.parquet(staging + "/reviews_clean")
        catalog = spark.read.parquet(staging + "/catalog")
        if catalog.count() != catalog.select("parent_asin").distinct().count():
            raise ValueError("Catalog read-back not unique")
        joined = reviews.join(catalog.select("parent_asin").withColumn("matched", F.lit(True)), "parent_asin", "left")
        coverage = joined.agg(F.count("*").alias("left_join_rows"), F.count("matched").alias("matched_reviews")).first().asDict()
        if coverage["left_join_rows"] != quality["datasets"]["reviews"]["clean_rows"]:
            raise ValueError("Join multiplied/dropped reviews")
        quality["coverage"] = coverage
        quality["review_identity"] = reviews.agg(F.countDistinct("user_id").alias("users"), F.countDistinct("parent_asin").alias("items"),
            F.countDistinct(F.struct("user_id", "parent_asin")).alias("pairs"),
            F.sum(F.when(F.col("event_ambiguous"), 1).otherwise(0)).alias("ambiguous_events")).first().asDict()
        quarantine_count = spark.read.parquet(staging + "/quarantine").count()
        if quarantine_count != sum(x["parse_quarantine"] + x["core_quarantine"] for x in quality["datasets"].values()):
            raise ValueError("Quarantine read-back count mismatch")
        if not testing:
            actual = (quality["datasets"]["reviews"]["input_rows"], quality["datasets"]["reviews"]["exact_copy_removed"],
                      quality["datasets"]["reviews"]["clean_rows"], quality["datasets"]["metadata"]["clean_rows"])
            if actual != EXPECTED[category] or coverage["matched_reviews"] != actual[2]:
                raise ValueError("Current v1 source differs from reviewed EDA expectations")
        for kind in files:
            after = inventory(spark, f"{input_base}/{category.lower()}_{kind}/{spec['version']}")
            if after != files[kind]:
                raise ValueError("Input inventory changed during processing")
        quality["outputs"] = {}
        for target in ("reviews_clean", "catalog", "quarantine", "duplicate_audit"):
            entries = hdfs.files(staging + "/" + target)
            output_frame = spark.read.parquet(staging + "/" + target)
            quality["outputs"][target] = {"bytes": sum(x["bytes"] for x in entries), "files": entries,
                                          "schema": output_frame.schema.jsonValue(), "schema_sha256": digest(output_frame.schema.json())}
        local_json(report / "quality.json", quality)
        hdfs.write_json(staging + "/quality.json", quality)
        guard.checkpoint("validated")
        if fail_before_publish:
            raise RuntimeError("Injected failure before publish")
        manifest.update(status="complete", finished_at=datetime.now(timezone.utc).isoformat(), quality=quality)
        hdfs.write_json(staging + "/manifest.json", manifest)
        guard.checkpoint("publish_ready")
        guard.close()
        if guard.error:
            raise guard.error
        if hdfs.exists(final) or not hdfs.fs.rename(hdfs.Path(staging), hdfs.Path(final)):
            raise RuntimeError("Final rename failed; run not published")
        published = True
        local_json(report / "run.json", manifest)
        print("[SILVER COMPLETE] " + final, flush=True)
        return manifest
    except BaseException as exc:
        if not published:
            manifest.update(status="interrupted" if isinstance(exc, (KeyboardInterrupt, InterruptedError)) else "failed",
                            error=type(exc).__name__ + ": " + str(exc), finished_at=datetime.now(timezone.utc).isoformat())
            local_json(report / "run.json", manifest)
            if owns_staging and hdfs and hdfs.exists(staging):
                hdfs.write_json(staging + "/manifest.json", manifest)
        raise
    finally:
        try:
            if guard and guard.thread.ident is not None:
                guard.close()
        finally:
            try:
                for frame in reversed(cached):
                    frame.unpersist(blocking=True)
            finally:
                try:
                    if spark:
                        spark.stop()
                finally:
                    signal.signal(signal.SIGTERM, previous_handler)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--category", required=True)
    parser.add_argument("--run-id")
    parser.add_argument("--testing", action="store_true")
    parser.add_argument("--input-base", default="hdfs://namenode:8020/project/raw")
    parser.add_argument("--output-base", default="hdfs://namenode:8020/project/processed/silver")
    parser.add_argument("--report-root", default="/workspace/outputs/processing/silver")
    parser.add_argument("--cores", type=int, choices=(1, 2), default=2)
    parser.add_argument("--shuffle-partitions", type=int, default=64)
    parser.add_argument("--min-free-gib", type=float, default=20)
    parser.add_argument("--output-budget-gib", type=float)
    parser.add_argument("--scratch-budget-gib", type=float)
    run_silver(**vars(parser.parse_args()))


if __name__ == "__main__":
    main()
