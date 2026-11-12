"""G0-G2: immutable temporal Gold and a text-only Kaggle export from pinned Silver."""
import argparse
import hashlib
import html
import json
import math
import re
import shutil
import signal
import unicodedata
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

from pyspark import StorageLevel
from pyspark.sql import Window, functions as F, types as T
from processing.silver import Hdfs, Resources, create_spark, local_json
from notebooks.eda_model_readiness import COUNTS, bins, read_manifest, schema_matches

CLEANING = "gold_text_nfc_html_whitespace_v1"
EXPECTED = {
    "All_Beauty": {"validation": (495387, 69848, 2681), "test": (618845, 32466, 1751)},
    "Video_Games": {"validation": (3511273, 242638, 44239), "test": (3984667, 161663, 36893)},
    "Industrial_and_Scientific": {"validation": (3237493, 394153, 72503), "test": (4071315, 325569, 74055)},
}


def file_hash(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 ** 2), b""):
            digest.update(block)
    return digest.hexdigest()


def clean_text(value):
    if not value:
        return ""
    text = re.sub(r"(?is)<(script|style)\b[^>]*>.*?</\1\s*>", " ", value)
    text = re.sub(r"<!--.*?-->", " ", text, flags=re.S)
    text = re.sub(r"(?i)</?(?:p|br|div|span|li|ul|ol|b|strong|em|i|table|tr|td|th|a|h[1-6])\b[^>]*>", " ", text)
    return " ".join(unicodedata.normalize("NFC", html.unescape(text)).split())


def review_text(title, body):
    a, b = clean_text(title), clean_text(body)
    return "\n".join(x for x in (a, b if b != a else "") if x)


def item_text(title, description):
    # Keep source order, remove exact cleaned duplicate paragraphs (including title).
    parts = dict.fromkeys(clean_text(x) for x in [title, *(description or [])])
    return "\n".join(x for x in parts if x)


def description_profile(catalog):
    """Measure the entire candidate catalog, without collecting full product text."""
    prepare = F.udf(item_text, T.StringType())
    clean = F.udf(clean_text, T.StringType())
    frame = catalog.select("parent_asin", clean("product_title").alias("title"),
        F.concat_ws("\n", F.transform("description", lambda x: x)).alias("raw_description"),
        prepare(F.lit(None).cast("string"), "description").alias("description"),
        prepare("product_title", "description").alias("combined")).persist(StorageLevel.DISK_ONLY)
    try:
        stats = frame.agg(F.count("*").alias("items"),
            F.sum((F.length("description") > 0).cast("long")).alias("with_description"),
            F.sum(((F.length("description") > 0) & (F.col("combined") == F.col("title"))).cast("long")).alias("description_only_repeats_title"),
            F.sum((F.col("combined") != F.col("title")).cast("long")).alias("description_adds_text"),
            F.sum(F.col("raw_description").rlike("(?i)<[a-z][^>]*>").cast("long")).alias("raw_description_has_html"),
            F.sum(F.length("combined")).alias("combined_characters"), F.sum(F.length("title")).alias("title_characters"),
            F.max(F.length("combined")).alias("max_combined_characters"),
            F.percentile_approx(F.length("combined"), [0.5, 0.95, 0.99], 10000).alias("combined_characters_p50_p95_p99")
        ).first().asDict()
        stats["description_characters_p50_p95_p99"] = frame.filter(F.length("description") > 0).agg(
            F.percentile_approx(F.length("description"), [0.5, 0.95, 0.99], 10000)).first()[0]
        # Deterministic examples for manual inspection; report stays local, outside Kaggle ZIP.
        stats["samples"] = [r.asDict() for r in frame.filter(F.col("combined") != F.col("title")).orderBy(
            F.sha2("parent_asin", 256)).select("parent_asin", "title", F.substring("description", 1, 1800).alias("description_excerpt")).limit(8).collect()]
        stats["scope"] = "all refit candidate items; metadata snapshot has no historical description versions"
        return stats
    finally:
        frame.unpersist(blocking=True)


def text_id(text):
    return hashlib.sha256((CLEANING + "\0" + text).encode("utf-8")).hexdigest() if text else None


def read_contract(protocol_path=None):
    directory = Path(__file__).parent
    protocol = json.loads(Path(protocol_path or directory / "gold_protocol.json").read_text(encoding="utf-8"))
    contract = json.loads((directory / "gold_contract.json").read_text(encoding="utf-8"))
    if (protocol.get("status"), protocol.get("protocol_id"), protocol.get("timezone")) != ("locked", contract["protocol_id"], "UTC"):
        raise ValueError("Gold requires the locked UTC protocol")
    for fold in ("validation", "test"):
        start, end = (datetime.fromisoformat(protocol[fold + "_" + name]) for name in ("start", "end"))
        if start >= end:
            raise ValueError("Invalid fold bounds")
    if protocol["validation_end"] > protocol["test_start"] or contract["text"]["cleaning_version"] != CLEANING:
        raise ValueError("Contract/protocol mismatch")
    return protocol, contract


def validate_location(value, production, testing, layer):
    url = urlsplit(value)
    if url.scheme != "hdfs" or url.netloc != "namenode:8020" or url.query or url.fragment or "%" in value:
        raise ValueError("Expected the existing HDFS namespace")
    if any(p in (".", "..") for p in url.path.split("/")):
        raise ValueError("Path traversal")
    expected = r"/project/testing/processed/gold/[A-Za-z0-9_-]+" if testing else re.escape(production)
    if not re.fullmatch(expected, url.path):
        raise ValueError(f"Invalid {layer} root")


def identify(frame, column, fields, category, fold):
    return frame.withColumn(column, F.sha2(F.to_json(F.struct(
        F.lit(category).alias("category"), F.lit(fold).alias("fold"), *[F.col(x) for x in fields])), 256))


def mapping(frame, key, index, partitions):
    # Distributed lexical rank avoids collecting millions of ID labels into the driver.
    ids = frame.select(key).distinct()
    if ids.count() >= 2 ** 31:
        raise ValueError("ALS IDs exceed signed Int capacity")
    ordered = ids.rdd.map(lambda r: r[0]).sortBy(lambda value: value, numPartitions=partitions)
    return frame.sparkSession.createDataFrame(ordered.zipWithIndex(), T.StructType([
        T.StructField(key, T.StringType(), False), T.StructField(index, T.IntegerType(), False)]))


def build_fold(events, category, fold, start, end, partitions, emit, checkpoint):
    cached = []
    def keep(frame):
        frame.persist(StorageLevel.DISK_ONLY)
        cached.append(frame)
        frame.count()
        return frame
    try:
        history = keep(events.filter(F.col("event_time") < F.lit(start).cast("timestamp")))
        emit("history_events", history, ["record_hash"])
        pairs = keep(history.groupBy("user_id", "parent_asin").agg(F.min("event_time").alias("first_pair_time"), F.count("*").alias("event_count")))
        users = keep(mapping(pairs, "user_id", "user_idx", partitions))
        items = keep(mapping(pairs, "parent_asin", "item_idx", partitions))
        emit("users", users, ["user_id"])
        emit("items", items, ["parent_asin"])
        indexed = keep(pairs.join(users, "user_id").join(items, "parent_asin").withColumn("strength", F.lit(1.0)))
        if indexed.count() != pairs.count():
            raise ValueError("Mappings changed pair count")
        emit("train_pairs", indexed, ["user_id", "parent_asin"])
        emit("candidates", items, ["parent_asin"])
        first_history = history.groupBy("user_id").agg(F.min("event_time").alias("first_history_time"))
        examples = indexed.join(first_history, "user_id").withColumn("prediction_time", F.col("first_pair_time")).withColumn(
            "history_empty", F.col("first_history_time") == F.col("prediction_time")).drop("first_history_time")
        examples = identify(examples, "example_id", ["user_id", "parent_asin", "prediction_time"], category, fold)
        emit("train_examples", examples, ["example_id"])
        support = keep(history.groupBy("user_id").agg(F.count("*").alias("history_events"),
            F.countDistinct("parent_asin").alias("history_items"), F.countDistinct("event_time").alias("history_times")))
        window = events.filter((F.col("event_time") >= F.lit(start).cast("timestamp")) & (F.col("event_time") < F.lit(end).cast("timestamp")))
        first = window.groupBy("user_id").agg(F.min("event_time").alias("event_time"))
        next_events = window.join(first, ["user_id", "event_time"])
        target = keep(next_events.groupBy("user_id", "parent_asin", "event_time").agg(
            F.min("record_hash").alias("target_record_hash"), F.count("*").alias("target_event_versions")))
        q = target.select("user_id").distinct().join(support, "user_id", "left").join(users, "user_id", "left").fillna(0, ["history_events", "history_items", "history_times"])
        q = identify(q.withColumn("prediction_time", F.lit(start).cast("timestamp")), "query_id", ["user_id", "prediction_time"], category, fold)
        q = keep(q.withColumn("unknown_user", F.col("user_idx").isNull()))
        emit("queries", q, ["query_id"])
        labels = keep(target.join(q.select("user_id", "query_id"), "user_id").join(items, "parent_asin", "left")
            .join(pairs.select("user_id", "parent_asin").withColumn("is_repeat", F.lit(True)), ["user_id", "parent_asin"], "left")
            .withColumn("in_candidates", F.col("item_idx").isNotNull()).fillna(False, ["is_repeat"])
            .select("query_id", "parent_asin", "item_idx", F.col("event_time").alias("target_time"), "target_record_hash", "target_event_versions", "in_candidates", "is_repeat"))
        emit("labels", labels, ["query_id", "parent_asin"])
        label_stats = labels.groupBy("query_id").agg(F.count("*").alias("target_count"),
            F.sum(F.col("in_candidates").cast("long")).alias("targets_in"), F.sum(F.col("is_repeat").cast("long")).alias("repeat_targets"))
        cohorts = q.join(label_stats, "query_id").withColumn("has_history", F.col("history_events") > 0).withColumn(
            "warm_full", F.col("has_history") & (F.col("targets_in") == F.col("target_count"))).withColumn(
            "candidate_group", F.when(F.col("targets_in") == 0, "outside").when(F.col("targets_in") == F.col("target_count"), "full").otherwise("partial"))
        for column in ("history_events", "history_items", "history_times"):
            cohorts = cohorts.withColumn(column + "_bin", bins(F.col(column)))
        cohorts = keep(cohorts)
        emit("cohorts", cohorts, ["query_id"])
        population = cohorts.agg(F.count("*").alias("queries"), F.sum(F.col("warm_full").cast("long")).alias("warm_full_queries"),
            F.sum(F.col("unknown_user").cast("long")).alias("cold_user_queries"), F.sum("target_count").alias("target_pairs"),
            F.sum("targets_in").alias("targets_in_candidates")).first().asDict()
        population = {key: value or 0 for key, value in population.items()}
        population.update(train_pairs=indexed.count(), train_users=users.count(), train_items=items.count(), history_events=history.count(), start=start, end=end)
        checkpoint(fold + ":checked")
        return population
    finally:
        for frame in reversed(cached):
            frame.unpersist(blocking=True)


def export_text(reviews, catalog, cutoff, candidate_items, emit, package, manifest, checkpoint, max_records, export_budget):
    prepare_item = F.udf(item_text, T.StringType())
    review = F.udf(review_text, T.StringType())
    cached = []
    def keep(frame):
        frame.persist(StorageLevel.DISK_ONLY)
        cached.append(frame)
        frame.count()
        return frame
    def with_id(frame):
        return frame.withColumn("text_id", F.when(F.length("text") > 0, F.sha2(F.concat(F.lit(CLEANING + "\0"), F.col("text")), 256)))
    try:
        item = keep(with_id(catalog.join(candidate_items.select("parent_asin"), "parent_asin")
            .select("parent_asin", F.col("record_hash").alias("source_record_hash"), prepare_item("product_title", "description").alias("text"))))
        past = reviews.filter(F.col("event_time") < F.lit(cutoff).cast("timestamp"))
        rev = keep(with_id(past.select("record_hash", "user_id", "parent_asin", "event_time", review("review_title", "review_text").alias("text"))))
        for name, frame, key in (("item_text_refs", item, "parent_asin"), ("review_text_refs", rev, "record_hash")):
            emit(name, frame.drop("text").withColumn("has_text", F.col("text_id").isNotNull()), [key])
        union = item.select("text_id", "text").withColumn("kind", F.lit("item")).unionByName(rev.select("text_id", "text").withColumn("kind", F.lit("review")))
        texts = keep(union.filter("text_id IS NOT NULL").groupBy("text_id", "text").agg(
            F.concat_ws("+", F.sort_array(F.collect_set("kind"))).alias("kind")).withColumn("characters", F.length("text")))
        total = texts.count()
        if texts.select("text_id").distinct().count() != total:
            raise ValueError("Text hash collision")
        package.mkdir(parents=True, exist_ok=False)
        checkpoint("text:write_export")
        parts = max(1, min(64, math.ceil(total / max_records)))
        texts.repartition(parts, "text_id").sortWithinPartitions("text_id").write.mode("error").option("maxRecordsPerFile", max_records).parquet((package / "texts").as_uri())
        if reviews.sparkSession.read.parquet((package / "texts").as_uri()).count() != total:
            raise ValueError("Text export count differs on read-back")
        length_bin = F.when(F.col("characters") <= 128, "1_short").when(F.col("characters") <= 512, "2_medium").when(F.col("characters") <= 2048, "3_long").otherwise("4_tail")
        sample_ids = texts.select("text_id", "kind", length_bin.alias("length_bin")).withColumn("position", F.row_number().over(
            Window.partitionBy("kind", "length_bin").orderBy("text_id"))).filter("position <= 16").select("text_id")
        texts.join(sample_ids, "text_id").coalesce(1).write.mode("error").parquet((package / "pilot").as_uri())
        profile = [r.asDict() for r in texts.groupBy("kind").agg(F.count("*").alias("texts"), F.max("characters").alias("max_characters"),
            F.percentile_approx("characters", [0.5, 0.95, 0.99], 10000).alias("characters_p50_p95_p99")).collect()]
        import pyarrow.parquet as pq
        files = []
        for path in sorted((package / "texts").glob("*.parquet")):
            files.append(dict(path=path.relative_to(package).as_posix(), bytes=path.stat().st_size, rows=pq.ParquetFile(path).metadata.num_rows, sha256=file_hash(path)))
        pilots = [dict(path=p.relative_to(package).as_posix(), bytes=p.stat().st_size, rows=pq.ParquetFile(p).metadata.num_rows, sha256=file_hash(p)) for p in sorted((package / "pilot").glob("*.parquet"))]
        if sum(f["rows"] for f in files) != total or sum(f["bytes"] for f in files + pilots) > export_budget:
            raise ValueError("Export count/budget mismatch")
        result = dict(status="pending_gold_publish", export_version="gold_text_export_v1", **manifest, cleaning_version=CLEANING,
            texts=total, files=files, pilot_files=pilots, character_profile=profile, item_text_policy="title_description_exact_paragraph_dedup_v1",
            item_references=item.count(), review_references=rev.count(), item_blank=item.filter("text_id IS NULL").count(), review_blank=rev.filter("text_id IS NULL").count(),
            contains="unique cleaned text and text_id only; no user IDs, ratings or evaluation labels", model_contract=read_contract()[1]["encoder"])
        local_json(package / "export_manifest.json", result)
        (package / "README.txt").write_text("Upload thư mục này hoặc file ZIP sang Kaggle Dataset riêng tư. Mở notebook 05_kaggle_text_embeddings.ipynb. Giữ manifest, texts/ và pilot/. Không kèm refs/labels từ HDFS.\n", encoding="utf-8")
        return result
    finally:
        for frame in reversed(cached):
            frame.unpersist(blocking=True)


def run_gold(category, run_id=None, cores=2, shuffle_partitions=64, min_free_gib=20, output_budget_gib=None, scratch_budget_gib=None,
             testing=False, fixture_source=None, output_base="hdfs://namenode:8020/project/processed/gold", fail_before_publish=False, text_only_from_run=None):
    protocol, contract = read_contract()
    if category not in COUNTS or cores not in (1, 2) or shuffle_partitions < 1:
        raise ValueError("Invalid category/runtime")
    for value in (min_free_gib, output_budget_gib, scratch_budget_gib):
        if value is not None and (not math.isfinite(value) or value < 0):
            raise ValueError("Budgets must be finite and nonnegative")
    if (fixture_source or fail_before_publish) and not testing:
        raise ValueError("Fixture overrides are testing-only")
    validate_location(output_base, "/project/processed/gold", testing, "output")
    selected = next(x for x in protocol["evidence"] if x["category"] == category)
    source = f"hdfs://namenode:8020/project/processed/silver/{category.lower()}/v1/{selected['silver_run']}"
    if testing:
        if not fixture_source:
            raise ValueError("Testing needs a fixture source")
        validate_location(fixture_source, "", True, "input")
        source = fixture_source
    run_id = run_id or datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%S%fZ")
    if not re.fullmatch(r"run_[A-Za-z0-9_]{8,64}", run_id):
        raise ValueError("Invalid run ID")
    report = Path("/workspace/outputs/processing/gold") / run_id
    report.mkdir(parents=True, exist_ok=True)
    if (report / "run.json").exists() or (report / "kaggle_texts").exists():
        raise FileExistsError("Local Gold run already exists; never overwrite")
    scratch = Path("/tmp/gold-spark") / run_id
    scratch.mkdir(parents=True, exist_ok=False)
    base = f"{output_base}/{category.lower()}/v1"
    staging, final = f"{base}/_staging/{run_id}", f"{base}/{run_id}"
    payload = staging + "/" + protocol["protocol_id"]
    final_path = final + "/" + protocol["protocol_id"]
    info = dict(status="running", category=category, run_id=run_id, testing=testing, source=source,
        final_path=final_path, staging_path=staging, started_at=datetime.now(timezone.utc).isoformat(),
        protocol=protocol, contract=contract, code_sha256=file_hash(__file__),
        contract_sha256=file_hash(Path(__file__).with_name("gold_contract.json")),
        protocol_sha256=file_hash(Path(__file__).with_name("gold_protocol.json")),
        helper_sha256={p: file_hash(Path(__file__).parents[1] / p) for p in ("processing/silver.py", "notebooks/eda_model_readiness.py")}, tables={}, folds={})
    local_json(report / "run.json", info)
    spark, guard, events = None, None, None
    published = False
    owns_staging = False
    previous = signal.getsignal(signal.SIGTERM)
    def interrupted(signum, frame):
        raise InterruptedError("Gold terminated")
    signal.signal(signal.SIGTERM, interrupted)
    try:
        spark = create_spark(scratch, cores, shuffle_partitions)
        spark.sparkContext.setLogLevel("WARN")
        hdfs = Hdfs(spark)
        if hdfs.exists(staging) or hdfs.exists(final):
            raise FileExistsError("Never overwrite a Gold run")
        silver = read_manifest(spark, source)
        if silver.get("status") != "complete" or silver.get("category") != category:
            raise ValueError("Silver must be complete and match category")
        source_hash = hashlib.sha256(json.dumps(silver, sort_keys=True).encode()).hexdigest()
        if not testing and (silver.get("run_id") != selected["silver_run"] or source_hash != selected["silver_manifest_sha256"]):
            raise ValueError("Silver differs from pinned protocol")
        reviews = spark.read.parquet(source + "/reviews_clean")
        catalog = spark.read.parquet(source + "/catalog")
        if not schema_matches(reviews.schema.jsonValue(), silver["schema"]["reviews"]) or not schema_matches(catalog.schema.jsonValue(), silver["schema"]["catalog"]):
            raise ValueError("Silver schema mismatch")
        actual = (reviews.count(), catalog.count())
        if not testing and actual != COUNTS[category]:
            raise ValueError("Silver counts differ from handoff")
        source_bytes = hdfs.bytes(source)
        output_budget = int((output_budget_gib * 2 ** 30) if output_budget_gib is not None else max(2 ** 30, source_bytes * 4))
        scratch_budget = int((scratch_budget_gib * 2 ** 30) if scratch_budget_gib is not None else max(2 ** 30, source_bytes * 8))
        hdfs.fs.mkdirs(hdfs.Path(staging))
        owns_staging = True
        hdfs.write_json(staging + "/owner.json", {"run_id": run_id}, overwrite=False)
        guard = Resources(spark, hdfs, staging, scratch, report, output_budget, scratch_budget, min_free_gib)
        guard.thread.start()
        info.update(source_manifest_sha256=source_hash, input_counts=actual, source_bytes=source_bytes,
            output_budget_bytes=output_budget, scratch_budget_bytes=scratch_budget, spark_version=spark.version)
        local_json(report / "run.json", info)

        def emit(name, frame, keys, folder=payload):
            guard.checkpoint("gold:" + name)
            count = frame.count()
            frame.write.mode("error").option("maxRecordsPerFile", 250000).parquet(folder + "/" + name)
            back = spark.read.parquet(folder + "/" + name)
            if back.count() != count or back.groupBy(*keys).count().filter("count > 1").limit(1).count():
                raise ValueError("Read-back count/key failure: " + name)
            if back.filter(" OR ".join(f"`{key}` IS NULL" for key in keys)).limit(1).count():
                raise ValueError("Null key: " + name)
            if frame.schema.simpleString() != back.schema.simpleString():
                raise ValueError("Read-back schema failure: " + name)
            info["tables"][folder.removeprefix(payload).strip("/") + "/" + name] = dict(rows=count, schema=back.schema.jsonValue(), bytes=hdfs.bytes(folder + "/" + name))

        fold_path = payload
        if text_only_from_run:
            if testing or not re.fullmatch(r"run_[A-Za-z0-9_]{8,64}", text_only_from_run):
                raise ValueError("Invalid production parent run")
            parent_path = f"{base}/{text_only_from_run}/{protocol['protocol_id']}"
            parent = read_manifest(spark, parent_path)
            if parent.get("status") != "complete" or parent.get("category") != category or parent.get("source_manifest_sha256") != source_hash or parent.get("protocol") != protocol or parent.get("parent_gold_path"):
                raise ValueError("Parent must be a complete original Gold with matching Silver/protocol")
            info.update(parent_gold_path=parent_path, phase="G2_text_refresh", folds=parent["folds"])
            fold_path = parent_path
        events = reviews.select("record_hash", "user_id", "parent_asin", "event_time").persist(StorageLevel.DISK_ONLY)
        events.count()
        if events.filter("record_hash IS NULL OR user_id IS NULL OR parent_asin IS NULL OR event_time IS NULL").limit(1).count():
            raise ValueError("Invalid core event")
        for fold in (() if text_only_from_run else ("validation", "test")):
            folder = payload + "/" + fold
            outcome = build_fold(events, category, fold, protocol[fold + "_start"], protocol[fold + "_end"], shuffle_partitions,
                lambda name, frame, keys: emit(name, frame, keys, folder), guard.checkpoint)
            if not testing and (outcome["train_pairs"], outcome["queries"], outcome["warm_full_queries"]) != EXPECTED[category][fold]:
                raise ValueError("Gold population differs from cutoff EDA")
            info["folds"][fold] = outcome
            hdfs.write_json(folder + "/sampler.json", contract["sampler"])
            local_json(report / "run.json", info)
        candidate_items = spark.read.parquet(fold_path + "/test/candidates")
        info["description_assessment"] = description_profile(catalog.join(candidate_items.select("parent_asin"), "parent_asin"))
        local_json(report / "description_assessment.json", info["description_assessment"])
        package = report / "kaggle_texts"
        export = export_text(reviews, catalog, protocol["test_start"], candidate_items, emit, package,
            dict(category=category, gold_run=run_id, gold_path=final_path, protocol_id=protocol["protocol_id"],
                 source_manifest_sha256=source_hash, contract_sha256=info["contract_sha256"]), guard.checkpoint, 25000, output_budget)
        info["text_export"] = export
        if read_manifest(spark, source) != silver:
            raise ValueError("Silver manifest changed")
        guard.checkpoint("gold:validated")
        if fail_before_publish:
            raise RuntimeError("Injected failure before Gold publish")
        info.update(status="complete", finished_at=datetime.now(timezone.utc).isoformat())
        export["status"] = "complete"
        hdfs.write_json(payload + "/manifest.json", info)
        guard.close()
        if guard.error or hdfs.exists(final) or not hdfs.fs.rename(hdfs.Path(staging), hdfs.Path(final)):
            raise RuntimeError("Gold publish failed")
        published = True
        local_json(package / "export_manifest.json", export)
        # Only the text package enters ZIP; local source references remain in HDFS.
        archive = report / f"{category.lower()}_kaggle_texts.zip"
        with zipfile.ZipFile(archive, "x", compression=zipfile.ZIP_STORED, allowZip64=True) as stream:
            for path in sorted(package.rglob("*")):
                if path.is_file() and not path.name.startswith((".", "_")):
                    stream.write(path, path.relative_to(package).as_posix())
        info["archive"] = dict(path=str(archive), bytes=archive.stat().st_size, sha256=file_hash(archive))
        local_json(report / "run.json", info)
        print("[GOLD COMPLETE] " + final_path, flush=True)
        return info
    except BaseException as exc:
        info.update(status="complete_export_failed" if published else "failed", error=f"{type(exc).__name__}: {exc}", finished_at=datetime.now(timezone.utc).isoformat())
        local_json(report / "run.json", info)
        if owns_staging and not published:
            hdfs.write_json(staging + "/manifest.json", info)
        raise
    finally:
        try:
            if guard and guard.thread.ident is not None:
                guard.close()
        finally:
            if events is not None:
                events.unpersist(blocking=True)
            if spark is not None:
                spark.stop()
            signal.signal(signal.SIGTERM, previous)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--category", required=True)
    parser.add_argument("--run-id")
    parser.add_argument("--cores", type=int, default=2)
    parser.add_argument("--shuffle-partitions", type=int, default=64)
    parser.add_argument("--min-free-gib", type=float, default=20)
    parser.add_argument("--output-budget-gib", type=float)
    parser.add_argument("--scratch-budget-gib", type=float)
    parser.add_argument("--text-only-from-run")
    print(run_gold(**vars(parser.parse_args()))["final_path"])
