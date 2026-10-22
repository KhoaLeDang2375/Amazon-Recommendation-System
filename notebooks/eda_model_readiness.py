"""Read-only Silver diagnostics for recommendation; no encoders, models or Gold writes."""
import argparse
import hashlib
import json
import math
import re
import signal
from datetime import datetime, timezone
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import functions as F

try:
    from notebooks.eda_recommendation import Report, RuntimeMonitor, check_disk_space, create_spark, get_scope
except ModuleNotFoundError:
    from eda_recommendation import Report, RuntimeMonitor, check_disk_space, create_spark, get_scope


FOLDS = (("validation", "2021-01-01", "2022-01-01"), ("test", "2022-01-01", "2023-10-01"))
COUNTS = {"All_Beauty": (694253, 112590), "Video_Games": (4570969, 137269),
          "Industrial_and_Scientific": (5130017, 427564)}
OLD_RUNS = {"All_Beauty": "run_20261014T055611280385Z", "Video_Games": "run_20261014T060728726293Z",
            "Industrial_and_Scientific": "run_20261014T064207289175Z"}
Q = [0.5, 0.9, 0.95, 0.99]


def save_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    temporary.replace(path)


def count_if(condition):
    return F.count(F.when(condition, 1))


def bins(column):
    return (F.when(column == 0, "0").when(column == 1, "1").when(column == 2, "2")
            .when(column < 5, "3-4").otherwise("5+"))


def usable(column):
    return F.coalesce(F.length(F.trim(column)) > 0, F.lit(False))


def content(catalog):
    title = F.coalesce(F.col("product_title"), F.lit(""))
    parts = F.concat(F.array(title), F.coalesce(F.col("features"), F.array().cast("array<string>")),
                     F.coalesce(F.col("description"), F.array().cast("array<string>")))
    return (catalog.withColumn("title_text", title).withColumn("expanded_text", F.concat_ws("\n", parts))
            .withColumn("has_title", usable(F.col("title_text")))
            .withColumn("has_content", usable(F.col("expanded_text")))
            .withColumn("list_elements", F.size(parts) - 1)
            .withColumn("repeated_parts", F.size(parts) > F.size(F.array_distinct(parts))))


def profile_text(catalog, report):
    frame = content(catalog).persist(StorageLevel.DISK_ONLY)
    try:
        texts = frame.select("parent_asin", F.explode(F.array(*[
            F.struct(F.lit(name).alias("field"), expr.alias("text")) for name, expr in (
                ("title", F.col("title_text")), ("features", F.concat_ws("\n", "features")),
                ("description", F.concat_ws("\n", "description")), ("expanded", F.col("expanded_text")))] )).alias("v"))
        texts = texts.select("parent_asin", "v.*").withColumn("chars", F.length("text")).withColumn("bytes", F.length(F.encode("text", "UTF-8")))
        report.table("item_text_profile", texts.groupBy("field").agg(F.count("*").alias("item_denominator"),
            count_if(F.col("chars") > 0).alias("nonempty"), F.expr("percentile_approx(chars, array(0.5,0.9,0.95,0.99), 10000)").alias("chars_p50_p90_p95_p99"),
            F.max("chars").alias("max_chars"), F.expr("percentile_approx(bytes, array(0.5,0.9,0.95,0.99), 10000)").alias("bytes_p50_p90_p95_p99"),
            F.max("bytes").alias("max_bytes"), *[count_if(F.col(metric) > limit).alias(f"{metric}_gt_{limit}")
                for metric in ("chars", "bytes") for limit in (512, 2048, 8192, 32768)]), ["field"])
        checks = {"html_like": r"<[^>]+>", "url": r"(?i)https?://|www\.",
                  "control": r"[\x00-\x08\x0B\x0C\x0E-\x1F]", "no_letters_digits": r"^[^\p{L}\p{N}]+$",
                  "repeated_character": r"(.)\1{19,}"}
        flagged = frame
        for name, pattern in checks.items():
            flagged = flagged.withColumn(name, F.col("expanded_text").rlike(pattern))
        flags = list(checks) + ["repeated_parts"]
        report.table("text_flags", flagged.agg(F.count("*").alias("item_denominator"),
            *[count_if(F.col(name)).alias(name) for name in flags],
            F.percentile_approx("list_elements", Q, 10000).alias("list_elements_p50_p90_p95_p99"),
            F.max("list_elements").alias("max_list_elements")))
        # Bound snippets before collection; do not export complete descriptions or raw IDs.
        sample = flagged.select(F.sha2(F.concat(F.lit("readiness-v1:"), F.col("parent_asin")), 256).alias("item_hash"),
            F.length("expanded_text").alias("chars"), F.substring("title_text", 1, 300).alias("title_excerpt"),
            F.substring("expanded_text", 1, 1000).alias("content_excerpt"), *flags)
        groups = [("representative", sample.orderBy("item_hash").limit(100)),
                  ("long", sample.orderBy(F.desc("chars"), "item_hash").limit(50)),
                  ("flagged", sample.filter(" OR ".join(flags)).orderBy("item_hash").limit(50))]
        selected = {}
        for stratum, selection in groups:
            for row in selection.collect():
                value = row.asDict()
                entry = selected.setdefault(value["item_hash"], {**value, "strata": []})
                entry["strata"].append(stratum)
        save_json(report.directory / "text_samples.local.json", list(selected.values()))
        save_json(report.directory / "text_sample_manifest.json", dict(seed="readiness-v1", selected=len(selected),
            stratum_limits=dict(representative=100, long=50, flagged=50), overlapping_strata=True,
            excerpts_only=True, inference="targeted sample; no population language estimate"))
        report.table("text_extremes", sample.select("item_hash", "chars").orderBy(F.desc("chars"), "item_hash").limit(20))
    finally:
        frame.unpersist(blocking=True)


def diagnostic_fold(events, catalog_features, start, end):
    """Return narrow frames; target rows never carry target rating/text as input."""
    history = events.filter(F.col("event_time") < F.lit(start).cast("timestamp"))
    pairs = history.groupBy("user_id", "parent_asin").agg(F.count("*").alias("events"),
        F.countDistinct("rating").alias("rating_versions"), count_if(F.col("rating").isNull()).alias("null_ratings"))
    candidates = pairs.select("parent_asin").distinct()
    h = history.join(catalog_features, "parent_asin", "left").groupBy("user_id").agg(
        F.count("*").alias("history_events"), F.countDistinct("parent_asin").alias("history_items"),
        F.countDistinct("event_time").alias("history_times"),
        F.countDistinct(F.when(F.col("has_title"), F.col("parent_asin"))).alias("title_items"),
        F.countDistinct(F.when(F.col("has_content"), F.col("parent_asin"))).alias("content_items"),
        count_if(F.col("has_review_text")).alias("review_text_events"),
        count_if(F.col("event_ambiguous")).alias("ambiguous_history_events"))
    window = events.filter((F.col("event_time") >= F.lit(start).cast("timestamp")) & (F.col("event_time") < F.lit(end).cast("timestamp")))
    first = window.groupBy("user_id").agg(F.min("event_time").alias("event_time"))
    next_events = window.join(first, ["user_id", "event_time"], "inner")
    targets = next_events.groupBy("user_id", "parent_asin").agg(count_if(F.col("event_ambiguous")).alias("ambiguous_target_events"),
        F.min("event_time").alias("target_time"))
    targets = (targets.join(candidates.withColumn("in_candidates", F.lit(True)), "parent_asin", "left")
        .join(pairs.select("user_id", "parent_asin").withColumn("repeat", F.lit(True)), ["user_id", "parent_asin"], "left")
        .fillna(False, ["in_candidates", "repeat"]))
    t = targets.groupBy("user_id").agg(F.count("*").alias("targets"), count_if(F.col("in_candidates")).alias("targets_in"),
        count_if(F.col("repeat")).alias("repeat_targets"), F.sum("ambiguous_target_events").alias("ambiguous_target_events"),
        F.min("target_time").alias("first_target_time"))
    users = events.filter(F.col("event_time") < F.lit(end).cast("timestamp")).select("user_id").distinct()
    queries = users.join(h, "user_id", "left").join(t, "user_id", "left").fillna(0)
    queries = queries.withColumn("candidate_group", F.when(F.col("targets") == 0, "no_target")
        .when(F.col("targets_in") == 0, "outside").when(F.col("targets_in") == F.col("targets"), "full").otherwise("partial"))
    return history, pairs, candidates, queries, targets


def analyze_fold(events, catalog_features, fold, policy, start, end, report, checkpoint):
    frames = []
    def keep(frame):
        frame.persist(StorageLevel.DISK_ONLY)
        frames.append(frame)
        # Materialize narrow dependencies in order, before AQE builds shared cache stages.
        frame.count()
        return frame
    prefix = f"{fold}_{policy}_"
    try:
        checkpoint(prefix + "start")
        history, pairs, candidates, queries, targets = diagnostic_fold(events, catalog_features, start, end)
        pairs = keep(pairs)
        candidates = keep(candidates)
        targets = keep(targets)
        queries = keep(queries)
        info = queries.agg(F.count("*").alias("users_before_window_end"),
            count_if(F.col("history_events") > 0).alias("users_with_history"), count_if(F.col("targets") > 0).alias("queries_with_target"),
            count_if((F.col("targets") > 0) & (F.col("history_events") > 0)).alias("personalizable_queries"),
            count_if(F.col("targets") == 0).alias("no_target_users"),
            count_if((F.col("targets") > 0) & (F.col("history_events") == 0)).alias("cold_user_queries"),
            F.coalesce(F.sum("targets"), F.lit(0)).alias("target_pairs"),
            F.coalesce(F.sum("targets_in"), F.lit(0)).alias("targets_in"),
            count_if(F.col("targets") > 1).alias("tied_queries"),
            F.coalesce(F.sum("repeat_targets"), F.lit(0)).alias("repeat_target_pairs"),
            count_if(F.col("ambiguous_target_events") > 0).alias("queries_with_ambiguous_target")).first().asDict()
        if info["personalizable_queries"] + info["cold_user_queries"] != info["queries_with_target"]:
            raise AssertionError("Query population conservation failed")
        report.rows(prefix + "query_population", [{"start_utc": start, "end_utc": end, **info}])
        q = queries.filter("targets > 0")
        report.table(prefix + "history_bins", q.select(F.explode(F.array(*[
            F.struct(F.lit(name).alias("measure"), bins(F.col(name)).alias("bin"))
            for name in ("history_events", "history_items", "history_times")])).alias("v")).select("v.*")
            .groupBy("measure", "bin").count(), ["measure", "bin"])
        report.table(prefix + "candidate_eligibility", q.withColumn("warm_user", F.col("history_events") > 0)
            .groupBy("warm_user", "candidate_group").agg(F.count("*").alias("queries"), F.sum("targets").alias("targets"),
                F.sum("targets_in").alias("targets_in"), F.sum("repeat_targets").alias("repeat_targets")), ["warm_user", "candidate_group"])
        totals = q.agg(F.count("*").alias("queries"), F.coalesce(F.sum("targets"), F.lit(0)).alias("targets"),
            F.coalesce(F.sum("targets_in"), F.lit(0)).alias("inside")).first()
        if (totals.queries, totals.targets, totals.inside) != (info["queries_with_target"], info["target_pairs"], info["targets_in"]):
            raise AssertionError("Target/query eligibility conservation failed")
        report.table(prefix + "target_ties_repeat", q.groupBy((F.col("targets") > 1).alias("tied"),
            (F.col("repeat_targets") > 0).alias("has_repeat"), (F.col("ambiguous_target_events") > 0).alias("ambiguous_target"))
            .agg(F.count("*").alias("queries"), F.sum("targets").alias("target_pairs")))
        report.table(prefix + "feature_eligibility", q.withColumn("history_bin", bins(F.col("history_items")))
            .withColumn("content_bin", bins(F.col("content_items"))).groupBy("history_bin", "content_bin").agg(
                F.count("*").alias("queries"), count_if(F.col("title_items") > 0).alias("queries_with_title_history"),
                count_if(F.col("review_text_events") > 0).alias("queries_with_review_text_history"),
                F.sum("history_items").alias("history_item_total"), F.sum("content_items").alias("content_item_total")), ["history_bin", "content_bin"])
        degree = pairs.groupBy("parent_asin").agg(F.count("*").alias("item_users"))
        train = pairs.agg(F.count("*").alias("positive_pairs"), F.countDistinct("user_id").alias("train_users"),
            F.countDistinct("parent_asin").alias("train_items"), count_if(F.col("events") > 1).alias("repeated_pairs"),
            count_if(F.col("rating_versions") > 1).alias("conflicting_rating_pairs"),
            count_if(F.col("null_ratings") > 0).alias("pairs_with_null_rating"), F.coalesce(F.sum("events"), F.lit(0)).alias("events")).first().asDict()
        train["density"] = train["positive_pairs"] / (train["train_users"] * train["train_items"]) if train["train_users"] and train["train_items"] else None
        report.rows(prefix + "train_support", [train])
        user_degrees = pairs.groupBy("user_id").agg(F.count("*").alias("degree"))
        report.table(prefix + "train_degree_bins", user_degrees.select(F.lit("user").alias("entity"), "degree")
            .unionByName(degree.select(F.lit("item").alias("entity"), F.col("item_users").alias("degree")))
            .withColumn("bin", bins(F.col("degree"))).groupBy("entity", "bin").count(), ["entity", "bin"])
        report.table(prefix + "target_train_support", targets.join(degree, "parent_asin", "left").fillna(0, ["item_users"])
            .withColumn("item_train_degree_bin", bins(F.col("item_users"))).groupBy("item_train_degree_bin").count(), ["item_train_degree_bin"])
        candidate_features = candidates.join(catalog_features, "parent_asin", "left")
        report.table(prefix + "deepfm_field_coverage", candidate_features.agg(F.count("*").alias("candidate_denominator"),
            count_if(F.col("has_title")).alias("with_title"), count_if(F.col("has_content")).alias("with_content"),
            count_if(usable(F.col("main_category"))).alias("with_main_category"), count_if(usable(F.col("store"))).alias("with_store"),
            F.countDistinct("main_category").alias("main_category_cardinality"), F.countDistinct("store").alias("store_cardinality")))
        negatives = user_degrees.withColumn("available", F.lit(train["train_items"]) - F.col("degree"))
        availability = negatives.agg(F.count("*").alias("train_user_denominator"), F.min("available").alias("minimum_unseen"),
            F.percentile_approx("available", Q, 10000).alias("unseen_p50_p90_p95_p99"),
            *[count_if(F.col("available") < n).alias(f"users_below_{n}") for n in (1, 4, 10)]).first().asDict()
        report.rows(prefix + "negative_sampling_feasibility", [{"negatives_per_positive": n, "positive_pairs": train["positive_pairs"],
            "nominal_rows": train["positive_pairs"] * (1 + n), "rule": "distinct train-pair positives; distinct negatives per positive; no future-label exclusion",
            **availability} for n in (1, 4, 10)])
        checkpoint(prefix + "complete")
        sketch = queries.select("user_id", "targets", "history_events", "first_target_time").persist(StorageLevel.DISK_ONLY)
        try:
            sketch.count()
        except BaseException:
            sketch.unpersist(blocking=True)
            raise
        return info, sketch
    finally:
        for frame in reversed(frames):
            frame.unpersist(blocking=True)


def read_manifest(spark, root):
    rows = spark.read.text(root + "/manifest.json").limit(20001).collect()
    if len(rows) > 20000:
        raise ValueError("Silver manifest unexpectedly large")
    return json.loads("\n".join(row.value for row in rows))


def schema_matches(actual, expected):
    # Silver joins may reorder columns; field types/nullability/metadata must still match.
    left = actual["fields"]
    right = expected["fields"]
    return (len(left) == len(right) == len({x["name"] for x in left}) and
            {x["name"]: x for x in left} == {x["name"]: x for x in right})


def validate_paths(category, silver_run, output, run_id):
    if category not in get_scope()["categories"] or not re.fullmatch(r"run_[A-Za-z0-9_]{8,64}", silver_run):
        raise ValueError("Invalid category/Silver run")
    if not re.fullmatch(r"run_[A-Za-z0-9_]{8,64}", run_id):
        raise ValueError("Invalid output run ID")
    root = Path(output).resolve()
    allowed = Path("/workspace/outputs/eda_model_readiness").resolve()
    if root != allowed:
        raise ValueError("Output root must be /workspace/outputs/eda_model_readiness")
    return f"hdfs://namenode:8020/project/processed/silver/{category.lower()}/v1/{silver_run}"


def run_readiness(category, silver_run, run_id=None, output="/workspace/outputs/eda_model_readiness", cores=2,
                  shuffle_partitions=64, min_free_gb=20):
    run_id = run_id or datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%S%fZ")
    source = validate_paths(category, silver_run, output, run_id)
    if not math.isfinite(min_free_gb) or min_free_gb < 0:
        raise ValueError("Reserve must be finite and nonnegative")
    check_disk_space(output, min_free_gb)
    root = Path(output) / run_id
    root.mkdir(parents=True, exist_ok=False)
    report = Report(root / category)
    monitor = RuntimeMonitor(root, min_free_gb)
    info = dict(status="running", run_id=run_id, category=category, silver_run=silver_run, source=source,
        started_at=datetime.now(timezone.utc).isoformat(), timezone="UTC", folds=FOLDS,
        policies={"A": "all reviews", "B": "verified true in history and target search"}, quantile_accuracy=10000,
        population="full Silver category; text semantic review uses bounded sample", no_models=True,
        code_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        helper_sha256=hashlib.sha256(Path(__file__).with_name("eda_recommendation.py").read_bytes()).hexdigest())
    save_json(root / "run.json", info)
    spark = None
    previous = signal.getsignal(signal.SIGTERM)
    def interrupt(signum, frame):
        raise KeyboardInterrupt("Readiness EDA terminated")
    signal.signal(signal.SIGTERM, interrupt)
    try:
        monitor.checkpoint("preflight")
        spark = create_spark(cores, shuffle_partitions)
        info["spark_config"] = {key: spark.conf.get(key) for key in ("spark.master", "spark.driver.memory", "spark.sql.shuffle.partitions", "spark.sql.session.timeZone")}
        manifest = read_manifest(spark, source)
        if manifest.get("status") != "complete" or manifest.get("category") != category or manifest.get("run_id") != silver_run:
            raise ValueError("Silver manifest does not identify a complete selected snapshot")
        info["silver_manifest_sha256"] = hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest()
        reviews = spark.read.parquet(source + "/reviews_clean")
        catalog = spark.read.parquet(source + "/catalog")
        actual = (reviews.count(), catalog.count())
        if actual != COUNTS[category]:
            raise ValueError(f"Silver counts differ from handoff: {actual}")
        expected_schemas = manifest["schema"]
        if not schema_matches(reviews.schema.jsonValue(), expected_schemas["reviews"]) or not schema_matches(catalog.schema.jsonValue(), expected_schemas["catalog"]):
            raise ValueError("Silver schema differs from its manifest")
        save_json(report.directory / "input_contract.json", dict(source=source, reviews=actual[0], catalog=actual[1],
            manifest_sha256=info["silver_manifest_sha256"], utc="UTC", metadata_time="crawl snapshot; effective time unavailable"))
        old = Path("/workspace/outputs/eda") / OLD_RUNS[category] / category
        report.rows("evidence_reuse", [dict(table=name, path=str(old / (name + ".csv")), available=(old / (name + ".csv")).is_file(),
            usage=purpose) for name, purpose in (("rating_by_verified", "raw rating-by-verification context"),
            ("review_text_length", "raw global review length; not recomputed"), ("sparsity", "raw full-history context"),
            ("long_tail", "raw popularity context"), ("matched_content", "raw safe item presence"),
            ("temporal_cold_start", "raw events at 2022; not query coverage"))])
        monitor.checkpoint("text_profile")
        profile_text(catalog, report)
        cf = content(catalog).select("parent_asin", "has_title", "has_content", "main_category", "store").persist(StorageLevel.DISK_ONLY)
        events = reviews.select("user_id", "parent_asin", "event_time", "event_ambiguous", "rating", "verified_purchase", "has_review_text").persist(StorageLevel.DISK_ONLY)
        outcomes = []
        try:
            cf.count()
            events.count()
            for fold, start, end in FOLDS:
                a_info, a_queries = analyze_fold(events, cf, fold, "A", start, end, report, monitor.checkpoint)
                # Keep only IDs and flags for policy comparison; never collect individual queries.
                a = a_queries.persist(StorageLevel.DISK_ONLY)
                try:
                    a.count()
                    b_info, b_queries = analyze_fold(events.filter(F.col("verified_purchase") == True), cf, fold, "B", start, end, report, monitor.checkpoint)
                    try:
                        b = b_queries.select("user_id", F.col("targets").alias("b_targets"), F.col("history_events").alias("b_history"),
                            F.col("first_target_time").alias("b_first_time"))
                        report.table(f"{fold}_verification_policy_impact", a.join(b, "user_id", "left").fillna(0).agg(
                            count_if(F.col("targets") > 0).alias("A_target_query_denominator"),
                            count_if((F.col("targets") > 0) & (F.col("b_targets") == 0)).alias("A_queries_losing_target_in_B"),
                            count_if((F.col("targets") > 0) & (F.col("b_targets") > 0) & ((F.col("targets") != F.col("b_targets")) |
                                (F.col("first_target_time") != F.col("b_first_time")))).alias("retained_queries_with_changed_targets"),
                            count_if((F.col("targets") > 0) & (F.col("history_events") > 0)).alias("A_personalizable_denominator"),
                            count_if((F.col("targets") > 0) & (F.col("history_events") > 0) & ((F.col("b_targets") == 0) | (F.col("b_history") == 0))).alias("A_personalizable_lost_in_B")))
                    finally:
                        b_queries.unpersist(blocking=True)
                finally:
                    a.unpersist(blocking=True)
                outcomes.extend([dict(fold=fold, policy="A", **a_info), dict(fold=fold, policy="B", **b_info)])
        finally:
            events.unpersist(blocking=True)
            cf.unpersist(blocking=True)
        report.rows("query_population_comparison", outcomes)
        decisions = [dict(decision=key, evidence=evidence, status="needs_review", proposal=proposal) for key, evidence, proposal in (
            ("interaction_policy", "verification_policy_impact; query_population_comparison", "Compare retention A/B before choosing; false is not a non-purchase label"),
            ("history_target_candidates", "history_bins; candidate_eligibility", "Fixed-window first timestamp; retain ties/repeats and out-of-candidate labels"),
            ("item_text", "item_text_profile; text_flags; text_samples.local.json", "Choose title-only or expanded after bounded semantic review"),
            ("pair_aggregation", "train_support", "Binary distinct user-item train pairs as initial baseline"),
            ("deepfm_fields", "deepfm_field_coverage; feature_eligibility", "Train IDs + usable content/category/store; history-only user features"),
            ("negative_sampling", "negative_sampling_feasibility", "Configure train-only sampler; do not materialize all negatives"))]
        report.rows("gold_decisions", decisions)
        if read_manifest(spark, source) != manifest:
            raise ValueError("Selected Silver manifest changed during EDA")
        lines = [f"# EDA bổ sung — {category}", "", "Kết quả full Silver; chưa train/encode/tạo Gold.",
            f"Nguồn: {source}", "", "## Query population", "", "| Fold | Policy | Queries có target | Có history | Cold user | Target pairs | In candidates |",
            "|---|---|---:|---:|---:|---:|---:|"]
        for row in outcomes:
            lines.append(f"| {row['fold']} | {row['policy']} | {row['queries_with_target']} | {row['personalizable_queries']} | {row['cold_user_queries']} | {row['target_pairs']} | {row['targets_in']} |")
        lines += ["", "Mỗi mẫu số là query hoặc query–target pair như tên cột; không thay bằng raw review events.",
            "Metadata là crawl snapshot. Mẫu text có chủ đích, không suy tỷ lệ ngôn ngữ toàn bộ.",
            "Rà ngữ nghĩa và chốt policy trong báo cáo tổng hợp; encoder/token cap/cost chưa đo.", "", "## Bảng bằng chứng", ""]
        lines += [f"- [{name}]({name}.csv)" for name in report.tables]
        (report.directory / "BAO_CAO_EDA_BO_SUNG.md").write_text("\n".join(lines), encoding="utf-8")
        monitor.checkpoint("complete")
        info.update(status="complete", checks="Silver schema/counts; query population conservation; bounded aggregate export")
    except BaseException as error:
        info.update(status="interrupted" if isinstance(error, KeyboardInterrupt) else "failed", error=str(error))
        raise
    finally:
        try:
            if spark is not None:
                spark.stop()
        finally:
            signal.signal(signal.SIGTERM, previous)
            info["finished_at"] = datetime.now(timezone.utc).isoformat()
            save_json(root / "run.json", info)
    print(f"[READINESS COMPLETE] {root}", flush=True)
    return root


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--category", required=True)
    parser.add_argument("--silver-run", required=True)
    parser.add_argument("--run-id")
    parser.add_argument("--cores", type=int, default=2)
    parser.add_argument("--shuffle-partitions", type=int, default=64)
    parser.add_argument("--min-free-gb", type=float, default=20)
    run_readiness(**vars(parser.parse_args()))
