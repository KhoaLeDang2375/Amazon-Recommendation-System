"""Small, reproducible implicit ALS experiment on a read-only Silver snapshot."""
import argparse
import hashlib
import json
import math
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark import StorageLevel
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.sql import functions as F

from eda_model_readiness import COUNTS, read_manifest, save_json, schema_matches
from eda_recommendation import RuntimeMonitor, check_disk_space, create_spark, get_scope


def sample_users(events, size, seed):
    """Select IDs without conditioning on history length or future labels."""
    users = events.select("user_id").distinct()
    return users.orderBy(F.sha2(F.concat_ws(":", F.lit(str(seed)), "user_id"), 256), "user_id").limit(size)


def split_fold(events, start, end):
    train = events.filter(F.col("event_time") < F.to_timestamp(F.lit(start)))
    pairs = train.select("user_id", "parent_asin").distinct()
    window = events.filter((F.col("event_time") >= F.to_timestamp(F.lit(start))) &
                           (F.col("event_time") < F.to_timestamp(F.lit(end))))
    first = window.groupBy("user_id").agg(F.min("event_time").alias("event_time"))
    targets = window.join(first, ["user_id", "event_time"]).select("user_id", "parent_asin").distinct()
    return pairs, targets


def query_population(pairs, targets):
    candidates = pairs.select("parent_asin").distinct().withColumn("in_candidates", F.lit(1))
    support = pairs.groupBy("user_id").agg(F.count("*").alias("history_items"))
    return (targets.join(candidates, "parent_asin", "left").groupBy("user_id")
            .agg(F.sort_array(F.collect_set("parent_asin")).alias("labels"),
                 F.count("*").alias("target_count"), F.count("in_candidates").alias("targets_in"))
            .join(support, "user_id", "left").fillna(0, ["history_items"])
            .withColumn("warm", (F.col("history_items") > 0) & (F.col("targets_in") == F.col("target_count"))))


def query_scores(queries, recommendations, k):
    """Keep every labeled query, including missing predictions and unreachable targets."""
    scored = (queries.join(recommendations, "user_id", "left")
              .withColumn("predictions", F.coalesce(F.col("predictions"), F.array().cast("array<string>")))
              .withColumn("top_k", F.slice("predictions", 1, k)))
    dcg = "aggregate(transform(top_k, (x, i) -> IF(array_contains(labels, x), 1D / log2(i + 2D), 0D)), 0D, (a, b) -> a + b)"
    idcg = f"aggregate(sequence(1, least(size(labels), {k})), 0D, (a, i) -> a + 1D / log2(i + 1D))"
    return (scored.withColumn("recall", F.size(F.array_intersect("top_k", "labels")) / F.size("labels"))
            .withColumn("ndcg", F.expr(dcg) / F.expr(idcg)))


def evaluate(queries, recommendations, model_name):
    rows = []
    for k in (10, 20):
        scored = query_scores(queries, recommendations, k)
        for cohort in ("all_queries", "warm_full_targets"):
            selected = scored if cohort == "all_queries" else scored.filter("warm")
            result = selected.agg(F.count("*").alias("queries"), F.avg("recall").alias("recall"),
                F.avg("ndcg").alias("ndcg"),
                F.sum(F.when(F.size("top_k") > 0, 1).otherwise(0)).alias("queries_with_recommendations")).first().asDict()
            rows.append(dict(model=model_name, cohort=cohort, k=k, **result))
    return rows


def fit_fold(spark, events, root, start, end, rank, max_iter, reg_param, alpha, seed, checkpoint):
    root.mkdir()
    frames = []

    def keep(frame):
        frame.persist(StorageLevel.DISK_ONLY)
        frames.append(frame)
        frame.count()
        return frame

    def elapsed(fn):
        started = time.monotonic()
        value = fn()
        return value, round(time.monotonic() - started, 3)

    try:
        pairs, targets = split_fold(events, start, end)
        pairs, targets = keep(pairs), keep(targets)
        pair_count, target_count = pairs.count(), targets.count()
        if not pair_count or not target_count:
            raise ValueError("Empty train or held-out targets; increase the user sample without choosing by model metrics")
        queries = keep(query_population(pairs, targets))
        population = queries.agg(F.count("*").alias("queries"), F.sum("target_count").alias("targets"),
            F.sum("targets_in").alias("targets_in_candidates"),
            F.sum(F.when(F.col("history_items") == 0, 1).otherwise(0)).alias("cold_user_queries"),
            F.sum(F.col("warm").cast("long")).alias("warm_full_target_queries"),
            F.sum(F.when(F.col("target_count") > 1, 1).otherwise(0)).alias("tied_queries")).first().asDict()
        checkpoint(root.name + ":mapping")
        indexed = pairs
        for column, output in (("user_id", "user_idx"), ("parent_asin", "item_idx")):
            indexer = StringIndexer(inputCol=column, outputCol=output, stringOrderType="alphabetAsc").fit(pairs)
            if len(indexer.labels) >= 2 ** 31:
                raise ValueError("ALS IDs exceed signed Int range")
            indexed = indexer.transform(indexed).withColumn(output, F.col(output).cast("int"))
        indexed = keep(indexed.withColumn("strength", F.lit(1.0)))
        users = keep(indexed.select("user_id", "user_idx").distinct())
        items = keep(indexed.select("parent_asin", "item_idx").distinct())
        if indexed.count() != pair_count:
            raise ValueError("Mapping changed the distinct training pair count")
        for name, frame in (("train_pairs", indexed), ("users", users), ("items", items),
                            ("targets", targets), ("queries", queries)):
            frame.write.mode("error").parquet((root / name).resolve().as_uri())
        popularity = [r.parent_asin for r in pairs.groupBy("parent_asin").count()
                      .orderBy(F.desc("count"), "parent_asin").limit(20).collect()]
        pop_recs = queries.select("user_id").withColumn("predictions", F.array(*[F.lit(x) for x in popularity]))
        checkpoint(root.name + ":fit")
        estimator = ALS(userCol="user_idx", itemCol="item_idx", ratingCol="strength", implicitPrefs=True,
            rank=rank, maxIter=max_iter, regParam=reg_param, alpha=alpha, seed=seed,
            numUserBlocks=2, numItemBlocks=2, coldStartStrategy="nan",
            intermediateStorageLevel="DISK_ONLY", finalStorageLevel="DISK_ONLY")
        model, fit_seconds = elapsed(lambda: estimator.fit(indexed))
        model.write().save((root / "model").resolve().as_uri())
        checkpoint(root.name + ":recommend")

        def recommend():
            eligible = queries.select("user_id").join(users, "user_id")
            recs = model.recommendForUserSubset(eligible.select("user_idx"), 20)
            exploded = recs.select("user_idx", F.posexplode("recommendations").alias("position", "rec"))
            decoded = exploded.join(items, F.col("rec.item_idx") == F.col("item_idx"))
            decoded = decoded.join(users, "user_idx").groupBy("user_id").agg(
                F.sort_array(F.collect_list(F.struct("position", "parent_asin"))).alias("ordered"))
            return keep(decoded.select("user_id", F.col("ordered.parent_asin").alias("predictions")))

        als_recs, recommend_seconds = elapsed(recommend)
        # Unknown users receive the same train-only popularity list, without dropping labels.
        cold = queries.filter("history_items = 0").select("user_id").join(pop_recs, "user_id")
        system_recs = keep(als_recs.unionByName(cold))
        checkpoint(root.name + ":evaluate")
        started = time.monotonic()
        metrics = evaluate(queries, pop_recs, "popularity") + evaluate(queries, als_recs, "als")
        metrics += evaluate(queries, system_recs, "als_popularity_fallback")
        evaluation_seconds = round(time.monotonic() - started, 3)
        system_recs.write.parquet((root / "recommendations").resolve().as_uri())
        # Small local inspection only; full user IDs remain in ignored Parquet artifacts.
        preview = system_recs.orderBy("user_id").limit(5).select(
            F.sha2("user_id", 256).alias("user_hash"), "predictions").collect()
        result = dict(start=start, end=end, train_pairs=pair_count, train_users=users.count(),
            train_items=items.count(), target_pairs=target_count, population=population,
            fallback_queries=population["cold_user_queries"], fit_seconds=fit_seconds,
            recommendation_seconds=recommend_seconds, evaluation_seconds=evaluation_seconds, metrics=metrics,
            preview=[r.asDict() for r in preview])
        save_json(root / "metrics.json", result)
        model.userFactors.unpersist(blocking=True)
        model.itemFactors.unpersist(blocking=True)
        return result
    finally:
        for frame in reversed(frames):
            frame.unpersist(blocking=True)


def _run_baseline(category="Video_Games", silver_run="run_20261016T022209697481Z", sample_size=5000,
                 seed=42, rank=16, max_iter=5, reg_param=0.1, alpha=1.0, evaluate_test=False,
                 min_free_gb=20):
    """Own and stop one Spark session; test is opt-in after freezing configuration."""
    import re
    if category not in get_scope()["categories"] or not re.fullmatch(r"run_[A-Za-z0-9_]{8,64}", silver_run):
        raise ValueError("Invalid category/Silver run")
    if any(type(x) is not int or x < 1 for x in (sample_size, rank, max_iter)):
        raise ValueError("sample_size, rank and max_iter must be positive integers")
    if any(not math.isfinite(x) or x < 0 for x in (reg_param, alpha, min_free_gb)):
        raise ValueError("Regularization, confidence and reserve must be finite and nonnegative")
    output = Path("/workspace/outputs/als_baseline")
    check_disk_space(output, min_free_gb)
    root = output / datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%S%fZ")
    root.mkdir(parents=True, exist_ok=False)
    source = f"hdfs://namenode:8020/project/processed/silver/{category.lower()}/v1/{silver_run}"
    info = dict(status="running", source=source, category=category, silver_run=silver_run,
        sample_size_requested=sample_size, seed=seed, rank=rank, max_iter=max_iter, reg_param=reg_param, alpha=alpha,
        evaluate_test=evaluate_test, started_at=datetime.now(timezone.utc).isoformat(),
        policy="all observed review interactions; binary distinct train pairs; repeat candidates allowed",
        sampling="smallest SHA256(seed:user_id), all events retained; subset metrics only",
        code_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        helper_sha256={name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
                       for name in ("eda_recommendation.py", "eda_model_readiness.py")}, folds={})
    save_json(root / "run.json", info)
    monitor = RuntimeMonitor(root, min_free_gb)
    spark = None
    events = None

    def checkpoint(phase):
        print(f"{datetime.now(timezone.utc).isoformat()} {phase}", flush=True)
        monitor.checkpoint(phase)

    try:
        checkpoint("preflight")
        spark = create_spark(2, 16)
        info["spark_version"] = spark.version
        spark.sparkContext.setCheckpointDir((root / "checkpoints").resolve().as_uri())
        info["spark_config"] = {k: spark.conf.get(k) for k in (
            "spark.master", "spark.driver.memory", "spark.sql.shuffle.partitions", "spark.sql.session.timeZone")}
        manifest = read_manifest(spark, source)
        if (manifest.get("status"), manifest.get("category"), manifest.get("run_id")) != ("complete", category, silver_run):
            raise ValueError("Silver manifest does not match a complete selected snapshot")
        info["silver_manifest_sha256"] = hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest()
        reviews = spark.read.parquet(source + "/reviews_clean")
        if not schema_matches(reviews.schema.jsonValue(), manifest["schema"]["reviews"]):
            raise ValueError("Silver schema differs from manifest")
        source_count = reviews.count()
        if source_count != COUNTS[category][0]:
            raise ValueError("Silver count differs from handoff")
        checkpoint("sample")
        started = time.monotonic()
        narrow = reviews.select("user_id", "parent_asin", "event_time")
        selected = sample_users(narrow, sample_size, seed)
        events = narrow.join(selected, "user_id").persist(StorageLevel.DISK_ONLY)
        info.update(source_reviews=source_count, sampled_events=events.count(),
                    sampled_users=events.select("user_id").distinct().count())
        if events.filter("user_id IS NULL OR parent_asin IS NULL OR event_time IS NULL").limit(1).count():
            raise ValueError("Sample contains invalid core fields")
        events.write.parquet((root / "sample_events").resolve().as_uri())
        info["prepare_seconds"] = round(time.monotonic() - started, 3)
        save_json(root / "run.json", info)
        folds = [("validation", "2021-01-01", "2022-01-01")]
        if evaluate_test:
            folds.append(("test", "2022-01-01", "2023-10-01"))
        for name, start, end in folds:
            info["folds"][name] = fit_fold(spark, events, root / name, start, end, rank, max_iter,
                                           reg_param, alpha, seed, checkpoint)
            save_json(root / "run.json", info)
        if read_manifest(spark, source) != manifest:
            raise ValueError("Silver manifest changed during experiment")
        checkpoint("complete")
        info["status"] = "complete"
    except BaseException as exc:
        info.update(status="failed", error=f"{type(exc).__name__}: {exc}")
        raise
    finally:
        try:
            if events is not None:
                events.unpersist(blocking=True)
            if spark is not None:
                spark.stop()
        except BaseException as exc:
            info.update(status="failed", cleanup_error=f"{type(exc).__name__}: {exc}")
            raise
        finally:
            info["finished_at"] = datetime.now(timezone.utc).isoformat()
            save_json(root / "run.json", info)
    return str(root)


def run_baseline(**config):
    """Serialize ALS runs across Linux notebook kernels; release the lock on failure."""
    import fcntl
    output = Path("/workspace/outputs/als_baseline")
    output.mkdir(parents=True, exist_ok=True)
    with (output / ".als.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("Another ALS run is active; wait for its kernel/job to finish") from exc
        return _run_baseline(**config)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--category", default="Video_Games")
    parser.add_argument("--silver-run", default="run_20261016T022209697481Z")
    parser.add_argument("--sample-size", type=int, default=5000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--rank", type=int, default=16)
    parser.add_argument("--max-iter", type=int, default=5)
    parser.add_argument("--reg-param", type=float, default=0.1)
    parser.add_argument("--alpha", type=float, default=1.0)
    parser.add_argument("--evaluate-test", action="store_true")
    print(run_baseline(**vars(parser.parse_args())), flush=True)
