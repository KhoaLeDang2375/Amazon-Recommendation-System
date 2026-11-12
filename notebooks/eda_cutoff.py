"""Compare temporal protocols on read-only Silver; no model scores or Gold writes."""
import argparse
import csv
import hashlib
import json
import math
import signal
from datetime import datetime, timezone
from pathlib import Path

CUTOFFS = ("2019-01-01", "2020-01-01", "2021-01-01")
HORIZONS = (3, 6, 12)
DATA_END = "2023-10-01"
# Planning bound for ONE binary hit-rate proportion, not power for model comparisons/NDCG.
MIN_WARM = math.ceil(1.96 ** 2 * 0.25 / 0.02 ** 2)
RULE = "latest feasible validation cutoff, then shortest horizon; >=2401 warm-full queries in every category; test starts one year later with equal horizon"


def end_date(start, months):
    value = datetime.strptime(start, "%Y-%m-%d")
    index = value.year * 12 + value.month - 1 + months
    return f"{index // 12:04d}-{index % 12 + 1:02d}-01"


def choose_protocol(rows, categories):
    """Use validation counts only; never maximize warm rate or consult test scores."""
    expected = {(c, d, m) for c in categories for d in CUTOFFS for m in HORIZONS}
    keys = [(r["category"], r["start"], r["months"]) for r in rows]
    if len(keys) != len(set(keys)) or set(keys) != expected:
        raise ValueError("Need exactly one complete comparison grid per category")
    feasible = []
    for start in CUTOFFS:
        for months in HORIZONS:
            group = [r for r in rows if r["start"] == start and r["months"] == months]
            if all(r["warm_full_queries"] >= MIN_WARM for r in group):
                feasible.append((start, months))
    if not feasible:
        return dict(status="insufficient_support", minimum_warm=MIN_WARM, rule=RULE)
    start, months = sorted(feasible, key=lambda x: (x[0], -x[1]))[-1]
    test = end_date(start, 12)
    if end_date(test, months) > DATA_END:
        raise ValueError("Test horizon is right-censored by dataset end")
    return dict(status="selected", rule=RULE, minimum_warm=MIN_WARM,
                validation_start=start, validation_end=end_date(start, months),
                test_start=test, test_end=end_date(test, months), months=months,
                policy="all observed review interactions", candidates="distinct train items; repeats allowed",
                population="all labeled queries; warm-full model comparison reported separately")


def measure(events, start, horizons, checkpoint=lambda _: None):
    """Cache user histories once per cutoff; compare horizons from the same first event."""
    from pyspark import StorageLevel
    from pyspark.sql import functions as F
    from eda_model_readiness import bins
    frames = []

    def keep(frame):
        frame.persist(StorageLevel.DISK_ONLY)
        frames.append(frame)
        frame.count()
        return frame

    def count(condition, name):
        return F.count(F.when(condition, 1)).alias(name)

    try:
        checkpoint(start + ":history")
        history = events.filter(F.col("event_time") < F.lit(start).cast("timestamp"))
        users = keep(history.groupBy("user_id").agg(F.count("*").alias("history_events"),
            F.countDistinct("parent_asin").alias("history_items"),
            F.countDistinct("event_time").alias("history_times")))
        items = keep(history.select("parent_asin").distinct())
        support = users.agg(F.count("*").alias("train_users"),
            F.coalesce(F.sum("history_items"), F.lit(0)).alias("train_pairs"),
            F.coalesce(F.sum("history_events"), F.lit(0)).alias("train_events")).first().asDict()
        support["train_items"] = items.count()
        possible = support["train_users"] * support["train_items"]
        support["train_density"] = support["train_pairs"] / possible if possible else None
        window = events.filter((F.col("event_time") >= F.lit(start).cast("timestamp")) &
                               (F.col("event_time") < F.lit(end_date(start, max(horizons))).cast("timestamp")))
        first = window.groupBy("user_id").agg(F.min("event_time").alias("event_time"))
        targets = window.join(first, ["user_id", "event_time"]).select("user_id", "event_time", "parent_asin").distinct()
        labels = targets.join(items.withColumn("known_item", F.lit(1)), "parent_asin", "left").groupBy("user_id", "event_time").agg(
            F.count("*").alias("targets"), F.count("known_item").alias("targets_in"))
        queries = keep(labels.join(users, "user_id", "left").fillna(0, ["history_events", "history_items", "history_times"])
            .withColumn("warm_full", (F.col("history_events") > 0) & (F.col("targets") == F.col("targets_in"))))
        rows, distributions = [], []
        for months in horizons:
            checkpoint(f"{start}:{months}months")
            q = queries.filter(F.col("event_time") < F.lit(end_date(start, months)).cast("timestamp"))
            row = q.agg(F.count("*").alias("queries"),
                count(F.col("history_events") == 0, "cold_user_queries"),
                count(F.col("targets_in") < F.col("targets"), "any_cold_item_queries"),
                count(F.col("targets_in") == 0, "all_cold_item_queries"),
                count(F.col("warm_full"), "warm_full_queries"),
                count(F.col("warm_full") & (F.col("history_items") >= 2), "warm_full_history2plus"),
                count(F.col("targets") > 1, "tied_queries"),
                F.coalesce(F.sum("targets"), F.lit(0)).alias("target_pairs"),
                F.coalesce(F.sum("targets_in"), F.lit(0)).alias("target_pairs_in"),
                F.percentile_approx((F.col("event_time").cast("double") - F.lit(start).cast("timestamp").cast("double")) / 86400,
                    [0.5, 0.9], 10000).alias("target_lag_days_p50_p90")).first().asDict()
            n, t = row["queries"], row["target_pairs"]
            row.update(start=start, end=end_date(start, months), months=months, window_days=(datetime.fromisoformat(end_date(start, months)) - datetime.fromisoformat(start)).days, **support)
            row.update(cold_user_rate=row["cold_user_queries"] / n if n else None,
                any_cold_item_query_rate=row["any_cold_item_queries"] / n if n else None,
                cold_item_target_rate=(t - row["target_pairs_in"]) / t if t else None,
                warm_full_rate=row["warm_full_queries"] / n if n else None,
                train_users_without_target=support["train_users"] - (n - row["cold_user_queries"]))
            if n - row["cold_user_queries"] > support["train_users"] or row["target_pairs_in"] > t:
                raise AssertionError("Population conservation failed")
            for cohort, selected in (("all_queries", q), ("warm_full", q.filter("warm_full"))):
                table = selected.select(F.explode(F.array(*[F.struct(F.lit(name).alias("measure"), bins(F.col(name)).alias("bin"))
                    for name in ("history_events", "history_items", "history_times")])).alias("v")).select("v.*").groupBy("measure", "bin").count()
                result = [r.asDict() for r in table.collect()]
                denominator = n if cohort == "all_queries" else row["warm_full_queries"]
                for name in ("history_events", "history_items", "history_times"):
                    if sum(r["count"] for r in result if r["measure"] == name) != denominator:
                        raise AssertionError("History bins do not conserve queries")
                distributions.extend(dict(start=start, months=months, cohort=cohort, **r) for r in result)
            rows.append(row)
        return rows, distributions
    finally:
        for frame in reversed(frames):
            frame.unpersist(blocking=True)


def run_study(category, silver_run, run_id=None, cores=2, shuffle_partitions=64, min_free_gb=20, test_start=None, months=None):
    from pyspark import StorageLevel
    from eda_model_readiness import COUNTS, read_manifest, save_json, schema_matches, validate_paths
    from eda_recommendation import Report, RuntimeMonitor, check_disk_space, create_spark
    if (test_start is None) != (months is None):
        raise ValueError("Test diagnostic needs both start and months")
    if test_start and (test_start not in tuple(end_date(s, 12) for s in CUTOFFS) or months not in HORIZONS or end_date(test_start, months) > DATA_END):
        raise ValueError("Invalid or censored test window")
    if not math.isfinite(min_free_gb) or min_free_gb < 0:
        raise ValueError("Invalid reserve")
    output = "/workspace/outputs/eda_model_readiness"
    run_id = run_id or datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%S%fZ")
    source = validate_paths(category, silver_run, output, run_id)
    check_disk_space(output, min_free_gb)
    root = Path(output) / run_id
    root.mkdir(parents=True, exist_ok=False)
    report = Report(root / category)
    monitor = RuntimeMonitor(root, min_free_gb)
    info = dict(status="running", kind="cutoff_test_diagnostic" if test_start else "cutoff_selection",
        category=category, silver_run=silver_run, run_id=run_id, source=source,
        cutoffs=[test_start] if test_start else CUTOFFS, horizons=[months] if months else HORIZONS,
        rule=RULE, minimum_warm=MIN_WARM, timezone="UTC", policy="all reviews", no_models=True,
        code_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        helper_sha256={name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
            for name in ("eda_model_readiness.py", "eda_recommendation.py")},
        started_at=datetime.now(timezone.utc).isoformat())
    save_json(root / "run.json", info)
    spark, events = None, None
    previous = signal.getsignal(signal.SIGTERM)
    def interrupt(signum, frame):
        raise KeyboardInterrupt("Cutoff EDA terminated")
    signal.signal(signal.SIGTERM, interrupt)
    def checkpoint(phase):
        print(phase, flush=True)
        monitor.checkpoint(phase)
    try:
        checkpoint("preflight")
        spark = create_spark(cores, shuffle_partitions)
        info["spark_version"] = spark.version
        manifest = read_manifest(spark, source)
        if (manifest.get("status"), manifest.get("category"), manifest.get("run_id")) != ("complete", category, silver_run):
            raise ValueError("Selected Silver is not complete")
        reviews = spark.read.parquet(source + "/reviews_clean")
        if reviews.count() != COUNTS[category][0] or not schema_matches(reviews.schema.jsonValue(), manifest["schema"]["reviews"]):
            raise ValueError("Silver count/schema differs from handoff")
        info["silver_manifest_sha256"] = hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest()
        events = reviews.select("user_id", "parent_asin", "event_time").persist(StorageLevel.DISK_ONLY)
        info["events"] = events.count()
        rows, histories = [], []
        for start in info["cutoffs"]:
            outcome, bins = measure(events, start, info["horizons"], checkpoint)
            rows.extend(dict(category=category, **r) for r in outcome)
            histories.extend(dict(category=category, **r) for r in bins)
        report.rows("cutoff_comparison", rows)
        report.rows("cutoff_history", histories)
        info.update(rows=rows, histories=histories)
        if read_manifest(spark, source) != manifest:
            raise ValueError("Silver manifest changed during study")
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
        finally:
            signal.signal(signal.SIGTERM, previous)
            info["finished_at"] = datetime.now(timezone.utc).isoformat()
            save_json(root / "run.json", info)
    return root


def summarize(paths, output):
    """Aggregate only explicitly selected complete runs; write a reviewable protocol lock."""
    from eda_recommendation import get_scope
    runs = [json.loads(Path(p).read_text(encoding="utf-8-sig")) for p in paths]
    categories = get_scope()["categories"]
    if any(r["status"] != "complete" or r["kind"] != "cutoff_selection" for r in runs):
        raise ValueError("Only complete selection runs may select a protocol")
    if len(runs) != len(categories) or set(r["category"] for r in runs) != set(categories):
        raise ValueError("Need all scope categories exactly once")
    if len({r["code_sha256"] for r in runs}) != 1 or len({json.dumps(r["helper_sha256"], sort_keys=True) for r in runs}) != 1:
        raise ValueError("Code/helper mismatch")
    if any((r["cutoffs"], r["horizons"], r["rule"], r["minimum_warm"]) != (list(CUTOFFS), list(HORIZONS), RULE, MIN_WARM) for r in runs):
        raise ValueError("Selection configuration mismatch")
    rows = [row for r in runs for row in r["rows"]]
    decision = choose_protocol(rows, categories)
    decision.update(evidence=[dict(path=str(p), category=r["category"], silver_run=r["silver_run"],
        code_sha256=r["code_sha256"], silver_manifest_sha256=r["silver_manifest_sha256"]) for p, r in zip(paths, runs)],
        caveat="2401 approximates worst-case 95% +/-2 percentage points for one hit-rate proportion; not an NDCG or paired-comparison power guarantee")
    directory = Path(output)
    directory.mkdir(parents=True, exist_ok=False)
    (directory / "protocol-lock.json").write_text(json.dumps(decision, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    with (directory / "comparison.csv").open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig, axes = plt.subplots(1, 3, figsize=(14, 4), sharex=True)
    for ax, category in zip(axes, categories):
        for months in HORIZONS:
            group = sorted((r for r in rows if r["category"] == category and r["months"] == months), key=lambda r: r["start"])
            ax.plot([r["start"][:4] for r in group], [r["warm_full_queries"] for r in group], marker="o", label=f"{months} tháng")
        ax.axhline(MIN_WARM, color="gray", linestyle="--", label="Ngưỡng 2.401")
        ax.set_title(category.replace("_", " "))
        ax.set_xlabel("Năm bắt đầu validation")
        ax.set_ylabel("Query warm, toàn bộ nhãn trong candidates")
        ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(directory / "warm_support.png", dpi=150)
    plt.close(fig)
    lines = ["# Cơ sở khóa cutoff", "", "Đo toàn bộ Silver, policy mọi review; không train hoặc dùng điểm mô hình để chọn mốc.", "",
        "Một query là một user tại mốc dự đoán. Nhãn là tập item ở thời điểm review đầu tiên trong cửa sổ; giữ ties và repeat.", "",
        "Ưu tiên cutoff gần đây nhất đủ ít nhất 2.401 query warm ở cả ba ngành, rồi chọn cửa sổ ngắn nhất tại mốc đó. Warm ở đây yêu cầu có history và toàn bộ nhãn thuộc item train.", "",
        "Ngưỡng 2.401 đến từ n = ceil(1,96² × 0,25 / 0,02²): dự toán sai số xấp xỉ ±2 điểm phần trăm cho một tỷ lệ hit ở mức 95%. Đây là ngân sách độ chính xác, không chứng minh đủ power để phân biệt hai model hoặc bảo đảm NDCG.", "",
        "Không tối đa hóa warm rate vì làm vậy thay đổi population. Không lọc k-core, rating, verified hoặc thay đáp án ngoài candidates.", "",
        "![So sánh support](warm_support.png)", "",
        "| Ngành | Cutoff | Tháng | User train | Item train | Cặp train | Query có nhãn | Warm | Warm ≥2 items | Cold user | Cold target item |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for r in rows:
        pct = lambda v: "N/A" if v is None else f"{v:.1%}"
        lines.append(f"| {r['category']} | {r['start']} | {r['months']} | {r['train_users']:,} | {r['train_items']:,} | {r['train_pairs']:,} | {r['queries']:,} | {r['warm_full_queries']:,} | {r['warm_full_history2plus']:,} | {pct(r['cold_user_rate'])} | {pct(r['cold_item_target_rate'])} |")
    lines += ["", "Cold-user rate lấy query có nhãn làm mẫu số. Cold-item rate lấy query–target pairs làm mẫu số; hai tỷ lệ giao nhau, không cộng. File CSV còn báo query có ít nhất một target ngoài candidates và độ trễ target P50/P90.", ""]
    if decision["status"] == "selected":
        lines += [f"**Khóa validation [{decision['validation_start']}, {decision['validation_end']}); test [{decision['test_start']}, {decision['test_end']}) UTC.** Refit chỉ dùng dữ liệu trước test_start; hai cửa sổ cùng {decision['months']} tháng.", "",
            "Mốc được chọn là lựa chọn vận hành có đủ support theo ngưỡng đã khai báo, không phải cutoff tối ưu duy nhất về khoa học. Test diagnostics chỉ dùng để công bố coverage, không đổi lựa chọn theo kết quả test.", "",
            "| Ngành, tại validation đã chọn | Warm history=1 item | Warm history≥2 items | Độ trễ target P50/P90, ngày |", "|---|---:|---:|---|"]
        for r in rows:
            if (r["start"], r["months"]) == (decision["validation_start"], decision["months"]):
                lines.append(f"| {r['category']} | {r['warm_full_queries'] - r['warm_full_history2plus']:,} | {r['warm_full_history2plus']:,} | {r['target_lag_days_p50_p90']} |")
        lines += ["", "Số warm đủ ngưỡng không đồng nghĩa đủ mẫu trong mọi bin history; nhóm ≥2 có thể nhỏ và cần báo riêng khi đánh giá. Các file cutoff_history.csv từng run giữ bins 0/1/2/3–4/≥5 cho events, items và timestamp groups ở cả population và warm."]
    else:
        lines += ["**Chưa khóa: không phương án nào đủ ngưỡng ở cả ba ngành.** Cần thay ngân sách độ chính xác hoặc xác định phạm vi thực nghiệm bằng lý do rõ ràng; không tự hạ ngưỡng sau khi xem điểm model."]
    lines += ["", "Quy mô train chỉ mô tả support, không chứng minh model học tốt. Một user với một item vẫn góp vào train nhưng ít thông tin để học sở thích. Cửa sổ dài cho thêm cơ hội quan sát target nhưng mục tiêu trở thành tương tác xa hơn; lag P50/P90 giúp đọc đúng khoảng dự đoán.", "",
        "Nguồn: các run complete được liệt kê trong protocol-lock.json; Silver không thay đổi. Không có Gold, embedding hoặc model mới trong đợt này."]
    (directory / "BAO_CAO_CUTOFF.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return directory


def attach_test(paths, output):
    """Publish coverage of the already selected test window; never reselect it."""
    directory = Path(output)
    decision = json.loads((directory / "protocol-lock.json").read_text(encoding="utf-8"))
    if decision["status"] != "selected":
        raise ValueError("Need an existing selection before test diagnostics")
    runs = [json.loads(Path(p).read_text(encoding="utf-8-sig")) for p in paths]
    sources = {r["category"]: r for r in decision["evidence"]}
    if len(runs) != len(sources) or set(r["category"] for r in runs) != set(sources):
        raise ValueError("Need test diagnostic for every selected category once")
    for r in runs:
        original = sources[r["category"]]
        if (r["status"], r["kind"], r["cutoffs"], r["horizons"]) != ("complete", "cutoff_test_diagnostic", [decision["test_start"]], [decision["months"]]):
            raise ValueError("Wrong test diagnostic window or incomplete run")
        if any(r[key] != original[key] for key in ("silver_run", "code_sha256", "silver_manifest_sha256")) or len(r["rows"]) != 1:
            raise ValueError("Test source/code differs from selected protocol")
    lines = ["", "## Coverage tại cửa sổ test đã khóa", "", "Đo sau khi đã chọn bằng thống kê validation; không dùng các số test dưới đây để đổi cutoff.", "",
        "| Ngành | Users train | Items train | Cặp train | Queries | Warm | Warm ≥2 items | Cold user | Cold target item | Lag P50/P90, ngày |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---|"]
    for run in runs:
        r = run["rows"][0]
        lines.append(f"| {r['category']} | {r['train_users']:,} | {r['train_items']:,} | {r['train_pairs']:,} | {r['queries']:,} | {r['warm_full_queries']:,} | {r['warm_full_history2plus']:,} | {r['cold_user_rate']:.1%} | {r['cold_item_target_rate']:.1%} | {r['target_lag_days_p50_p90']} |")
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    categories = list(sources)
    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    bin_names = ["0", "1", "2", "3-4", "5+"]
    for ax, cohort in zip(axes, ("all_queries", "warm_full")):
        base = [0.] * len(categories)
        for name in bin_names:
            counts, rates = [], []
            for category in categories:
                run = next(r for r in runs if r["category"] == category)
                row = run["rows"][0]
                n = row["queries"] if cohort == "all_queries" else row["warm_full_queries"]
                count = sum(r["count"] for r in run["histories"] if r["cohort"] == cohort and r["measure"] == "history_items" and r["bin"] == name)
                counts.append(count)
                rates.append(count / n * 100 if n else 0)
            ax.bar([c.replace("_", "\n") for c in categories], rates, bottom=base, label=name)
            base = [a + b for a, b in zip(base, rates)]
        ax.set_title("Toàn bộ query" if cohort == "all_queries" else "Chỉ query warm, nhãn trong candidates")
        ax.set_ylabel("% query theo số item trong history")
        ax.legend(title="History items", fontsize=8)
    fig.tight_layout()
    fig.savefig(directory / "test_history.png", dpi=150)
    plt.close(fig)
    lines += ["", "![Lịch sử tại test](test_history.png)", "",
        "Cột 0 là cold user. Các cột 1/2/3–4/≥5 là số item khác nhau trong lịch sử, không phải số review. Với ALS/NeuMF thuần ID, so chất lượng trên nhóm warm chung; báo toàn population cùng fallback riêng. Target ngoài candidates vẫn tính miss trong metric toàn population.", "",
        "Các counts là thống kê đầy đủ của snapshot, không phải ước lượng từ mẫu user. Công thức sai số ở trên chỉ là quy tắc dự toán cỡ mẫu cho đánh giá tương lai."]
    report = directory / "BAO_CAO_CUTOFF.md"
    report.write_text(report.read_text(encoding="utf-8") + "\n".join(lines) + "\n", encoding="utf-8")
    decision.update(status="locked", test_evidence=[dict(path=str(p), category=r["category"], silver_run=r["silver_run"],
        code_sha256=r["code_sha256"], silver_manifest_sha256=r["silver_manifest_sha256"]) for p, r in zip(paths, runs)])
    (directory / "protocol-lock.json").write_text(json.dumps(decision, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return directory


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summarize", nargs="+")
    parser.add_argument("--attach-test", nargs="+")
    parser.add_argument("--output")
    parser.add_argument("--category")
    parser.add_argument("--silver-run")
    parser.add_argument("--run-id")
    parser.add_argument("--cores", type=int, default=2)
    parser.add_argument("--shuffle-partitions", type=int, default=64)
    parser.add_argument("--min-free-gb", type=float, default=20)
    parser.add_argument("--test-start")
    parser.add_argument("--months", type=int)
    args = vars(parser.parse_args())
    paths, output = args.pop("summarize"), args.pop("output")
    test_paths = args.pop("attach_test")
    if paths and test_paths:
        parser.error("Choose summarize or attach-test")
    if test_paths:
        if not output:
            parser.error("--attach-test requires --output")
        print(attach_test(test_paths, output))
        raise SystemExit(0)
    if paths:
        if not output:
            parser.error("--summarize requires --output")
        print(summarize(paths, output))
    else:
        if not args["category"] or not args["silver_run"]:
            parser.error("--category and --silver-run required")
        print(run_study(**args))
