"""Read-only JSONL EDA: one category and one owned Spark session per run."""
import argparse
import csv
import hashlib
import json
import math
import os
import shutil
import signal
import sys
import tempfile
from collections import Counter
from datetime import datetime, timezone
from functools import partial
from pathlib import Path

from pyspark import SparkContext, StorageLevel
from pyspark.sql import SparkSession, Window, functions as F, types as T

def get_scope():
    # Driver only: Spark workers importing this module do not need the scope file.
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion/scripts"))
    from dataset_scope import load_scope
    return load_scope()


def get_categories():
    return tuple(get_scope()["categories"])

FIELDS = {
    "reviews": {"user_id": "str", "asin": "str", "parent_asin": "str", "rating": "number",
                "title": "str", "text": "str", "images": "list", "timestamp": "int",
                "helpful_vote": "int", "verified_purchase": "bool"},
    "metadata": {"parent_asin": "str", "title": "str", "main_category": "str", "store": "str",
                 "price": "number", "average_rating": "number", "rating_number": "int",
                 "features": "list", "description": "list", "categories": "list",
                 "images": "list", "videos": "list", "details": "dict", "bought_together": "list"},
}
PLACEHOLDERS = {"n/a", "na", "none", "null", "unknown", "not available"}
MEDIA_URL_KEYS = {
    "reviews": {"small_image_url", "medium_image_url", "large_image_url"},
    "metadata": {"thumb", "large", "hi_res"},
}
# Bounds are dataset-specific, UTC; ambiguous seconds/milliseconds stay unresolved.
MIN_SECONDS = datetime(1996, 1, 1, tzinfo=timezone.utc).timestamp()
MAX_SECONDS = datetime(2023, 10, 1, tzinfo=timezone.utc).timestamp()
SCHEMA = T.StructType([
    T.StructField(k, t, True) for k, t in [
        ("kind", T.StringType()), ("source", T.StringType()), ("line_hash", T.StringType()),
        ("record_hash", T.StringType()), ("profile_json", T.StringType()),
        ("user_id", T.StringType()), ("asin", T.StringType()), ("parent_asin", T.StringType()),
        ("rating", T.DoubleType()), ("price", T.DoubleType()), ("helpful_vote", T.DoubleType()),
        ("verified", T.StringType()), ("timestamp_seconds", T.DoubleType()),
        ("timestamp_unit", T.StringType()), ("timestamp_digits", T.IntegerType()),
        ("text_hash", T.StringType()), ("text_chars", T.IntegerType()),
        ("category", T.StringType()), ("content_text", T.BooleanType()),
        ("has_title", T.BooleanType()), ("has_features", T.BooleanType()),
        ("has_description", T.BooleanType()), ("has_image", T.BooleanType()),
        ("has_store", T.BooleanType()), ("has_brand", T.BooleanType()),
        ("missing_pattern", T.StringType()), ("content_pattern", T.StringType()),
        ("facts_json", T.StringType()),
        ("user_original_hash", T.StringType()), ("parent_original_hash", T.StringType()),
    ]
])


def digest(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def text_value(value):
    return isinstance(value, str) and bool(value.strip()) and value.strip().lower() not in PLACEHOLDERS


def key_value(value):
    return value.strip() if text_value(value) else None


def state(obj, field):
    if field not in obj:
        return "absent"
    value = obj[field]
    if value is None:
        return "null"
    if isinstance(value, str):
        if value == "":
            return "empty_string"
        if not value.strip():
            return "whitespace"
        if value.strip().lower() in PLACEHOLDERS:
            return "placeholder"
    if isinstance(value, (list, dict)) and not value:
        return "empty_collection"
    return "present"


def numeric(value):
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (ValueError, OverflowError):
        return None


def resolve_timestamp(value):
    number = numeric(value)
    if number is None or not number.is_integer():
        return None, "invalid"
    seconds = MIN_SECONDS <= number < MAX_SECONDS
    millis = MIN_SECONDS <= number / 1000 < MAX_SECONDS
    if seconds == millis:
        return None, "ambiguous" if seconds else "outside_dataset_bounds"
    return (number, "seconds") if seconds else (number / 1000, "milliseconds")


def nested_issues(field, value, parent, dataset="metadata"):
    """A row-level issue can contain multiple bad elements; count the row once."""
    issues = set()
    if not isinstance(value, list):
        return issues
    if field in {"features", "description"} and any(not isinstance(x, str) for x in value):
        issues.add("non_string_element")
    if field == "categories":
        def depth(x):
            return 1 + max((depth(v) for v in x), default=0) if isinstance(x, list) else 0
        issues.add(f"category_depth_{depth(value)}")
        def bad(x):
            return any(bad(v) for v in x) if isinstance(x, list) else not isinstance(x, str)
        if bad(value):
            issues.add("non_string_leaf")
    if field == "bought_together":
        ids = [key_value(x) for x in value]
        if any(x is None for x in ids):
            issues.add("invalid_item_id")
        if len(ids) != len(set(ids)):
            issues.add("repeated_item_id")
        if parent and parent in ids:
            issues.add("self_link")
    if field in {"images", "videos"}:
        if any(not isinstance(x, dict) for x in value):
            issues.add("non_object_element")
        for element in value:
            if not isinstance(element, dict):
                continue
            expected_keys = {"url"} if field == "videos" else MEDIA_URL_KEYS[dataset]
            url_fields = [k for k in element if k in expected_keys]
            if not url_fields:
                issues.add("no_expected_url_key")
            for key in url_fields:
                url = element[key]
                if url is not None and (not isinstance(url, str) or not url.startswith(("https://", "http://"))):
                    issues.add("invalid_url_format")
    return issues


def parse_partition(rows, dataset):
    """Parse once; aggregate audit counters per partition instead of exploding every field."""
    counters, examples = Counter(), {}
    fields = FIELDS[dataset]

    def count(field, metric, label, source=None, line_hash=None):
        key = (field, metric, label)
        counters[key] += 1
        if source and key not in examples:
            examples[key] = {"source": source, "line_sha256": line_hash}

    for row in rows:
        source, line = row["source"], row["value"]
        line_hash = digest(line)
        count("__record__", "rows", "input")
        duplicates = []

        def pairs_hook(pairs):
            obj = {}
            for k, v in pairs:
                if k in obj:
                    duplicates.append(k)
                obj[k] = v
            return obj

        try:
            obj = json.loads(line, object_pairs_hook=pairs_hook)
            if not isinstance(obj, dict):
                raise ValueError("non_object_json")
            if duplicates:
                raise ValueError("duplicate_json_key")
        except (ValueError, RecursionError) as exc:
            label = str(exc) if str(exc) in {"non_object_json", "duplicate_json_key"} else "malformed_json"
            count("__record__", "parse_error", label, source, line_hash)
            continue
        count("__record__", "rows", "parsed")
        for field in obj.keys() - fields.keys():
            count(field, "schema", "extra_field")
        for field, expected in fields.items():
            st = state(obj, field)
            count(field, "state", st)
            if field not in obj or obj[field] is None:
                continue
            value = obj[field]
            typ = type(value).__name__
            count(field, "original_type", typ)
            right_type = (type(value) in (int, float) if expected == "number" else typ == expected)
            if not right_type:
                count(field, "issue", "wrong_original_type", source, line_hash)
            if expected in {"number", "int"}:
                num = numeric(value)
                if not right_type and num is not None:
                    count(field, "issue", "convertible_wrong_type", source, line_hash)
                if num is None:
                    count(field, "issue", "unparseable_or_nonfinite", source, line_hash)
                elif field in {"rating", "average_rating"} and not 1 <= num <= 5:
                    count(field, "issue", "outside_1_5", source, line_hash)
                elif field in {"rating_number", "helpful_vote"} and (num < 0 or not num.is_integer()):
                    count(field, "issue", "not_nonnegative_integer", source, line_hash)
                elif field == "price" and num < 0:
                    count(field, "issue", "negative_price", source, line_hash)
                if field == "price" and num == 0:
                    count(field, "suspect", "zero_price", source, line_hash)
                if field == "timestamp":
                    _, unit = resolve_timestamp(value)
                    count(field, "timestamp_unit", unit, source, line_hash)
            if isinstance(value, str) and value != value.strip():
                count(field, "issue", "leading_trailing_whitespace", source, line_hash)
            structure_issues = nested_issues(field, value, key_value(obj.get("parent_asin")), dataset)
            for issue in structure_issues:
                metric = "structure" if issue.startswith("category_depth_") else "issue"
                count(field, metric, issue, source, line_hash)
            valid = right_type
            if expected in {"number", "int"}:
                valid = num is not None
                if valid and field in {"rating", "average_rating"}:
                    valid = 1 <= num <= 5
                if valid and field in {"rating_number", "helpful_vote"}:
                    valid = num >= 0 and num.is_integer()
                if valid and field == "price":
                    valid = num >= 0
                if field == "timestamp":
                    valid = resolve_timestamp(value)[0] is not None
            if expected == "str":
                valid = text_value(value)
            if any(not issue.startswith("category_depth_") for issue in structure_issues):
                valid = False
            if valid:
                count(field, "valid_or_convertible", "yes")
            if field in {"images", "videos"} and isinstance(value, list):
                for element in value:
                    if isinstance(element, dict):
                        for key in element:
                            count(field, "element_key_occurrences", key)
            if field == "details" and isinstance(value, dict):
                for key, detail in value.items():
                    count(field, "detail_key", key)
                    count(field, "detail_value_type", f"{key}:{type(detail).__name__}")
        keys = {k: key_value(obj.get(k)) for k in ("user_id", "asin", "parent_asin")}
        title, text = obj.get("title"), obj.get("text")
        has_title = text_value(title)
        features = obj.get("features")
        description = obj.get("description")
        has_features = isinstance(features, list) and any(text_value(x) for x in features)
        has_description = isinstance(description, list) and any(text_value(x) for x in description)
        images = obj.get("images")
        has_image = isinstance(images, list) and any(isinstance(x, dict) and any(
            isinstance(v, str) and v.startswith(("http://", "https://"))
            for k, v in x.items() if k in MEDIA_URL_KEYS[dataset]) for x in images)
        rating = numeric(obj.get("rating"))
        price = numeric(obj.get("price"))
        ts, unit = resolve_timestamp(obj.get("timestamp")) if dataset == "reviews" else (None, None)
        raw_ts = numeric(obj.get("timestamp"))
        details = obj.get("details")
        has_brand = isinstance(details, dict) and any(k.lower() == "brand" and text_value(v) for k, v in details.items())
        content_pattern = "|".join(k for k, yes in [("title", has_title), ("features", has_features),
                                                  ("description", has_description)] if not yes) or "complete"
        # Facts exclude missing values, but retain original representations for conflict detection.
        facts = {k: digest(canonical(v)) for k, v in obj.items() if state(obj, k) == "present"} if dataset == "metadata" else {}
        out = dict(kind="record", source=source, line_hash=line_hash, record_hash=digest(canonical(obj)),
                   **keys, rating=rating if rating is not None and 1 <= rating <= 5 else None,
                   price=price if price is not None and price >= 0 else None,
                   helpful_vote=numeric(obj.get("helpful_vote")),
                   verified=str(obj["verified_purchase"]).lower() if type(obj.get("verified_purchase")) is bool else "invalid_or_missing",
                   timestamp_seconds=ts, timestamp_unit=unit,
                   timestamp_digits=len(str(abs(int(raw_ts)))) if raw_ts is not None and raw_ts.is_integer() else None,
                   text_hash=digest(text) if text_value(text) else None,
                   text_chars=len(text) if text_value(text) else 0,
                   category=key_value(obj.get("main_category")), content_text=has_title or has_features or has_description,
                   has_title=has_title, has_features=has_features, has_description=has_description,
                   has_image=has_image, has_store=text_value(obj.get("store")), has_brand=has_brand,
                   missing_pattern="|".join(k for k in ("user_id", "parent_asin", "timestamp")
                                            if (keys.get(k) is None if k != "timestamp" else ts is None)) or "complete",
                   content_pattern=content_pattern,
                   facts_json=canonical(facts) if dataset == "metadata" else None)
        out["user_original_hash"] = digest(obj["user_id"]) if keys["user_id"] else None
        out["parent_original_hash"] = digest(obj["parent_asin"]) if keys["parent_asin"] else None
        yield tuple(out.get(field.name) for field in SCHEMA)
    profile = canonical({"counts": [[*key, n] for key, n in counters.items()],
                         "examples": [[*key, ex] for key, ex in examples.items()]})
    yield tuple({"kind": "summary", "profile_json": profile}.get(field.name) for field in SCHEMA)


def inventory(spark, path):
    jvm = spark._jvm
    root = jvm.org.apache.hadoop.fs.Path(path)
    fs = root.getFileSystem(spark._jsc.hadoopConfiguration())
    if not fs.exists(root):
        raise FileNotFoundError(path)
    iterator = fs.listFiles(root, True)
    files = []
    while iterator.hasNext():
        entry = iterator.next()
        name = entry.getPath().toString()
        if name.endswith((".tmp", "._COPYING_")):
            raise ValueError(f"File chưa finalized: {name}")
        if name.endswith(".jsonl") or name.endswith(".jsonl.gz"):
            files.append({"path": name, "bytes": entry.getLen(), "modified_ms": entry.getModificationTime()})
    if not files:
        raise ValueError(f"Không có JSONL finalized: {path}")
    return sorted(files, key=lambda x: x["path"])


class Report:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=False)
        self.tables = []

    def rows(self, name, rows, columns=None):
        rows = list(rows)
        columns = columns or (list(rows[0]) if rows else ["status"])
        with (self.directory / f"{name}.csv").open("w", encoding="utf-8-sig", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)
        self.tables.append(name)

    def table(self, name, frame, order=None):
        if order:
            frame = frame.orderBy(*order)
        # Only aggregate tables reach the driver; fail instead of silently truncating.
        rows = frame.limit(10001).collect()
        if len(rows) > 10000:
            raise ValueError(f"Aggregate table quá lớn: {name}; hãy bổ sung bin/group trước khi export")
        self.rows(name, [r.asDict() for r in rows], frame.columns)

    def chart(self, table, x, y, title, log=False, horizontal=False):
        import pandas as pd
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        frame = pd.read_csv(self.directory / f"{table}.csv")
        if frame.empty:
            return
        fig, ax = plt.subplots(figsize=(10, 5))
        if horizontal:
            ax.barh(frame[x].astype(str), frame[y])
            ax.set_xlabel(y)
            ax.set_ylabel(x)
        else:
            ax.bar(frame[x].astype(str), frame[y])
            ax.set_ylabel(y)
            ax.set_xlabel(x)
            ax.tick_params(axis="x", rotation=60)
            if log:
                ax.set_yscale("log")
        ax.set_title(title)
        fig.tight_layout()
        fig.savefig(self.directory / f"{table}.png", dpi=140)
        plt.close(fig)

    def heatmap(self, name, frame, fields):
        counts = frame.agg(F.count("*").alias("denominator"), *[
            F.count(F.when(left & right, 1)).alias(f"c_{i}_{j}")
            for i, (_, left) in enumerate(fields) for j, (_, right) in enumerate(fields)]).first().asDict()
        denominator = counts.pop("denominator")
        self.rows(name, [dict(left=a, right=b, count=counts[f"c_{i}_{j}"], denominator=denominator,
                             rate=counts[f"c_{i}_{j}"] / denominator if denominator else None)
                        for i, (a, _) in enumerate(fields) for j, (b, _) in enumerate(fields)])
        if not denominator:
            return
        import matplotlib.pyplot as plt
        matrix = [[counts[f"c_{i}_{j}"] / denominator for j in range(len(fields))] for i in range(len(fields))]
        fig, ax = plt.subplots(figsize=(7, 6))
        plot = ax.imshow(matrix, vmin=0, vmax=1, cmap="Blues")
        labels = [x[0] for x in fields]
        ax.set_xticks(range(len(labels)), labels, rotation=45, ha="right")
        ax.set_yticks(range(len(labels)), labels)
        for i in range(len(fields)):
            for j in range(len(fields)):
                ax.text(j, i, f"{matrix[i][j]:.1%}", ha="center", va="center")
        ax.set_title(f"Co-unavailable / {denominator:,} parsed rows")
        fig.colorbar(plot, ax=ax)
        fig.tight_layout()
        fig.savefig(self.directory / f"{name}.png", dpi=140)
        plt.close(fig)

    def insight_charts(self):
        import ast
        import pandas as pd
        import matplotlib.pyplot as plt
        monthly = pd.read_csv(self.directory / "activity_monthly.csv")
        if not monthly.empty:
            fig, axes = plt.subplots(2, 1, figsize=(12, 7), sharex=True)
            dates = pd.to_datetime(monthly["month"])
            axes[0].plot(dates, monthly["reviews"], label="Reviews")
            axes[0].plot(dates, monthly["active_users"], label="Active users")
            axes[0].plot(dates, monthly["active_items"], label="Active items")
            axes[0].set_ylabel("Count in month")
            axes[0].legend()
            axes[1].plot(dates, monthly["mean_rating"])
            axes[1].set_ylabel("Mean valid rating (1-5)")
            axes[1].set_xlabel("UTC month; boundary months may be partial")
            fig.tight_layout()
            fig.savefig(self.directory / "activity_monthly.png", dpi=140)
            plt.close(fig)
        tail = pd.read_csv(self.directory / "long_tail_curve.csv")
        if not tail.empty and tail["pairs"].sum():
            fig, ax = plt.subplots(figsize=(8, 5))
            ax.plot(tail["percentile"], tail["pairs"].cumsum() / tail["pairs"].sum())
            ax.set(xlabel="Top % items (degree descending)", ylabel="Cumulative share of distinct pairs", title="Long tail")
            fig.tight_layout()
            fig.savefig(self.directory / "long_tail_curve.png", dpi=140)
            plt.close(fig)
        rating = pd.read_csv(self.directory / "rating_by_category.csv").dropna(subset=["rating"])
        if not rating.empty:
            pivot = rating.pivot_table(index="category", columns="rating", values="count", fill_value=0)
            pivot = pivot.div(pivot.sum(axis=1), axis=0)
            ax = pivot.plot.bar(stacked=True, figsize=(10, 6))
            ax.set(xlabel="Safe category; unmatched/unresolved separate", ylabel="Share among valid ratings", title="Rating by category")
            ax.figure.tight_layout()
            ax.figure.savefig(self.directory / "rating_by_category.png", dpi=140)
            plt.close(ax.figure)
        price = pd.read_csv(self.directory / "price_by_category.csv")
        if not price.empty:
            boxes = []
            for _, row in price.iterrows():
                q = ast.literal_eval(row["quartiles"])
                boxes.append(dict(label=str(row["category"]), q1=q[0], med=q[1], q3=q[2],
                                  whislo=row["min"], whishi=row["max"], fliers=[]))
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.bxp(boxes, showfliers=False)
            ax.set_ylabel("Source price units; no currency conversion")
            ax.set_title("Safe items: approximate quartiles; whiskers=min/max")
            ax.tick_params(axis="x", rotation=60)
            fig.tight_layout()
            fig.savefig(self.directory / "price_by_category.png", dpi=140)
            plt.close(fig)


def load(spark, path, dataset, report, persisted):
    files = inventory(spark, path)
    report.rows(f"{dataset}_sources", files)
    raw = spark.read.text([x["path"] for x in files]).withColumn("source", F.input_file_name())
    parsed = spark.createDataFrame(raw.rdd.mapPartitions(partial(parse_partition, dataset=dataset)), SCHEMA)
    parsed.persist(StorageLevel.DISK_ONLY)
    persisted.append(parsed)  # Register before the first action, including failed loads.
    counts, examples = Counter(), {}
    for row in parsed.filter("kind = 'summary'").select("profile_json").collect():
        summary = json.loads(row[0])
        for field, metric, label, n in summary["counts"]:
            counts[(field, metric, label)] += n
        for field, metric, label, example in summary["examples"]:
            examples.setdefault((field, metric, label), example)
    total = counts[("__record__", "rows", "parsed")]
    report.rows(f"{dataset}_quality", [dict(field=k[0], metric=k[1], label=k[2], count=n,
                denominator=counts[("__record__", "rows", "input")] if k[0] == "__record__" else total,
                rate=n / total if total and k[0] != "__record__" else None)
                for k, n in sorted(counts.items())])
    # One trace per issue; field values and review/user IDs are deliberately omitted.
    report.rows(f"{dataset}_error_examples", [dict(field=k[0], metric=k[1], label=k[2], **v)
                for k, v in sorted(examples.items())], ["field", "metric", "label", "source", "line_sha256"])
    report.rows(f"{dataset}_missing", [dict(field=field, denominator=total,
                absent=counts[(field, "state", "absent")], null=counts[(field, "state", "null")],
                empty_string=counts[(field, "state", "empty_string")],
                whitespace=counts[(field, "state", "whitespace")],
                placeholder=counts[(field, "state", "placeholder")],
                empty_collection=counts[(field, "state", "empty_collection")],
                unavailable_rate=sum(counts[(field, "state", s)] for s in
                                     ("absent", "null", "empty_string", "whitespace")) / total if total else None,
                structurally_valid_or_convertible_rate=counts[(field, "valid_or_convertible", "yes")] / total if total else None,
                role="key" if field in {"user_id", "asin", "parent_asin"} else "label" if field == "rating" else "time" if field == "timestamp" else "content_or_auxiliary")
                for field in FIELDS[dataset]])
    report.chart(f"{dataset}_missing", "field", "unavailable_rate",
                 f"{dataset}: missing / {total:,} parsed rows (empty collections separate)", horizontal=True)
    records = parsed.filter("kind = 'record'").drop("profile_json", "kind")
    report.rows(f"{dataset}_overview", [dict(input_rows=counts[("__record__", "rows", "input")],
                parsed_rows=total, parse_errors=counts[("__record__", "rows", "input")] - total,
                files=len(files), bytes=sum(x["bytes"] for x in files), scope="full",
                expected_fields=len(FIELDS[dataset]), actual_fields=sum(1 for field in FIELDS[dataset]
                    if counts[(field, "state", "absent")] < total) + sum(1 for key in counts if key[1] == "schema"),
                absent_schema_fields="|".join(field for field in FIELDS[dataset] if counts[(field, "state", "absent")] == total))])
    return records, parsed, counts


def stats(frame, column):
    return frame.agg(F.count(column).alias("valid_values"), F.min(column).alias("min"),
                     F.avg(column).alias("mean"), F.stddev_samp(column).alias("stddev"),
                     F.percentile_approx(column, [0.01, 0.25, 0.5, 0.75, 0.95, 0.99], 10000).alias("p01_p25_p50_p75_p95_p99"),
                     F.max(column).alias("max"))


def duplicates(frame, keys):
    groups = frame.dropna(subset=keys).groupBy(*keys).agg(F.count("*").alias("n"),
                        F.countDistinct("record_hash").alias("versions")).filter("n > 1")
    return groups, groups.agg(F.count("*").alias("groups"),
                F.coalesce(F.sum("n"), F.lit(0)).alias("rows_in_groups"),
                F.coalesce(F.sum(F.col("n") - 1), F.lit(0)).alias("excess_rows"),
                F.count(F.when(F.col("versions") > 1, 1)).alias("conflicting_groups"))


def degree_bin(column):
    return (F.when(column == 1, "1").when(column == 2, "2").when(column < 5, "3-4")
            .when(column < 10, "5-9").when(column < 50, "10-49").when(column < 100, "50-99")
            .otherwise("100+"))


def identity_overview(frame):
    return frame.agg(F.count("*").alias("parsed_review_denominator"), *[
        measure for field in ("user_id", "asin", "parent_asin") for measure in (
            F.count(field).alias(field + "_valid_rows"), F.countDistinct(field).alias(field + "_distinct"))]).first().asDict()


def metadata_group(rows):
    """Distinguish copies, compatible complements and conflicts without choosing a row."""
    facts, hashes, n = {}, set(), 0
    for row in rows:
        n += 1
        hashes.add(row.record_hash)
        for key, value in json.loads(row.facts_json).items():
            facts.setdefault(key, set()).add(value)
    conflicts = sorted(k for k, values in facts.items() if len(values) > 1)
    classification = "identical" if len(hashes) == 1 else "conflicting" if conflicts else "complementary"
    return n, classification, "|".join(conflicts)


def analyze_category(spark, category, base, version, output, split_date, run_core=False, checkpoint=lambda phase: None):
    print(f"[START] {category}", flush=True)
    report = Report(Path(output) / category)
    persisted = []
    try:
        checkpoint("reviews_profile")
        r, r_cache, rc = load(spark, f"{base}/{category.lower()}_reviews/{version}", "reviews", report, persisted)
        checkpoint("metadata_profile")
        m, m_cache, mc = load(spark, f"{base}/{category.lower()}_metadata/{version}", "metadata", report, persisted)
        checkpoint("quality_duplicates")
        r = r.withColumn("date", F.to_timestamp(F.from_unixtime("timestamp_seconds")))
        r = r.withColumn("month", F.date_format("date", "yyyy-MM")).withColumn("year", F.year("date"))
        review_count = rc[("__record__", "rows", "parsed")]
        metadata_count = mc[("__record__", "rows", "parsed")]
        report.rows("reviews_ids", [identity_overview(r)])
        duplicate_summaries = []
        for name, frame, keys in [
            ("review_exact", r, ["record_hash"]),
            ("review_event_variant", r, ["user_id", "asin", "timestamp_seconds"]),
            ("review_event_parent", r, ["user_id", "parent_asin", "timestamp_seconds"]),
            ("review_user_parent", r, ["user_id", "parent_asin"]),
            ("metadata_parent", m, ["parent_asin"]),
        ]:
            groups, summary = duplicates(frame, keys)
            duplicate_summaries.append(dict(check=name, **summary.first().asDict()))
            if name in {"review_event_variant", "review_event_parent"}:
                report.table(name + "_examples", groups.select(F.sha2(F.concat_ws("|", *keys), 256).alias("group_sha256"),
                             "n", "versions").orderBy(F.desc("n")).limit(5))
        report.rows("duplicates", duplicate_summaries)
        reused = r.filter("text_hash IS NOT NULL").groupBy("text_hash").agg(F.count("*").alias("n"),
                    F.countDistinct("user_id").alias("users"), F.countDistinct("parent_asin").alias("items"))
        report.table("reused_text", reused.filter("n > 1 AND (users > 1 OR items > 1)").agg(
            F.count("*").alias("groups"), F.sum("n").alias("rows")))
        report.table("review_missing_patterns", r.groupBy("missing_pattern").count())
        report.table("metadata_content_patterns", m.groupBy("content_pattern").count())
        report.heatmap("review_co_unavailable", r, [("user", F.col("user_id").isNull()),
            ("parent", F.col("parent_asin").isNull()), ("time", F.col("timestamp_seconds").isNull()),
            ("text", F.col("text_chars") == 0)])
        report.heatmap("metadata_co_unavailable", m, [("title", ~F.col("has_title")),
            ("features", ~F.col("has_features")), ("description", ~F.col("has_description")),
            ("price", F.col("price").isNull()), ("image", ~F.col("has_image"))])
        normalizations = []
        for label, frame, key, representation in [("review_user", r, "user_id", "user_original_hash"),
                ("review_parent", r, "parent_asin", "parent_original_hash"),
                ("metadata_parent", m, "parent_asin", "parent_original_hash")]:
            multi = frame.dropna(subset=[key]).groupBy(key).agg(F.countDistinct(representation).alias("original_variants"),
                    F.count("*").alias("rows")).filter("original_variants > 1")
            normalizations.append(dict(check=label, **multi.agg(F.count("*").alias("normalized_keys_with_multiple_original_forms"),
                     F.coalesce(F.sum("rows"), F.lit(0)).alias("affected_rows")).first().asDict()))
        report.rows("id_normalization", normalizations)

        checkpoint("metadata_join")
        mg = m.filter("parent_asin IS NOT NULL").groupBy("parent_asin").agg(
            F.count("*").alias("metadata_rows"), F.countDistinct("record_hash").alias("versions"))
        mg.persist(StorageLevel.DISK_ONLY)
        persisted.append(mg)
        duplicate_ids = mg.filter("metadata_rows > 1").select("parent_asin")
        duplicate_meta = m.join(duplicate_ids, "parent_asin", "left_semi")
        # Only duplicated keys enter Python; all fields participate in conflict detection.
        # ponytail: collect only duplicate-key groups; switch to per-field aggregation if a key has massive replication.
        classifier = F.udf(metadata_group, "n long, classification string, conflicting_fields string")
        classified = duplicate_meta.groupBy("parent_asin").agg(
            classifier(F.collect_list(F.struct("record_hash", "facts_json"))).alias("info")).select("parent_asin", "info.*")
        report.table("metadata_conflict_classes", classified.groupBy("classification", "conflicting_fields").agg(
            F.count("*").alias("groups"), F.sum("n").alias("rows"), F.sum(F.col("n") - 1).alias("excess_rows")))
        report.table("metadata_conflict_examples", classified.select(F.sha2("parent_asin", 256).alias("item_sha256"),
                     "n", "classification", "conflicting_fields").orderBy(F.desc("n")).limit(5))
        # Safe content attribution: singleton keys or exact copies only. No synthetic merged profiles.
        safe = m.join(mg.filter("versions = 1").select("parent_asin"), "parent_asin", "left_semi").dropDuplicates(["parent_asin"])
        attrs = safe.select("parent_asin", F.col("category").alias("safe_category"))
        joined = r.select("parent_asin", "user_id", "rating", "verified", "text_chars", "date", "month", "year")
        joined = joined.join(mg, "parent_asin", "left").join(attrs, "parent_asin", "left")
        joined = joined.withColumn("category", F.when(F.col("parent_asin").isNull(), "__invalid_parent__")
                   .when(F.col("metadata_rows").isNull(), "__unmatched__")
                   .when(F.col("versions") > 1, "__unresolved_metadata__")
                   .otherwise(F.coalesce("safe_category", F.lit("__missing_category__"))))
        joined = joined.withColumn("matched", F.col("metadata_rows").isNotNull()).withColumn("safe_match", F.col("versions") == 1)
        joined.persist(StorageLevel.DISK_ONLY)
        persisted.append(joined)
        joined_count = joined.count()
        assert joined_count == review_count, "Join đã nhân hoặc mất dòng!"
        valid_parent = joined.filter("parent_asin IS NOT NULL")
        cov = valid_parent.agg(F.count("*").alias("review_denominator"),
              F.count(F.when(F.col("matched"), 1)).alias("matched_reviews"),
              F.count(F.when(F.col("safe_match"), 1)).alias("safe_reviews"),
              F.countDistinct("parent_asin").alias("item_denominator"),
              F.countDistinct(F.when(F.col("matched"), F.col("parent_asin"))).alias("matched_items"),
              F.countDistinct(F.when(F.col("safe_match"), F.col("parent_asin"))).alias("safe_items")).first().asDict()
        metadata_items = mg.count()
        cov.update(metadata_item_denominator=metadata_items,
                   metadata_without_reviews=mg.join(r.select("parent_asin").distinct(), "parent_asin", "left_anti").count(),
                   reviews_invalid_parent=review_count - cov["review_denominator"], left_join_rows=joined_count)
        cov["review_coverage"] = cov["matched_reviews"] / cov["review_denominator"] if cov["review_denominator"] else None
        cov["item_coverage"] = cov["matched_items"] / cov["item_denominator"] if cov["item_denominator"] else None
        cov["safe_review_coverage"] = cov["safe_reviews"] / cov["review_denominator"] if cov["review_denominator"] else None
        cov["safe_item_coverage"] = cov["safe_items"] / cov["item_denominator"] if cov["item_denominator"] else None
        cov["metadata_without_reviews_rate"] = cov["metadata_without_reviews"] / metadata_items if metadata_items else None
        report.rows("metadata_coverage", [cov])
        for group in ["category", "month"]:
            report.table("coverage_by_" + group, joined.groupBy(group).agg(F.count("*").alias("all_reviews"),
                F.count(F.when(F.col("parent_asin").isNotNull(), 1)).alias("valid_parent_reviews"),
                F.count(F.when(F.col("matched"), 1)).alias("matched_reviews"),
                F.count(F.when(F.col("safe_match"), 1)).alias("safe_reviews")), [group])
        matched_items = safe.join(r.select("parent_asin").distinct(), "parent_asin", "left_semi")
        report.table("matched_content", matched_items.agg(F.count("*").alias("safe_matched_item_denominator"),
             *[F.count(F.when(F.col(c), 1)).alias(c) for c in
               ("has_title", "has_features", "has_description", "has_image", "has_store", "has_brand", "content_text")],
             F.count(F.when(F.col("price").isNotNull(), 1)).alias("has_valid_price"),
             F.count(F.when(~F.col("content_text"), 1)).alias("no_content_text")))
        report.table("metadata_missing_by_category", m.groupBy("category").agg(F.count("*").alias("denominator"),
             *[F.count(F.when(~F.col(c), 1)).alias("missing_" + c) for c in
               ("has_title", "has_features", "has_description", "has_image", "has_store", "has_brand")]))

        valid_rating = r.filter("rating IS NOT NULL")
        report.table("rating_distribution", r.groupBy("rating").count(), ["rating"])
        checkpoint("rating_price")
        report.table("rating_summary", stats(valid_rating, "rating"))
        for group in ["category", "year", "verified"]:
            report.table("rating_by_" + group, joined.groupBy(group, "rating").count(), [group, "rating"])
        for label, key in [("user", "user_id"), ("item", "parent_asin")]:
            averages = valid_rating.dropna(subset=[key]).groupBy(key).agg(F.count("*").alias("n"), F.avg("rating").alias("mean_rating"))
            report.table(f"rating_{label}_support", averages.withColumn("support", degree_bin(F.col("n"))).groupBy("support").agg(
                F.count("*").alias("groups"), F.avg("mean_rating").alias("mean_group_rating"), F.sum("n").alias("reviews")))
        report.table("review_text_by_rating_year", r.groupBy("rating", "year").agg(F.count("*").alias("denominator"),
             F.count(F.when(F.col("text_chars") == 0, 1)).alias("unusable_text"), F.avg("text_chars").alias("mean_chars")))
        report.table("review_text_length", stats(r, "text_chars"))
        report.table("helpful_vote_summary", stats(r.filter("helpful_vote >= 0 AND helpful_vote = floor(helpful_vote)"), "helpful_vote"))
        prices = safe.filter("price IS NOT NULL")
        report.table("price_summary", stats(prices, "price"))
        report.table("price_by_category", prices.groupBy("category").agg(F.count("*").alias("valid_item_denominator"),
                F.min("price").alias("min"), F.percentile_approx("price", [0.25, 0.5, 0.75], 10000).alias("quartiles"),
                F.max("price").alias("max")))
        report.table("price_bins", prices.withColumn("bin", F.when(F.col("price") == 0, "zero")
                     .otherwise(F.floor(F.log10("price")).cast("string"))).groupBy("bin").count(), ["bin"])
        report.table("price_linear_histogram", prices.withColumn("bin_10", F.least(F.floor(F.col("price") / 10), F.lit(100)))
                     .groupBy("bin_10").count(), ["bin_10"])
        report.table("price_extreme_examples", prices.select(F.sha2("parent_asin", 256).alias("item_sha256"), "price", "source", "line_hash")
                     .orderBy(F.desc("price")).limit(5))
        report.rows("price_population", [dict(metadata_rows=metadata_count, safe_items=safe.count(),
                     valid_price_items=prices.count(), unresolved_metadata_keys=mg.filter("versions > 1").count(),
                     rule="one item per singleton/exact-copy key; unresolved keys excluded")])
        report.chart("rating_distribution", "rating", "count", f"Rating, all {review_count:,} parsed reviews; null = invalid/missing")
        report.chart("price_bins", "bin", "count", "Price: floor(log10(price)), zero separate; safe items")
        report.chart("price_linear_histogram", "bin_10", "count", "Price: 10-unit bins; bin 100 includes all >=1000")

        events = r.filter("timestamp_seconds IS NOT NULL")
        report.table("timestamp_units", r.groupBy("timestamp_unit", "timestamp_digits").count())
        checkpoint("temporal")
        report.table("time_range", events.agg(F.min("date").alias("first_utc"), F.max("date").alias("last_utc"), F.count("*").alias("valid_time_reviews")))
        monthly = joined.filter("date IS NOT NULL").groupBy("month").agg(F.count("*").alias("reviews"),
            F.countDistinct("user_id").alias("active_users"), F.countDistinct("parent_asin").alias("active_items"),
            F.avg("rating").alias("mean_rating"), F.count(F.when(F.col("text_chars") == 0, 1)).alias("unusable_text"),
            F.count(F.when(F.col("matched"), 1)).alias("metadata_matched"))
        first_users = events.filter("user_id IS NOT NULL").groupBy("user_id").agg(F.min("month").alias("month")).groupBy("month").count().withColumnRenamed("count", "first_seen_users")
        first_items = events.filter("parent_asin IS NOT NULL").groupBy("parent_asin").agg(F.min("month").alias("month")).groupBy("month").count().withColumnRenamed("count", "first_seen_items")
        bounds = events.agg(F.min("date"), F.max("date")).first()
        if bounds[0] is not None:
            # Start at month boundary; sequence from a 31st would otherwise skip February.
            calendar = spark.range(1).select(F.explode(F.sequence(F.trunc(F.lit(bounds[0]).cast("date"), "month"),
                       F.trunc(F.lit(bounds[1]).cast("date"), "month"), F.expr("interval 1 month"))).alias("day"))
            calendar = calendar.select(F.date_format("day", "yyyy-MM").alias("month"))
            monthly = calendar.join(monthly, "month", "left").join(first_users, "month", "left").join(first_items, "month", "left")
            monthly = monthly.fillna(0, subset=["reviews", "active_users", "active_items", "unusable_text", "metadata_matched", "first_seen_users", "first_seen_items"])
            monthly = monthly.withColumn("boundary_month", F.col("month").isin(bounds[0].strftime("%Y-%m"), bounds[1].strftime("%Y-%m")))
        report.table("activity_monthly", monthly, ["month"])
        report.table("activity_yearly", joined.filter("date IS NOT NULL").groupBy("year").agg(F.count("*").alias("reviews"),
               F.countDistinct("user_id").alias("active_users"), F.countDistinct("parent_asin").alias("active_items"), F.avg("rating").alias("mean_rating")), ["year"])
        report.table("activity_daily_peaks", events.groupBy(F.to_date("date").alias("day")).count().orderBy(F.desc("count")).limit(20))
        histories = events.filter("user_id IS NOT NULL").groupBy("user_id").agg(F.count("*").alias("events"),
            F.countDistinct("parent_asin").alias("items"), F.min("timestamp_seconds").alias("first"), F.max("timestamp_seconds").alias("last"),
            F.countDistinct(F.to_date("date")).alias("days"), F.countDistinct("month").alias("months"))
        report.table("user_history", histories.agg(F.count("*").alias("user_denominator"),
            F.count(F.when(F.col("days") == 1, 1)).alias("one_day_users"), F.count(F.when(F.col("months") == 1, 1)).alias("one_month_users"),
            F.percentile_approx((F.col("last") - F.col("first")) / 86400, [0.25, 0.5, 0.75, 0.95], 10000).alias("span_days_quartiles_p95")))
        gaps = events.filter("user_id IS NOT NULL").withColumn("previous", F.lag("timestamp_seconds").over(Window.partitionBy("user_id").orderBy("timestamp_seconds")))
        gaps = gaps.withColumn("gap_seconds", F.col("timestamp_seconds") - F.col("previous"))
        report.table("interaction_gaps", stats(gaps, "gap_seconds"))
        report.table("interaction_gap_bins", gaps.filter("gap_seconds IS NOT NULL").withColumn("bin", F.when(F.col("gap_seconds") == 0, "zero")
                     .otherwise(F.floor(F.log10("gap_seconds")).cast("string"))).groupBy("bin").count())
        report.table("timestamp_ties", events.filter("user_id IS NOT NULL").groupBy("user_id", "timestamp_seconds").count()
                     .filter("count > 1").agg(F.count("*").alias("tied_groups"), F.sum("count").alias("events_in_ties")))

        keyed = r.filter("user_id IS NOT NULL AND parent_asin IS NOT NULL")
        checkpoint("sparsity_history")
        pairs = keyed.groupBy("user_id", "parent_asin").agg(F.count("*").alias("events"))
        pairs.persist(StorageLevel.DISK_ONLY)
        persisted.append(pairs)
        users = pairs.groupBy("user_id").agg(F.count("*").alias("degree"), F.sum("events").alias("events"))
        items = pairs.groupBy("parent_asin").agg(F.count("*").alias("degree"), F.sum("events").alias("events"))
        users.persist(StorageLevel.DISK_ONLY)
        items.persist(StorageLevel.DISK_ONLY)
        persisted.extend([users, items])
        u, i, e = users.count(), items.count(), pairs.count()
        pair_info = dict(users=u, items=i, distinct_pairs=e, keyed_review_rows=keyed.count(),
                         density=e / (u * i) if u and i else None, sparsity=1 - e / (u * i) if u and i else None,
                         repeated_pairs=pairs.filter("events > 1").count())
        report.rows("sparsity", [pair_info])
        for label, frame in [("user", users), ("item", items)]:
            report.table(label + "_degree_summary", stats(frame, "degree"))
            report.table(label + "_degree_bins", frame.withColumn("bin", degree_bin(F.col("degree"))).groupBy("bin").agg(
                          F.count("*").alias("entities"), F.sum("degree").alias("pairs"), F.sum("events").alias("reviews")))
            report.chart(label + "_degree_bins", "bin", "entities", f"Distinct neighbors / {u if label == 'user' else i:,} {label}s", log=True)
        ranked = items.withColumn("rank", F.row_number().over(Window.orderBy(F.desc("degree"), "parent_asin")))
        ranked.persist(StorageLevel.DISK_ONLY)
        persisted.append(ranked)
        report.rows("long_tail", [dict(top_fraction=fraction, items_in_top=max(1, math.ceil(i * fraction)) if i else 0,
                distinct_pairs=e, pairs_in_top=ranked.filter(F.col("rank") <= max(1, math.ceil(i * fraction))).agg(F.sum("degree")).first()[0],
                ties="ceil(I*fraction), exact top count, parent_asin lexical tie-break") for fraction in (0.01, 0.05, 0.1)])
        report.table("long_tail_curve", ranked.withColumn("percentile", F.ceil(F.col("rank") * 100 / F.lit(i or 1)))
                     .groupBy("percentile").agg(F.sum("degree").alias("pairs"), F.sum("events").alias("reviews")), ["percentile"])
        report.table("coverage_by_popularity", items.join(mg, "parent_asin", "left").withColumn("bin", degree_bin(F.col("degree"))).groupBy("bin").agg(
            F.count("*").alias("item_denominator"), F.count(F.when(F.col("metadata_rows").isNotNull(), 1)).alias("matched_items"),
            F.sum("events").alias("review_denominator"), F.sum(F.when(F.col("metadata_rows").isNotNull(), F.col("events")).otherwise(0)).alias("matched_reviews")))
        report.table("sparsity_by_category", joined.filter("user_id IS NOT NULL AND parent_asin IS NOT NULL").groupBy("category").agg(
            F.countDistinct("user_id").alias("users"), F.countDistinct("parent_asin").alias("items"),
            F.countDistinct(F.struct("user_id", "parent_asin")).alias("pairs"), F.count("*").alias("reviews")))

        impacts = []
        for threshold in (1, 2, 3, 5):
            retained = pairs.join(users.filter(F.col("degree") >= threshold).select("user_id"), "user_id", "left_semi")
            impact = retained.agg(F.countDistinct("user_id").alias("users"), F.countDistinct("parent_asin").alias("items"),
                                 F.count("*").alias("pairs"), F.sum("events").alias("reviews")).first().asDict()
            impact["reviews"] = impact["reviews"] or 0
            impact["review_retention_rate"] = impact["reviews"] / pair_info["keyed_review_rows"] if pair_info["keyed_review_rows"] else None
            impacts.append(dict(rule=f"user >= {threshold} distinct items (one pass)", denominator=pair_info["keyed_review_rows"], **impact))
        report.rows("user_threshold_impact", impacts)
        if run_core:
            core, iterations = pairs, 0
            while True:
                iterations += 1
                eligible_u = core.groupBy("user_id").count().filter("count >= 5").select("user_id")
                eligible_i = core.groupBy("parent_asin").count().filter("count >= 5").select("parent_asin")
                next_core = core.join(eligible_u, "user_id", "left_semi").join(eligible_i, "parent_asin", "left_semi").persist(StorageLevel.DISK_ONLY)
                persisted.append(next_core)
                checkpoint("five_core_iteration")
                old_n, new_n = core.count(), next_core.count()
                if core is not pairs:
                    core.unpersist(blocking=True)
                    persisted[:] = [frame for frame in persisted if frame is not core]
                core = next_core
                if old_n == new_n:
                    break
            report.table("five_core_impact", core.agg(F.countDistinct("user_id").alias("users"), F.countDistinct("parent_asin").alias("items"),
                          F.count("*").alias("pairs"), F.sum("events").alias("reviews")).withColumn("iterations", F.lit(iterations)))
            core.unpersist(blocking=True)
            persisted[:] = [frame for frame in persisted if frame is not core]

        checkpoint("processing_diagnostics")
        conditions = [("valid_keys", F.col("user_id").isNotNull() & F.col("parent_asin").isNotNull()),
                      ("valid_rating", F.col("rating").isNotNull()), ("resolved_time", F.col("date").isNotNull()),
                      ("metadata_membership", F.col("matched")), ("safe_metadata", F.coalesce(F.col("safe_match"), F.lit(False))),
                      ("usable_review_text", F.col("text_chars") > 0)]
        cumulative, gates = F.lit(True), []
        for name, condition in conditions:
            cumulative = cumulative & condition
            gates += [F.count(F.when(condition, 1)).alias(name + "_independent"),
                      F.count(F.when(cumulative, 1)).alias(name + "_cumulative")]
        report.table("processing_gate_impact", joined.agg(F.count("*").alias("parsed_denominator"), *gates))
        # Global chronological boundary is a diagnostic only; no train/test or features are written.
        train = keyed.filter(F.col("date") < F.lit(split_date).cast("timestamp"))
        test = keyed.filter(F.col("date") >= F.lit(split_date).cast("timestamp"))
        seen_users = train.select("user_id").distinct().withColumn("seen_user", F.lit(True))
        seen_items = train.select("parent_asin").distinct().withColumn("seen_item", F.lit(True))
        cold = test.join(seen_users, "user_id", "left").join(seen_items, "parent_asin", "left").fillna(False, subset=["seen_user", "seen_item"])
        report.table("temporal_cold_start", cold.groupBy("seen_user", "seen_item").agg(F.count("*").alias("test_reviews"),
                     F.countDistinct("user_id").alias("test_users"), F.countDistinct("parent_asin").alias("test_items")))
        repeated_test = test.join(train.select("user_id", "parent_asin").distinct(), ["user_id", "parent_asin"], "left_semi").count()
        report.rows("temporal_split_diagnostic", [dict(cutoff_utc=split_date, train_reviews=train.count(), test_reviews=test.count(),
                     keyed_reviews_unresolved_time=keyed.filter("date IS NULL").count(), test_reviews_on_seen_pairs=repeated_test,
                     rule="global chronological cutoff; tied timestamps remain in same side; diagnostic only")])

        findings = [dict(finding="Read/parse errors", count=rc[("__record__", "rows", "input")] - review_count + mc[("__record__", "rows", "input")] - metadata_count,
                     evidence="reviews_quality.csv; metadata_quality.csv", next_step="Quarantine lỗi và truy vết file + SHA256; không bỏ im lặng", status="measured"),
                    dict(finding="Unresolved metadata keys", count=mg.filter("versions > 1").count(), evidence="metadata_conflict_classes.csv",
                     next_step="Chọn quy tắc giải quyết xung đột trước join/features; không chọn first tùy ý", status="measured"),
                    dict(finding="Review parent unmatched", count=cov["review_denominator"] - cov["matched_reviews"], evidence="metadata_coverage.csv",
                     next_step="Giữ cohort CF-only; phân tích riêng content/cold-start khi thiếu metadata", status="measured"),
                    dict(finding="Repeated user-item pairs", count=pair_info["repeated_pairs"], evidence="duplicates.csv; sparsity.csv",
                     next_step="Quyết định latest/aggregate theo mục tiêu; loại bản sao trước, không xóa mọi pair lặp", status="measured"),
                    dict(finding="Invalid/unresolved review time", count=review_count - events.count(), evidence="timestamp_units.csv",
                     next_step="Không dùng cho chronological split trước khi sửa/xác minh đơn vị", status="measured"),
                    dict(finding="One-item users", count=users.filter("degree = 1").count(), evidence="user_degree_bins.csv; user_threshold_impact.csv",
                     next_step="Chọn min-history theo retention; giữ cold-start evaluation riêng", status="measured")]
        report.rows("processing_actions", findings)
        checkpoint("charts_summary")
        report.insight_charts()
        summary = dict(category=category, review_rows=review_count, metadata_rows=metadata_count, **pair_info, **cov)
        (report.directory / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        (report.directory / "BAO_CAO_EDA.md").write_text(
            f"# EDA {category}\n\nPhạm vi: toàn bộ JSONL finalized, không sampling.\n\n"
            f"- Reviews đọc được: {review_count:,}; metadata: {metadata_count:,}.\n"
            f"- Users: {u:,}; items: {i:,}; cặp khác nhau: {e:,}.\n"
            f"- Density: {pair_info['density']}; coverage review: {cov['review_coverage']}.\n"
            f"- Join trước/sau: {review_count:,}/{joined_count:,}.\n\n"
            "Xem `processing_actions.csv` cho phát hiện, bằng chứng và đề xuất; `processing_gate_impact.csv` đo từng điều kiện độc lập và cộng dồn; thứ tự điều kiện không phải policy bắt buộc.\n\n"
            "Quantile dùng percentile_approx, accuracy=10000; distinct/count chính xác. Khóa được trim trên bản phân tích, số dạng sai có trong quality. Placeholder báo cáo riêng; empty collection không tự coi là lỗi. Price chỉ ở cấp item an toàn, đơn vị tiền tệ theo nguồn chưa tự quy đổi.\n\n"
            "Thời gian UTC; khoảng hợp lý [1996-01-01, 2023-10-01), phù hợp Amazon Reviews 2023. Tháng biên có thể không đầy đủ. Gaps bằng 0 gồm ties/copies; không suy ra thứ tự ties hoặc gian lận. Timestamp missing/invalid không vào split.\n\n"
            "Metadata snapshot hiện có có thể chứa thông tin tương lai: price, average_rating, rating_number không dùng như feature lịch sử trước khi xử lý leakage. Khớp category chỉ từ singleton/exact copies; complementary/conflicting keys vẫn unresolved.\n\n"
            "Trace dùng file + SHA256 dòng UTF-8 (không gồm newline), không phải số dòng. Chưa đo overlap/global distinct giữa category. Không sửa raw, không tạo processed/model features.\n\n"
            "## Bảng thống kê\n\n" + "\n".join(f"- [{name}]({name}.csv)" for name in report.tables) + "\n",
            encoding="utf-8")
        print(f"[DONE] {category}: {review_count:,} reviews, {metadata_count:,} metadata", flush=True)
        return summary
    finally:
        had_error = sys.exc_info()[0] is not None
        cleanup_errors = []
        for frame in reversed(persisted):
            try:
                frame.unpersist(blocking=True)
            except Exception as error:
                cleanup_errors.append(type(error).__name__)
        if cleanup_errors:
            if not had_error:
                raise RuntimeError(f"Cache cleanup failed: {cleanup_errors}")
            print(f"[CLEANUP WARNING] {cleanup_errors}; Spark.stop will run", file=sys.stderr, flush=True)


def check_disk_space(output, min_free_gb):
    if min_free_gb < 0:
        raise ValueError("min_free_gb phải >= 0")
    existing = Path(output).resolve()
    while not existing.exists():
        existing = existing.parent
    values = dict(output_free_bytes=shutil.disk_usage(existing).free,
                  temporary_free_bytes=shutil.disk_usage(tempfile.gettempdir()).free)
    if min(values.values()) < min_free_gb * 1024 ** 3:
        raise RuntimeError(f"Cần ít nhất {min_free_gb} GiB trống trên filesystem output và Spark tạm")
    return values


class RuntimeMonitor:
    """Phase snapshots, not a quota or a continuous peak-disk measurement."""
    def __init__(self, directory, min_free_gb):
        self.directory, self.min_free_gb = Path(directory), min_free_gb
        self.samples = []

    def checkpoint(self, phase):
        sample = dict(phase=phase, utc=datetime.now(timezone.utc).isoformat(),
                      output_free_bytes=shutil.disk_usage(self.directory).free,
                      temporary_free_bytes=shutil.disk_usage(tempfile.gettempdir()).free)
        for name in ("memory.current", "memory.peak", "memory.max"):
            path = Path('/sys/fs/cgroup') / name
            value = path.read_text().strip() if path.is_file() else None
            sample[name.replace('.', '_') + '_bytes'] = int(value) if value and value.isdigit() else None
        self.samples.append(sample)
        data = dict(samples=self.samples, measurement="phase snapshots; filesystem free includes other processes",
                    minimum_output_free_bytes=min(x['output_free_bytes'] for x in self.samples),
                    minimum_temporary_free_bytes=min(x['temporary_free_bytes'] for x in self.samples))
        temporary = self.directory / 'resources.json.tmp'
        temporary.write_text(json.dumps(data, indent=2), encoding='utf-8')
        temporary.replace(self.directory / 'resources.json')
        if min(sample['output_free_bytes'], sample['temporary_free_bytes']) < self.min_free_gb * 1024 ** 3:
            raise RuntimeError(f"Disk reserve below {self.min_free_gb} GiB at phase: {phase}")


def validate_run(categories, split_date, min_free_gb):
    categories = tuple(categories)
    if len(categories) != 1 or categories[0] not in get_categories():
        raise ValueError("Mỗi process chỉ chạy một category thuộc experiment scope")
    if min_free_gb < 0:
        raise ValueError("min_free_gb phải >= 0")
    datetime.strptime(split_date, "%Y-%m-%d")
    return categories


def create_spark(cores=1, shuffle_partitions=64):
    if cores not in (1, 2) or shuffle_partitions < 1:
        raise ValueError("cores: 1/2; shuffle_partitions phải >= 1")
    if (SparkContext._active_spark_context is not None or SparkSession.getActiveSession() is not None
            or SparkSession._instantiatedSession is not None):
        raise RuntimeError("Đã có Spark session; dừng session/kernel cũ trước khi chạy EDA")
    # This entrypoint owns a dedicated session, including its driver-memory setting.
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-memory 1g pyspark-shell'
    spark = (SparkSession.builder.master(f"local[{cores}]").appName("recommendation-eda")
             .config("spark.driver.memory", "1g")
             .config("spark.local.dir", str(Path(tempfile.gettempdir()) / 'eda-spark'))
             .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
             .config("spark.sql.files.maxPartitionBytes", "33554432")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
             .config("spark.cleaner.periodicGC.interval", "1min")
             .config("spark.ui.retainedJobs", "30").config("spark.ui.retainedStages", "60")
             .config("spark.sql.ui.retainedExecutions", "20").config("spark.sql.caseSensitive", "true")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        spark.sparkContext.setLogLevel("WARN")
    except BaseException:
        spark.stop()
        raise
    return spark


def analyze_all(spark, categories=("All_Beauty",), base="hdfs://namenode:8020/project/raw", version="v1",
                output="/workspace/outputs/eda", split_date="2022-01-01", run_core=False, min_free_gb=20):
    categories = validate_run(categories, split_date, min_free_gb)
    check_disk_space(output, min_free_gb)
    root = Path(output) / datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%S%fZ")
    root.mkdir(parents=True)
    info = dict(status="running", categories=categories, completed_categories=[], scope="full_category",
                experiment_scope=get_scope(), scope_sha256=digest(canonical(get_scope())),
                code_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                base=base, version=version, split_date=split_date, timezone="UTC", optional_five_core=run_core,
                min_free_gb=min_free_gb, cross_category_measurement="not_measured",
                spark_config={name: spark.conf.get(name) for name in (
                    'spark.master', 'spark.driver.memory', 'spark.local.dir', 'spark.sql.shuffle.partitions',
                    'spark.sql.files.maxPartitionBytes', 'spark.sql.session.timeZone')})
    (root / "run.json").write_text(json.dumps(info, indent=2), encoding="utf-8")
    monitor = RuntimeMonitor(root, min_free_gb)
    try:
        monitor.checkpoint('input_inventory')
        for dataset in FIELDS:
            inventory(spark, f"{base}/{categories[0].lower()}_{dataset}/{version}")
        spark.conf.set("spark.sql.caseSensitive", "true")
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        spark.sparkContext.addPyFile(str(Path(__file__).resolve()))
        summary = analyze_category(spark, categories[0], base, version, root, split_date,
                                   run_core, monitor.checkpoint)
        info['completed_categories'] = list(categories)
        monitor.checkpoint('category_complete')
        overview = Report(root / "comparison")
        overview.rows("categories", [summary])
        overview.rows("category_sparsity", [dict(category=categories[0], users=summary['users'],
            items=summary['items'], pairs=summary['distinct_pairs'], review_events=summary['keyed_review_rows'],
            density=summary['density'], sparsity=summary['sparsity'])])
        info['status'] = 'complete'
    except KeyboardInterrupt:
        info['status'] = 'interrupted'
        raise
    except Exception as error:
        info['status'] = 'failed'
        (root / 'failure.txt').write_text(str(error), encoding='utf-8')
        raise
    finally:
        info['finished_at'] = datetime.now(timezone.utc).isoformat()
        (root / "run.json").write_text(json.dumps(info, indent=2), encoding="utf-8")
    print(f"[REPORT] {root}", flush=True)
    return root


def run_eda(categories=None, base=None, version=None, output="/workspace/outputs/eda", split_date="2022-01-01",
            run_core=False, min_free_gb=20, cores=1, shuffle_partitions=64):
    scope = get_scope()
    categories = validate_run(scope['categories'][:1] if categories is None else categories, split_date, min_free_gb)
    check_disk_space(output, min_free_gb)  # Before creating a JVM or scanning inputs.
    spark = None
    previous_handler = signal.getsignal(signal.SIGTERM)
    def interrupt(signum, frame):
        raise KeyboardInterrupt("EDA received termination signal")
    signal.signal(signal.SIGTERM, interrupt)
    try:
        spark = create_spark(cores, shuffle_partitions)
        return analyze_all(spark, categories, base or 'hdfs://namenode:8020' + scope['hdfs_base'],
                           version or scope['version'], output, split_date, run_core, min_free_gb)
    finally:
        try:
            if spark is not None:
                spark.stop()
        finally:
            signal.signal(signal.SIGTERM, previous_handler)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    scope = get_scope()
    parser.add_argument("--categories", nargs="+", choices=scope['categories'], default=scope['categories'][:1])
    parser.add_argument("--base", default="hdfs://namenode:8020" + scope['hdfs_base'])
    parser.add_argument("--version", default=scope['version'])
    parser.add_argument("--output", default="/workspace/outputs/eda")
    parser.add_argument("--split-date", default="2022-01-01")
    parser.add_argument("--five-core", action="store_true")
    parser.add_argument("--min-free-gb", type=int, default=20)
    parser.add_argument("--cores", type=int, choices=(1, 2), default=1)
    parser.add_argument("--shuffle-partitions", type=int, default=64)
    args = parser.parse_args()
    if len(args.categories) != 1 or args.min_free_gb < 0 or args.shuffle_partitions < 1:
        parser.error("Chọn một category; min-free-gb >= 0; shuffle-partitions >= 1")
    run_eda(args.categories, args.base, args.version, args.output, args.split_date, args.five_core,
            args.min_free_gb, args.cores, args.shuffle_partitions)


if __name__ == "__main__":
    main()
