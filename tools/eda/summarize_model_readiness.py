"""Combine explicitly selected complete readiness runs using only aggregate CSV/JSON."""
import argparse
import ast
import csv
import json
from pathlib import Path


def read_csv(path):
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def rate(a, b):
    return f"{100 * a / b:.2f}%" if b else "N/A"


def summarize(directories, output):
    runs = []
    for directory in directories:
        root = Path(directory).resolve()
        manifest = json.loads((root / "run.json").read_text(encoding="utf-8"))
        batch = json.loads(root.with_suffix(".batch.json").read_text(encoding="utf-8-sig"))
        if manifest["status"] != "complete" or batch["status"] != "complete" or manifest["run_id"] != batch["run_id"]:
            raise ValueError(f"Incomplete run/batch: {root}")
        if runs and any(manifest.get(key) != runs[0][1].get(key) for key in ("folds", "policies", "code_sha256", "helper_sha256")):
            raise ValueError("Runs do not share the same protocol/code")
        if any(manifest["category"] == item[1]["category"] for item in runs):
            raise ValueError("Duplicate category")
        runs.append((root, manifest, root / manifest["category"]))
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    lines = ["# EDA bổ sung trước GOLD — 18/10/2026", "",
        "Nguồn: các final Silver snapshots; chỉ các readiness runs/batches complete dưới đây được tổng hợp.",
        "Bốn hướng: content-based, implicit ALS, NCF/NeuMF, DeepFM bổ sung nội dung. Chưa encode/train/tạo GOLD.", "",
        "## Population dùng đánh giá", "",
        "A: mọi review interaction. B: verified_purchase=true cho cả history và tìm target.",
        "Query: một user/category/window; target là tập parents tại timestamp đầu tiên; history trước đầu window.",
        "Candidates: items đã thấy trong train của fold/policy, giữ repeat. Metadata là crawl snapshot.", "",
        "| Category | Fold | Policy | Queries có target | Có history / queries | Full in-catalog queries | Warm + full | Partial | Outside |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|"]
    collected = []
    for root, manifest, folder in runs:
        population = read_csv(folder / "query_population_comparison.csv")
        for row in population:
            fold, policy = row["fold"], row["policy"]
            groups = read_csv(folder / f"{fold}_{policy}_candidate_eligibility.csv")
            totals = {name: sum(int(g["queries"]) for g in groups if g["candidate_group"] == name) for name in ("full", "partial", "outside")}
            warm_full = sum(int(g["queries"]) for g in groups if g["warm_user"] == "True" and g["candidate_group"] == "full")
            total = int(row["queries_with_target"])
            if sum(totals.values()) != total:
                raise ValueError("Candidate/query count conservation failed")
            if sum(int(g["targets"]) for g in groups) != int(row["target_pairs"]) or sum(int(g["targets_in"]) for g in groups) != int(row["targets_in"]):
                raise ValueError("Candidate/target count conservation failed")
            prefix = f"{fold}_{policy}_"
            histories = read_csv(folder / (prefix + "history_bins.csv"))
            for measure in ("history_events", "history_items", "history_times"):
                if sum(int(h["count"]) for h in histories if h["measure"] == measure) != total:
                    raise ValueError("History bins do not conserve query population")
            for table in ("feature_eligibility", "target_ties_repeat"):
                if sum(int(v["queries"]) for v in read_csv(folder / (prefix + table + ".csv"))) != total:
                    raise ValueError(f"{table} does not conserve query population")
            positives = int(read_csv(folder / (prefix + "train_support.csv"))[0]["positive_pairs"])
            for negative in read_csv(folder / (prefix + "negative_sampling_feasibility.csv")):
                if int(negative["nominal_rows"]) != positives * (1 + int(negative["negatives_per_positive"])):
                    raise ValueError("Negative row estimate differs from positive-pair count")
            lines.append(f"| {manifest['category']} | {fold} | {policy} | {total:,} | {rate(int(row['personalizable_queries']), total)} | {totals['full']:,} | {warm_full:,} | {totals['partial']:,} | {totals['outside']:,} |")
            collected.append(dict(category=manifest["category"], warm_full=warm_full, **row, **totals))
    lines += ["", "Full/partial/outside phân hoạch queries có target; có history không đồng nghĩa tất cả targets nằm trong candidates. Warm + full là có history và toàn bộ targets thuộc candidates, chưa phải model score coverage.",
        "Validation: history trước 2021, target trong 2021. Test: history trước 2022, target từ 2022 đến trước 2023-10-01.",
        "Đây là diagnostic fixed-cutoff, chưa phải metric mô hình hay tái lập nguyên bản split của paper.", "",
        "## Ảnh hưởng verified-only", "",
        "| Category | Fold | A queries mất target khi sang B | A queries có history+target mất khả năng đó | Retained queries đổi target |",
        "|---|---|---:|---:|---:|"]
    for root, manifest, folder in runs:
        for fold, _, _ in manifest["folds"]:
            row = read_csv(folder / f"{fold}_verification_policy_impact.csv")[0]
            lines.append(f"| {manifest['category']} | {fold} | {row['A_queries_losing_target_in_B']} / {row['A_target_query_denominator']} | {row['A_personalizable_lost_in_B']} / {row['A_personalizable_denominator']} | {row['retained_queries_with_changed_targets']} |")
    lines += ["", "False là chưa được xác minh mua, không phải nhãn chưa mua. Policy B có thể đổi đáp án và candidates, không chỉ giảm rows.",
        "", "## Text sản phẩm — không phải review text", "",
        "| Category | Field | Items | P50 chars | P95 chars | P99 chars | Max chars | >8192 chars / items |",
        "|---|---|---:|---:|---:|---:|---:|---:|"]
    for root, manifest, folder in runs:
        for row in read_csv(folder / "item_text_profile.csv"):
            if row["field"] not in ("title", "expanded"):
                continue
            q = ast.literal_eval(row["chars_p50_p90_p95_p99"])
            count, denominator = int(row['chars_gt_8192']), int(row['item_denominator'])
            tail = f"{count:,} / {denominator:,} ({100 * count / denominator:.4f}%)" if denominator else "N/A"
            lines.append(f"| {manifest['category']} | {row['field']} | {denominator:,} | {q[0]} | {q[2]} | {q[3]} | {row['max_chars']} | {tail} |")
    lines += ["", "Quantiles xấp xỉ accuracy=10000; counts chính xác. Ký tự/UTF-8 bytes không phải token.",
        "Regex flags cần rà ngữ nghĩa; excerpts có giới hạn, mẫu strata giao nhau không dùng ước lượng ngôn ngữ toàn population.", "",
        "## Phình số dòng nếu lưu sẵn negatives", "",
        "Giả định 4 negatives/positive, một positive/distinct train user–item pair; chưa sinh negatives.", "",
        "| Category | Fold | Policy | Positive pairs | Nominal rows với 4 negatives | Users thiếu 4 unseen items |",
        "|---|---|---|---:|---:|---:|"]
    for root, manifest, folder in runs:
        for fold, _, _ in manifest["folds"]:
            for policy in ("A", "B"):
                row = read_csv(folder / f"{fold}_{policy}_negative_sampling_feasibility.csv")[1]
                lines.append(f"| {manifest['category']} | {fold} | {policy} | {int(row['positive_pairs']):,} | {int(row['nominal_rows']):,} | {row['users_below_4']} / {row['train_user_denominator']} |")
    lines += ["", "Không dùng future labels để lọc negatives. Ưu tiên sampler lúc train thay cho lưu bảng tăng nhiều lần.",
        "", "## Rà mẫu text và cách diễn giải", "",
        "Đã rà title và đoạn đầu rút gọn, không đọc toàn văn mọi mô tả dài. Mẫu có chủ đích và strata giao nhau.",
        "Trong excerpts có HTML/URL, lặp nội dung và ví dụ mô tả không khớp title; Industrial còn có dấu hiệu mojibake ở ký hiệu kỹ thuật. Chưa đo tỷ lệ lỗi ngữ nghĩa/encoding toàn catalog.",
        "Độ dài phần lớn không cực đoan theo ký tự/bytes; điều này chưa chứng minh encoder hoặc mô hình sẽ vừa RAM/VRAM.", ""]
    for root, manifest, folder in runs:
        notes = folder / "text_review_notes.md"
        if not notes.is_file():
            raise ValueError(f"Semantic sample notes missing: {notes}")
        sample = json.loads((folder / "text_sample_manifest.json").read_text(encoding="utf-8"))
        lines.append(f"- {manifest['category']}: {sample['selected']} mẫu theo category; [ghi chú rà mẫu](<{notes.as_posix()}>).")
    lines += ["", "## Đề xuất chuyển sang GOLD", "",
        "| Quyết định | Đề xuất và căn cứ |",
        "|---|---|",
        "| Interaction policy | A phù hợp đối chiếu next-interaction; B phù hợp phạm vi verified-review. Chọn mục tiêu trước khi khóa Gold; không chọn chỉ vì B ít rows hoặc metric cao hơn. A/B có thể đổi đáp án. |",
        "| Split và query | Có thể giữ diagnostic mốc 2021/2022 làm đề xuất đầu tiên: đã đo đủ population. History trước mốc, first-next timestamp, giữ ties/repeat. Chưa gọi là split nguyên bản paper. |",
        "| Candidates và cold-start | Catalog quan sát trong train của từng fold/category. Giữ target ngoài candidates trong labels/report; phân biệt warm+full với toàn population, có fallback cho history=0. |",
        "| Text | Giữ tham chiếu title/features/description; ưu tiên title-only làm baseline, expanded là biến thể có cleaning nhẹ. Không bỏ số, mã model, đơn vị, phủ định; không tự re-encode Unicode hoặc drop item theo regex. |",
        "| ALS/NCF train view | Khởi đầu bằng binary distinct user–item pairs; repeated rating conflicts không xóa interaction. Mappings theo fold/category, không hash IDs thành factors. |",
        "| DeepFM | IDs và content references làm nền; main_category/store là tùy chọn theo coverage/cardinality. Chưa đưa rating_number/average_rating/price snapshot làm đặc trưng lịch sử. |",
        "| Negative sampling | Giữ positives và train-only sampler config. Tỷ lệ 1/4/10 là các kịch bản đã tính số rows, chưa chọn bằng test. False verification không phải negative label. |",
        "| Còn ngoài phạm vi | Chọn encoder/token cap/dimensions/batch size, embeddings, model metrics và benchmark tài nguyên để giai đoạn sau. EDA không kết luận thuật toán nào thắng. |",
        "", "## Truy vết", ""]
    for root, manifest, folder in runs:
        lines.append(f"- {manifest['category']}: [{manifest['run_id']}](<{folder.as_posix()}/BAO_CAO_EDA_BO_SUNG.md>), Silver `{manifest['silver_run']}`.")
    lines += ["", "Encoder/token cap/chi phí mô hình chưa đo theo phạm vi đã thống nhất. Các quyết định Gold cần đọc thêm feature eligibility, train support, text flags và ghi chú rà mẫu."]
    (output / "BAO_CAO_TONG_HOP.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    (output / "summary.json").write_text(json.dumps(dict(runs=[m for _, m, _ in runs], population=collected), ensure_ascii=False, indent=2), encoding="utf-8")
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", nargs="+", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    print(summarize(args.runs, args.output))
