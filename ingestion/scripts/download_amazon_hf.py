"""Download selected Amazon Reviews 2023 raw JSONL files without publishing them."""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
REPO = "McAuley-Lab/Amazon-Reviews-2023"
CATEGORIES = (
    "All_Beauty", "Video_Games", "Sports_and_Outdoors", "Toys_and_Games",
    "Industrial_and_Scientific", "Health_and_Personal_Care",
)


def filenames(categories, kind):
    result = []
    for category in dict.fromkeys(categories):
        if kind in ("both", "reviews"):
            result.append(f"raw/review_categories/{category}.jsonl")
        if kind in ("both", "metadata"):
            result.append(f"raw/meta_categories/meta_{category}.jsonl")
    return result


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify(path, size, checksum):
    if not path.is_file() or path.stat().st_size != size:
        return False
    return sha256(path) == checksum


def existing_file(destination, staging, name, size, checksum):
    # Exact expected names only: no dataset/spool/archive directory traversal.
    for candidate in dict.fromkeys((destination / name, staging / name)):
        if candidate.exists():
            if not verify(candidate, size, checksum):
                raise ValueError(f"File co san khong khop nguon, khong ghi de: {candidate}")
            return candidate
    return None


def retry(action, attempts):
    for attempt in range(attempts):
        try:
            return action()
        except Exception:
            if attempt + 1 == attempts:
                raise
            time.sleep(min(2 ** attempt, 15))


def save_manifest(path, manifest):
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--categories", nargs="+", choices=CATEGORIES, default=list(CATEGORIES))
    parser.add_argument("--kind", choices=("both", "reviews", "metadata"), default="both")
    parser.add_argument("--output", type=Path, default=ROOT / "data/staging/amazon-reviews-2023")
    parser.add_argument("--revision", default="main")
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--attempts", type=int, default=3)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--ask-token", action="store_true", help="Nhap token an, khong luu token")
    parser.add_argument("--high-performance", action="store_true", help="Xet dung nhieu CPU/mang hon")
    args = parser.parse_args()
    if not 1 <= args.workers <= 12 or not 1 <= args.attempts <= 10:
        parser.error("workers: 1..12; attempts: 1..10")
    return args


def main():
    args = parse_args()
    if args.ask_token:
        from getpass import getpass
        token = getpass("HF token (Enter de tai public): ").strip() or False
    else:
        token = os.environ.get("HF_TOKEN") or False
    # Configure before importing Hub: its environment settings are read on import.
    os.environ.setdefault("HF_HOME", str(ROOT / "data/staging/.hf-home"))
    os.environ.setdefault("HF_HUB_DOWNLOAD_TIMEOUT", "60")
    os.environ.setdefault("HF_DEBUG", "0")
    if args.high_performance:
        os.environ["HF_XET_HIGH_PERFORMANCE"] = "1"
    try:
        from huggingface_hub import HfApi, hf_hub_download
    except ImportError:
        print("Cai dependencies: py -m pip install -r ingestion/requirements-download.txt", file=sys.stderr)
        return 1

    destination = args.output.resolve()
    staging = ROOT / "data/staging"
    # Download destinations must stay in staging, away from Flume and real HDFS data.
    if not destination.is_relative_to(staging.resolve()):
        print("--output phai nam trong data/staging.", file=sys.stderr)
        return 1
    destination.mkdir(parents=True, exist_ok=True)
    lock = destination / ".download.lock"
    try:
        descriptor = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        print(f"Dang co tien trinh tai hoac lock cu: {lock}", file=sys.stderr)
        return 1
    os.close(descriptor)
    try:
        api = HfApi(token=token, endpoint="https://huggingface.co")
        revision = retry(lambda: api.repo_info(REPO, repo_type="dataset", revision=args.revision).sha, args.attempts)
        selected = filenames(args.categories, args.kind)
        remote = retry(lambda: api.get_paths_info(REPO, selected, repo_type="dataset", revision=revision), args.attempts)
        by_name = {item.path: item for item in remote}
        plans = []
        for filename in selected:
            item = by_name.get(filename)
            lfs = getattr(item, "lfs", None)
            checksum = lfs.get("sha256") if isinstance(lfs, dict) else getattr(lfs, "sha256", None)
            if not item or not checksum or item.size is None:
                raise ValueError(f"Nguon thieu file/kich thuoc/SHA256: {filename}")
            name = Path(filename).name
            existing = existing_file(destination, staging, name, item.size, checksum)
            plans.append(dict(filename=filename, size=item.size, sha256=checksum,
                              path=str(existing or destination / name),
                              status="skipped_verified" if existing else "pending"))
            print(f"{'SKIP (SHA256 OK)' if existing else 'DOWNLOAD'} {name}: {item.size / 1e9:.3f} GB", flush=True)
        pending = [entry for entry in plans if entry["status"] == "pending"]
        required = sum(entry["size"] for entry in pending)
        print(f"Can tai: {len(pending)}/{len(plans)} file, {required / 1e9:.3f} GB; revision={revision}", flush=True)
        if args.dry_run:
            return 0
        if shutil.disk_usage(destination).free < required + 1024 ** 3:
            raise ValueError("Khong du dung luong trong (can du lieu moi + 1 GiB du phong).")
        manifest_path = destination / "download-manifest.json"
        manifest = dict(repo=REPO, revision=revision, files=plans,
                        checked_at=datetime.now(timezone.utc).isoformat())
        save_manifest(manifest_path, manifest)
        work = destination / ".hf-downloads"

        def download(entry):
            target = Path(entry["path"])
            def transfer():
                path = Path(hf_hub_download(REPO, entry["filename"], repo_type="dataset",
                            revision=revision, token=token, local_dir=work, endpoint="https://huggingface.co"))
                if not verify(path, entry["size"], entry["sha256"]):
                    # Preserve suspect bytes for review, bypass cached result on next run.
                    path.rename(path.with_name(path.name + f".invalid-{time.time_ns()}"))
                    raise ValueError("Checksum mismatch")
                return path
            path = retry(transfer, args.attempts)
            # Same volume: link atomically refuses any concurrently created target.
            # No second full copy and no overwrite of existing data.
            os.link(path, target)
            path.unlink()
            return "downloaded_verified"

        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(download, entry): entry for entry in pending}
            for future in as_completed(futures):
                entry = futures[future]
                try:
                    entry["status"] = future.result()
                    print(f"OK {Path(entry['path']).name}", flush=True)
                except Exception as error:
                    entry["status"] = "failed"
                    # Do not print request objects, URLs or exception text containing credentials.
                    entry["error_type"] = type(error).__name__
                    print(f"FAILED {Path(entry['path']).name}: {type(error).__name__}", file=sys.stderr)
                save_manifest(manifest_path, manifest)
        failures = sum(entry["status"] == "failed" for entry in plans)
        print(f"Hoan tat: {len(plans) - failures} OK/bo qua, {failures} loi. Manifest: {manifest_path}")
        return 1 if failures else 0
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 1
    except Exception as error:
        print(f"Loi {type(error).__name__}; kiem tra token/mang/quyen ghi, chay lai de tiep tuc.", file=sys.stderr)
        return 1
    finally:
        lock.unlink(missing_ok=True)


if __name__ == "__main__":
    sys.exit(main())
