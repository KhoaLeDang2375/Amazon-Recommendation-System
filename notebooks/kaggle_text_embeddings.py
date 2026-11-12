"""Self-contained Kaggle encoder: one spawned process per GPU, bounded Parquet IO."""
import argparse
import hashlib
import importlib.metadata
import json
import os
import shutil
import signal
import time
from pathlib import Path


def sha256(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 ** 2), b""):
            value.update(chunk)
    return value.hexdigest()


def save_json(path, data):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def child_path(root, relative):
    root = Path(root).resolve()
    path = (root / relative).resolve()
    if path == root or not path.is_relative_to(root) or path.suffix != ".parquet":
        raise ValueError("Manifest path escapes input/output package")
    return path


def normalize_rows(total):
    import numpy as np
    norm = np.linalg.norm(total, axis=1, keepdims=True)
    if not np.isfinite(total).all() or (norm <= 0).any():
        raise ValueError("Invalid embedding norm")
    return (total / norm).astype("float32")


def encode_batch(model, texts, batch_size, policy, device):
    """Tokenize overflow IDs directly; no decode/re-tokenize at chunk boundaries."""
    import numpy as np
    import torch
    tokenizer = model.tokenizer
    overflow = policy == "chunk_mean"
    if overflow and not tokenizer.is_fast:
        raise ValueError("chunk_mean requires a fast tokenizer with overflow-to-sample mapping")
    encoded = tokenizer(texts, truncation=True, max_length=model.max_seq_length, padding=False,
        return_overflowing_tokens=overflow, return_special_tokens_mask=True)
    if overflow and "overflow_to_sample_mapping" not in encoded:
        raise ValueError("Tokenizer did not return all chunks; refusing silent truncation")
    owners = np.asarray(encoded.get("overflow_to_sample_mapping", list(range(len(texts)))), dtype="int64")
    weights = np.asarray([max(1, sum(1 - v for v in mask)) for mask in encoded["special_tokens_mask"]], dtype="float32")
    total = np.zeros((len(texts), model.get_sentence_embedding_dimension()), dtype="float32")
    chunk_counts = np.bincount(owners, minlength=len(texts))
    token_counts = np.bincount(owners, weights=weights, minlength=len(texts)).astype("int64")
    fields = [name for name in tokenizer.model_input_names if name in encoded]
    for start in range(0, len(owners), batch_size):
        end = min(len(owners), start + batch_size)
        features = tokenizer.pad([{name: encoded[name][i] for name in fields} for i in range(start, end)], padding=True, return_tensors="pt")
        features = {name: value.to(device) for name, value in features.items()}
        with torch.inference_mode():
            embeddings = model(features)["sentence_embedding"].float()
            embeddings = torch.nn.functional.normalize(embeddings, p=2, dim=1).cpu().numpy()
        np.add.at(total, owners[start:end], embeddings * weights[start:end, None])
    return normalize_rows(total), token_counts, chunk_counts


def validate_input(root, entry):
    import pyarrow.parquet as pq
    path = child_path(root, entry["path"])
    if not path.is_file() or path.stat().st_size != entry["bytes"] or sha256(path) != entry["sha256"]:
        raise ValueError("Input checksum mismatch: " + entry["path"])
    if pq.ParquetFile(path).metadata.num_rows != entry["rows"]:
        raise ValueError("Input row count mismatch")
    return path


def restore_shard(entry, output, signature, min_free_gib=5, resume=None):
    """Restore/validate completed checkpoints without loading a GPU model."""
    import pyarrow.parquet as pq
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    metadata = output.with_suffix(".json")
    if resume:
        previous = Path(resume)
        previous_meta = previous.with_suffix(".json")
        if not output.exists() and previous.is_file() and previous_meta.is_file():
            state = json.loads(previous_meta.read_text())
            if state.get("status") != "complete" or state.get("signature") != signature or state.get("input_sha256") != entry["sha256"] or sha256(previous) != state.get("output_sha256"):
                raise ValueError("Resume bundle checksum/config mismatch")
            if shutil.disk_usage(output.parent).free < previous.stat().st_size + min_free_gib * 2 ** 30:
                raise RuntimeError("Insufficient space to copy resume shard")
            shutil.copyfile(previous, output)
            shutil.copyfile(previous_meta, metadata)
    if output.exists() and metadata.exists():
        state = json.loads(metadata.read_text())
        if state.get("status") != "complete" or state.get("signature") != signature or state.get("input_sha256") != entry["sha256"]:
            raise ValueError("Existing output belongs to another input/config")
        if sha256(output) != state["output_sha256"] or pq.ParquetFile(output).metadata.num_rows != entry["rows"]:
            raise ValueError("Existing output corrupted")
        return state
    return None


def encode_shard(root, entry, output, signature, encoder, io_rows=128, min_free_gib=5, resume=None):
    """Encoder callback makes IO/checkpoint verification testable without a GPU."""
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq
    source = validate_input(root, entry)
    output = Path(output)
    state = restore_shard(entry, output, signature, min_free_gib, resume)
    if state:
        return state
    metadata = output.with_suffix(".json")
    if shutil.disk_usage(output.parent).free < entry["rows"] * signature["dimension"] * 4 + min_free_gib * 2 ** 30:
        raise RuntimeError("Output disk reserve is insufficient")
    schema = pa.schema([("text_id", pa.string()), ("embedding", pa.list_(pa.float32(), signature["dimension"])),
        ("encoded_tokens", pa.int64()), ("chunks", pa.int32())])
    temporary = output.with_suffix(".parquet.tmp")
    rows = tokens = chunks = 0
    seen = set()
    key_digest = hashlib.sha256()
    started = time.monotonic()
    with pq.ParquetWriter(temporary, schema, compression="snappy") as writer:
        for batch in pq.ParquetFile(source).iter_batches(batch_size=io_rows, columns=["text_id", "text"]):
            values = batch.to_pydict()
            ids, texts = values["text_id"], values["text"]
            for identifier, text in zip(ids, texts):
                expected = hashlib.sha256((signature["cleaning_version"] + "\0" + text).encode("utf-8")).hexdigest()
                if not text or identifier != expected or identifier in seen:
                    raise ValueError("Blank, duplicate or mismatched text_id")
                seen.add(identifier)
                key_digest.update((identifier + "\n").encode())
            vectors, token_count, chunk_count = encoder(texts)
            vectors = np.asarray(vectors, dtype="float32")
            if vectors.shape != (len(ids), signature["dimension"]) or not np.isfinite(vectors).all():
                raise ValueError("Wrong embedding shape or non-finite value")
            if not np.allclose(np.linalg.norm(vectors, axis=1), 1, atol=0.002):
                raise ValueError("Embeddings must be L2 normalized")
            table = pa.Table.from_arrays([pa.array(ids), pa.FixedSizeListArray.from_arrays(pa.array(vectors.ravel(), type=pa.float32()), signature["dimension"]),
                pa.array(token_count, type=pa.int64()), pa.array(chunk_count, type=pa.int32())], schema=schema)
            writer.write_table(table)
            rows += len(ids)
            tokens += int(sum(token_count))
            chunks += int(sum(chunk_count))
            if shutil.disk_usage(output.parent).free < min_free_gib * 2 ** 30:
                raise RuntimeError("Output disk reserve reached")
    if rows != entry["rows"] or pq.ParquetFile(temporary).metadata.num_rows != rows:
        raise ValueError("Output count mismatch")
    temporary.replace(output)
    state = dict(status="complete", signature=signature, input_sha256=entry["sha256"], rows=rows,
        encoded_tokens=tokens, chunks=chunks, text_ids_sha256=key_digest.hexdigest(), output_sha256=sha256(output),
        seconds=round(time.monotonic()-started, 3))
    save_json(metadata, state)
    return state


def pending_tasks(tasks, max_shards=None, min_free_gib=5):
    if max_shards is not None and (isinstance(max_shards, bool) or not isinstance(max_shards, int) or max_shards < 1):
        raise ValueError("max_shards_per_run must be null or a positive integer")
    pending = []
    for task in tasks:
        if not restore_shard(task["entry"], task["output"], task["signature"], min_free_gib, task["resume"]):
            pending.append(task)
    return pending if max_shards is None else pending[:max_shards]


def dataset_snapshot(bundle, destination):
    """Hardlink verified vector shards only; omit raw input, pilot and temporary files."""
    bundle, destination = Path(bundle), Path(destination)
    manifest = json.loads((bundle / "embedding_manifest.json").read_text())
    if manifest.get("status") not in ("partial", "complete") or not manifest.get("shards"):
        raise ValueError("Need a partial/complete bundle with at least one verified shard")
    target = destination / bundle.name
    target.mkdir(parents=True, exist_ok=False)
    for state in manifest["shards"]:
        source = child_path(bundle, state["path"])
        checked = restore_shard(dict(sha256=state["input_sha256"], rows=state["rows"]), source, manifest["signature"], 0)
        if checked != {key: value for key, value in state.items() if key != "path"}:
            raise ValueError("Dataset snapshot checkpoint differs from manifest")
        output = child_path(target, state["path"])
        output.parent.mkdir(parents=True, exist_ok=True)
        os.link(source, output)
        shutil.copyfile(source.with_suffix(".json"), output.with_suffix(".json"))
    save_json(target / "embedding_manifest.json", manifest)
    return destination


def worker(device, tasks, config, snapshot):
    import torch
    from sentence_transformers import SentenceTransformer
    torch.set_num_threads(2)
    os.environ["TOKENIZERS_PARALLELISM"] = "false"
    model = SentenceTransformer(snapshot, device=device, trust_remote_code=False)
    model.eval()
    model.max_seq_length = config["encoder"]["max_seq_length"]
    if model.get_sentence_embedding_dimension() != config["encoder"]["dimension"]:
        raise ValueError("Model dimension mismatch")
    if config["compute_dtype"] == "float16":
        model.half()
    for task in tasks:
        signature = task["signature"]
        encode = lambda texts: encode_batch(model, texts, config["batch_size"], signature["long_text_policy"], device)
        encode_shard(task["root"], task["entry"], task["output"], signature, encode,
            config["io_rows"], config["min_free_gib"], task["resume"])
        print(device, task["entry"]["path"], "complete", flush=True)


def run(config):
    import torch
    import pyarrow.parquet as pq
    from huggingface_hub import snapshot_download
    if config["long_text_policy"] not in ("head", "chunk_mean") or config["compute_dtype"] not in ("float16", "float32"):
        raise ValueError("Invalid encoding policy")
    if min(config["batch_size"], config["io_rows"]) < 1 or config["min_free_gib"] < 0:
        raise ValueError("Invalid batch size/reserve")
    if len(config["gpu_ids"]) != 2 or len(set(config["gpu_ids"])) != 2 or any(i < 0 or i >= torch.cuda.device_count() for i in config["gpu_ids"]):
        raise RuntimeError("Select Kaggle GPU T4 x2; this notebook requires two distinct CUDA GPUs")
    roots = [Path(p).resolve() for p in config["input_roots"]]
    if not roots:
        roots = sorted(p.parent for p in Path("/kaggle/input").rglob("export_manifest.json"))
    if not roots or len(roots) != len(set(roots)):
        raise ValueError("Set input_roots to Gold Kaggle package directories")
    output = Path(config["output_root"]).resolve()
    if any(output.is_relative_to(root) or root.is_relative_to(output) for root in roots):
        raise ValueError("Output and read-only inputs must be separate")
    output.mkdir(parents=True, exist_ok=True)
    encoder_config = config["encoder"]
    snapshot = snapshot_download(encoder_config["model"], revision=encoder_config["revision"],
        allow_patterns=["*.json", "*.safetensors", "*.txt", "1_Pooling/*"])
    versions = {name: importlib.metadata.version(name) for name in ("torch", "transformers", "sentence-transformers", "pyarrow", "numpy")}
    tasks = []
    packages = []
    from transformers import AutoTokenizer
    tokenizer = AutoTokenizer.from_pretrained(snapshot, trust_remote_code=False)
    for root in roots:
        manifest_path = root / "export_manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("status") != "complete" or manifest.get("export_version") != "gold_text_export_v1":
            raise ValueError("Export must be complete")
        if encoder_config.get("output_dtype") != "float32" or any(manifest["model_contract"][key] != encoder_config[key] for key in ("model", "revision", "dimension", "max_seq_length", "normalize_embeddings", "output_dtype")):
            raise ValueError("Encoder differs from export contract")
        if manifest["model_contract"].get("long_text_policy") != config["long_text_policy"]:
            raise ValueError("Long-text policy differs from export contract")
        paths = [entry["path"] for entry in manifest["files"]]
        if len(paths) != len(set(paths)) or sum(entry["rows"] for entry in manifest["files"]) != manifest["texts"]:
            raise ValueError("Duplicate shards or incorrect export counts")
        signature = dict(encoder_config, cleaning_version=manifest["cleaning_version"], long_text_policy=config["long_text_policy"],
            export_manifest_sha256=sha256(manifest_path), compute_dtype=config["compute_dtype"], versions=versions,
            encoder_code_sha256=sha256(__file__), batch_size=config["batch_size"], io_rows=config["io_rows"])
        fingerprint = hashlib.sha256(json.dumps(signature, sort_keys=True).encode()).hexdigest()[:16]
        bundle_name = manifest["category"] + "_" + manifest["gold_run"] + "_" + fingerprint
        if Path(bundle_name).name != bundle_name:
            raise ValueError("Invalid bundle name")
        if any(p["destination"].name == bundle_name for p in packages):
            raise ValueError("Duplicate Gold export bundle; select one copy of each package")
        destination = output / bundle_name
        destination.mkdir(exist_ok=True)
        pilot = []
        for entry in manifest["pilot_files"]:
            path = validate_input(root, entry)
            for batch in pq.ParquetFile(path).iter_batches(batch_size=128):
                for row in batch.to_pylist():
                    length = len(tokenizer(row["text"], truncation=False)["input_ids"])
                    pilot.append(dict(kind=row["kind"], characters=row["characters"], tokens=length,
                        would_truncate=length > encoder_config["max_seq_length"]))
        save_json(destination / "pilot_tokens.json", dict(sample="length-stratified; not a population estimate", rows=pilot))
        print(manifest["category"], "texts=", manifest["texts"], "pilot long texts=", sum(r["would_truncate"] for r in pilot), "/", len(pilot), flush=True)
        package_tasks = []
        for entry in manifest["files"]:
            relative = Path(entry["path"])
            target = child_path(destination, entry["path"])
            previous = str(Path(config["resume_root"]) / bundle_name / relative) if config.get("resume_root") else None
            package_tasks.append(dict(root=str(root), entry=entry, output=str(target), signature=signature, resume=previous))
        tasks.extend(package_tasks)
        packages.append(dict(destination=destination, manifest=manifest, signature=signature, tasks=package_tasks))
    tasks = [] if config.get("pilot_only") else pending_tasks(tasks, config.get("max_shards_per_run"), config["min_free_gib"])
    print("New shards scheduled this session:", len(tasks), flush=True)
    hardware = [dict(id=i, name=torch.cuda.get_device_name(i), memory=torch.cuda.get_device_properties(i).total_memory) for i in config["gpu_ids"]]
    if not config.get("pilot_only"):
        missing = [task for task in tasks if not Path(task["output"]).exists()]
        needed = int(sum(task["entry"]["rows"] * encoder_config["dimension"] * 4 * 1.15 for task in missing))
        # Parquet can briefly coexist with its finalized shard on each GPU worker.
        needed += 2 * max((task["entry"]["rows"] for task in tasks), default=0) * encoder_config["dimension"] * 4
        free = shutil.disk_usage(output).free
        print("Estimated additional vector bytes:", needed, "available:", free, flush=True)
        if needed + config["min_free_gib"] * 2 ** 30 > free:
            raise RuntimeError("Insufficient output space for selected packages; choose fewer input_roots and encode one category per session")
    context = __import__("multiprocessing").get_context("spawn")
    # Assign largest input shards first by byte load; workers never share one output file.
    assigned = [[], []]
    load = [0, 0]
    for task in sorted(tasks, key=lambda t:t["entry"]["bytes"], reverse=True):
        rank = min(range(2), key=lambda i:load[i])
        assigned[rank].append(task)
        load[rank] += task["entry"]["bytes"]
    for rank in range(2):
        pilot_tasks = []
        for root, package in zip(roots, packages):
            for entry in package["manifest"]["pilot_files"]:
                pilot_tasks.append(dict(root=str(root), entry=entry,
                    output=str(package["destination"] / "_pilot" / f"gpu{rank}" / Path(entry["path"]).name),
                    signature=package["signature"], resume=None))
        assigned[rank] = pilot_tasks + ([] if config.get("pilot_only") else assigned[rank])
    for package in packages:
        save_json(package["destination"] / "embedding_manifest.json", dict(status="running", signature=package["signature"], config=config, hardware=hardware))
    processes = [context.Process(target=worker, args=(f"cuda:{gpu}", assigned[i], config, snapshot)) for i, gpu in enumerate(config["gpu_ids"])]
    try:
        for process in processes:
            process.start()
        while any(p.is_alive() for p in processes):
            for process in processes:
                process.join(timeout=1)
                if process.exitcode not in (None, 0):
                    raise RuntimeError("GPU worker failed; complete shard checkpoints are retained")
        if any(p.exitcode != 0 for p in processes):
            raise RuntimeError("GPU worker failed")
        for package in packages:
            pilot_states = [dict(path=p.relative_to(package["destination"]).as_posix(), **json.loads(p.read_text())) for p in (package["destination"] / "_pilot").rglob("*.json")]
            save_json(package["destination"] / "pilot_throughput.json", dict(hardware=hardware, shards=pilot_states,
                note="Small stratified sample; model loading and token pilot excluded; not full-corpus throughput"))
            print(package["manifest"]["category"], "pilot rows/s per shard:",
                [round(s["rows"] / max(s["seconds"], 0.001), 2) for s in pilot_states], flush=True)
        if config.get("pilot_only"):
            for package in packages:
                save_json(package["destination"] / "embedding_manifest.json", dict(status="pilot_complete", signature=package["signature"], hardware=hardware))
            return
        for package in packages:
            states = []
            for task in package["tasks"]:
                state = restore_shard(task["entry"], task["output"], package["signature"], config["min_free_gib"])
                if state:
                    states.append(dict(path=Path(task["output"]).relative_to(package["destination"]).as_posix(), **state))
            complete = len(states) == len(package["tasks"])
            if complete and sum(s["rows"] for s in states) != package["manifest"]["texts"]:
                raise ValueError("Final row conservation failed")
            save_json(package["destination"] / "embedding_manifest.json", dict(status="complete" if complete else "partial", signature=package["signature"], config=config, hardware=hardware,
                gold_path=package["manifest"]["gold_path"], shards=states, texts=package["manifest"]["texts"],
                completed_texts=sum(s["rows"] for s in states), total_shards=len(package["tasks"])))
    except BaseException as error:
        for package in packages:
            save_json(package["destination"] / "embedding_manifest.json", dict(status="failed", signature=package["signature"], error=f"{type(error).__name__}: {error}"))
        raise
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
            if process.pid is not None:
                process.join(timeout=30)
    print("Session finished; check complete/partial in embedding_manifest.json:", output, flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    def interrupted(signum, frame):
        raise InterruptedError("Encoding interrupted; complete shard checkpoints are retained")
    signal.signal(signal.SIGTERM, interrupted)
    run(json.loads(Path(args.config).read_text(encoding="utf-8")))
