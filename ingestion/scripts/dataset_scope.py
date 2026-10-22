"""Read the committed experiment scope; no dataset or network access."""
import json
from pathlib import Path
import re


def load_scope(path=None):
    path = Path(path) if path is not None else Path(__file__).resolve().parents[1] / "manifests/experiment-scope.json"
    scope = json.loads(path.read_text(encoding="utf-8"))
    categories = scope.get("categories") if isinstance(scope, dict) else None
    if (not isinstance(categories, list) or not categories
            or any(not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9_]+", name) for name in categories)
            or len(set(categories)) != len(categories)
            or scope.get("kinds") != ["reviews", "metadata"]
            or not isinstance(scope.get("version"), str)
            or not re.fullmatch(r"[A-Za-z0-9_-]+", scope["version"])
            or scope.get("hdfs_base") != "/project/raw"
            or scope.get("dataset") != "Amazon Reviews 2023"):
        raise ValueError("Invalid experiment scope; inspect ingestion/manifests/experiment-scope.json")
    return scope
