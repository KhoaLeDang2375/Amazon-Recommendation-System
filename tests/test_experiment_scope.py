"""Scope checks without Spark, Docker, network or real dataset reads."""
import ast
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "ingestion/scripts"))
from dataset_scope import load_scope


class ScopeTests(unittest.TestCase):
    def test_scope_and_driver_use_same_categories(self):
        scope = load_scope()
        self.assertEqual(scope["categories"], ["All_Beauty", "Video_Games", "Industrial_and_Scientific"])
        # Extract driver helpers only: do not import PySpark or create a Spark session.
        tree = ast.parse((ROOT / "notebooks/eda_recommendation.py").read_text(encoding="utf-8"))
        helpers = [node for node in tree.body if isinstance(node, ast.FunctionDef)
                   and node.name in ("get_scope", "get_categories")]
        namespace = {"Path": Path, "__file__": str(ROOT / "notebooks/eda_recommendation.py")}
        exec(compile(ast.Module(body=helpers, type_ignores=[]), "scope_helpers", "exec"), namespace)
        self.assertEqual(list(namespace["get_categories"]()), scope["categories"])

    def test_invalid_scope_fails_before_data_access(self):
        for categories in ([], ["All_Beauty", "All_Beauty"], ["../other"], [123], "All_Beauty"):
            with self.subTest(categories=categories), tempfile.TemporaryDirectory(dir=ROOT / "tests") as directory:
                scope = dict(load_scope(), categories=categories)
                path = Path(directory) / "scope.json"
                path.write_text(json.dumps(scope), encoding="utf-8")
                with self.assertRaises(ValueError):
                    load_scope(path)

    def test_notebook_selects_one_category_from_scope(self):
        notebook = json.loads((ROOT / "notebooks/02_eda_recommendation.ipynb").read_text(encoding="utf-8"))
        source = "\n".join("".join(cell["source"]) for cell in notebook["cells"] if cell["cell_type"] == "code")
        assignments = [node for node in ast.walk(ast.parse(source)) if isinstance(node, ast.Assign)
                       and any(isinstance(target, ast.Name) and target.id == "SELECTED_CATEGORIES" for target in node.targets)]
        self.assertEqual(len(assignments), 1)
        self.assertEqual(ast.unparse(assignments[0].value), "scope['categories'][:1]")


if __name__ == "__main__":
    unittest.main()
