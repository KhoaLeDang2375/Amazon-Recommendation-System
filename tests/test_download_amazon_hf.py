"""Offline checks; fixtures stay under tests, never real dataset directories."""
import hashlib
import importlib.util
import io
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("downloader", ROOT / "ingestion/scripts/download_amazon_hf.py")
downloader = importlib.util.module_from_spec(spec)
spec.loader.exec_module(downloader)


class DownloadTests(unittest.TestCase):
    def test_download_verifies_and_second_run_makes_no_transfer(self):
        with tempfile.TemporaryDirectory(dir=ROOT / "tests") as directory:
            root = Path(directory)
            content = b'{"title":"example"}\n'
            checksum = hashlib.sha256(content).hexdigest()
            api = MagicMock()
            api.repo_info.return_value = SimpleNamespace(sha="test-revision")
            api.get_paths_info.return_value = [
                SimpleNamespace(path=name, size=len(content), lfs={"sha256": checksum})
                for name in downloader.filenames(["All_Beauty"], "both")
            ]
            def transfer(repo, filename, **kwargs):
                path = Path(kwargs["local_dir"]) / filename
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(content)
                return str(path)
            fake = SimpleNamespace(HfApi=MagicMock(return_value=api), hf_hub_download=MagicMock(side_effect=transfer))
            with patch.object(downloader, "ROOT", root), patch.dict(sys.modules, huggingface_hub=fake), \
                    patch.object(sys, "argv", ["download", "--categories", "All_Beauty"]), \
                    patch.dict(downloader.os.environ, {}, clear=True), patch("sys.stdout", new_callable=io.StringIO):
                self.assertEqual(downloader.main(), 0)
                self.assertEqual(fake.hf_hub_download.call_count, 2)
                self.assertEqual(downloader.main(), 0)
                self.assertEqual(fake.hf_hub_download.call_count, 2)
            destination = root / "data/staging/amazon-reviews-2023"
            self.assertEqual((destination / "All_Beauty.jsonl").read_bytes(), content)
            self.assertFalse((destination / ".download.lock").exists())

    def test_verified_existing_file_is_reused_without_manifest(self):
        with tempfile.TemporaryDirectory(dir=ROOT / "tests") as directory:
            staging = Path(directory)
            destination = staging / "amazon-reviews-2023"
            destination.mkdir()
            path = staging / "meta_Video_Games.jsonl"
            content = b'{"title":"example"}\n'
            path.write_bytes(content)
            actual = downloader.existing_file(destination, staging, path.name, len(content), hashlib.sha256(content).hexdigest())
            self.assertEqual(actual, path)

    def test_same_size_corruption_is_not_overwritten(self):
        with tempfile.TemporaryDirectory(dir=ROOT / "tests") as directory:
            staging = Path(directory)
            path = staging / "Video_Games.jsonl"
            path.write_bytes(b"bad\n")
            with self.assertRaises(ValueError):
                downloader.existing_file(staging, staging, path.name, 4, hashlib.sha256(b"good").hexdigest())
            self.assertEqual(path.read_bytes(), b"bad\n")

    def test_partial_is_not_treated_as_complete(self):
        with tempfile.TemporaryDirectory(dir=ROOT / "tests") as directory:
            staging = Path(directory)
            (staging / "Video_Games.jsonl.incomplete").write_bytes(b"partial")
            self.assertIsNone(downloader.existing_file(staging, staging, "Video_Games.jsonl", 10, "unused"))

    def test_selection_is_exact_and_deduplicated(self):
        paths = downloader.filenames(list(downloader.CATEGORIES) * 2, "both")
        self.assertEqual(len(paths), 12)
        self.assertIn("raw/meta_categories/meta_Health_and_Personal_Care.jsonl", paths)
        self.assertIn("raw/review_categories/Industrial_and_Scientific.jsonl", paths)


if __name__ == "__main__":
    unittest.main()
