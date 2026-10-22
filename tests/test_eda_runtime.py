"""Lifecycle/error checks inside Spark image; no JVM or real dataset needed."""
import json
import importlib.util
from pathlib import Path
import signal
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'notebooks'))
import eda_recommendation as eda

smoke_spec = importlib.util.spec_from_file_location('verify_smoke', Path(__file__).resolve().parent / 'integration/verify-smoke.py')
smoke = importlib.util.module_from_spec(smoke_spec)
smoke_spec.loader.exec_module(smoke)


class RuntimeChecks(unittest.TestCase):
    def test_smoke_rejects_raw_before_session_and_bounds_collection(self):
        for path in ('hdfs://namenode:8020/project/raw/all_beauty_reviews/v1',
                     'hdfs://namenode:8020/project/testing/../raw',
                     'hdfs://namenode:8020/project/testing/%2e%2e/raw',
                     'file:///project/testing/raw/smoke/v1'):
            with self.subTest(path=path), patch.object(smoke, 'create_spark') as create, patch.object(sys, 'argv', ['smoke', path]):
                with self.assertRaises(ValueError):
                    smoke.main()
                create.assert_not_called()
        spark = MagicMock()
        frame = spark.read.option.return_value.json.return_value
        records = [SimpleNamespace(id=i, event='sample', value=i * 10) for i in (1, 2, 3)]
        frame.select.return_value.limit.return_value.collect.return_value = records
        smoke.verify(spark, smoke.DEFAULT_PATH)
        frame.select.return_value.limit.assert_called_once_with(4)
        frame.select.return_value.limit.return_value.collect.return_value = records + records[:1]
        with self.assertRaises(AssertionError):
            smoke.verify(spark, smoke.DEFAULT_PATH)

    def test_low_disk_or_multi_category_never_creates_spark(self):
        with patch.object(eda, 'create_spark') as create, patch.object(eda.shutil, 'disk_usage', return_value=SimpleNamespace(free=1)):
            with self.assertRaises(RuntimeError):
                eda.run_eda(output=tempfile.gettempdir())
            with self.assertRaises(ValueError):
                eda.run_eda(['All_Beauty', 'Video_Games'])
            create.assert_not_called()

    def test_owned_session_stops_on_success_failure_and_termination(self):
        previous = signal.getsignal(signal.SIGTERM)
        for outcome in ('success', 'failure', 'termination'):
            with self.subTest(outcome=outcome):
                spark = MagicMock()
                def analyze(*args):
                    if outcome == 'failure':
                        raise RuntimeError('fixture failure')
                    if outcome == 'termination':
                        signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
                    return Path('fixture-report')
                with patch.object(eda, 'create_spark', return_value=spark), patch.object(eda, 'check_disk_space'), patch.object(eda, 'analyze_all', side_effect=analyze):
                    if outcome == 'success':
                        self.assertEqual(eda.run_eda(), Path('fixture-report'))
                    else:
                        with self.assertRaises(RuntimeError if outcome == 'failure' else KeyboardInterrupt):
                            eda.run_eda()
                spark.stop.assert_called_once()
                self.assertEqual(signal.getsignal(signal.SIGTERM), previous)

    def test_load_registers_cache_before_failed_action(self):
        spark, parsed, report = MagicMock(), MagicMock(), MagicMock()
        spark.createDataFrame.return_value = parsed
        parsed.filter.return_value.select.return_value.collect.side_effect = RuntimeError('failed scan')
        persisted = []
        with patch.object(eda, 'inventory', return_value=[{'path': 'fixture.jsonl'}]), patch.object(eda.F, 'input_file_name'):
            with self.assertRaises(RuntimeError):
                eda.load(spark, 'fixture', 'reviews', report, persisted)
        self.assertEqual(persisted, [parsed])

    def test_category_load_failure_releases_all_registered_cache(self):
        first, second = MagicMock(), MagicMock()
        first.unpersist.side_effect = RuntimeError('cleanup failure')
        def load(spark, path, dataset, report, persisted):
            persisted.append(first if dataset == 'reviews' else second)
            if dataset == 'metadata':
                raise ValueError('metadata load failed')
            return MagicMock(), first, {}
        with tempfile.TemporaryDirectory() as directory, patch.object(eda, 'load', side_effect=load):
            with self.assertRaisesRegex(ValueError, 'metadata load failed'):
                eda.analyze_category(MagicMock(), 'All_Beauty', 'fixture', 'v1', directory, '2022-01-01')
        first.unpersist.assert_called_once_with(blocking=True)
        second.unpersist.assert_called_once_with(blocking=True)

    def test_failed_and_interrupted_runs_preserve_status(self):
        for failure, status in [(ValueError('fixture failure'), 'failed'), (KeyboardInterrupt(), 'interrupted')]:
            with self.subTest(status=status), tempfile.TemporaryDirectory() as directory:
                spark = MagicMock()
                spark.conf.get.return_value = 'fixture-config'
                with patch.object(eda, 'inventory', return_value=[]), patch.object(eda, 'analyze_category', side_effect=failure):
                    with self.assertRaises(type(failure)):
                        eda.analyze_all(spark, output=directory, min_free_gb=0)
                root = next(Path(directory).glob('run_*'))
                info = json.loads((root / 'run.json').read_text())
                self.assertEqual(info['status'], status)
                self.assertEqual(info['completed_categories'], [])
                self.assertFalse((root / '_work').exists())

    def test_phase_disk_drop_is_recorded_and_stops_next_phase(self):
        with tempfile.TemporaryDirectory() as directory:
            monitor = eda.RuntimeMonitor(directory, 20)
            with patch.object(eda.shutil, 'disk_usage', return_value=SimpleNamespace(free=50 * 1024 ** 3)):
                monitor.checkpoint('before')
            with patch.object(eda.shutil, 'disk_usage', return_value=SimpleNamespace(free=1)):
                with self.assertRaises(RuntimeError):
                    monitor.checkpoint('after')
            data = json.loads((Path(directory) / 'resources.json').read_text())
            self.assertEqual([x['phase'] for x in data['samples']], ['before', 'after'])
            self.assertEqual(data['minimum_temporary_free_bytes'], 1)

    def test_existing_session_is_not_reused_or_stopped(self):
        with patch.object(eda, 'SparkSession') as session:
            session.getActiveSession.return_value = object()
            with self.assertRaises(RuntimeError):
                eda.create_spark()
            session.builder.getOrCreate.assert_not_called()
        with patch.object(eda, 'SparkSession') as session, patch.object(eda, 'SparkContext') as context:
            session.getActiveSession.return_value = None
            session._instantiatedSession = None
            context._active_spark_context = object()
            with self.assertRaises(RuntimeError):
                eda.create_spark()
            session.builder.getOrCreate.assert_not_called()


if __name__ == '__main__':
    unittest.main()
