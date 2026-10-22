"""Read only the isolated smoke namespace, with bounded driver collection."""
from pathlib import Path, PurePosixPath
import sys
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'notebooks'))
from eda_recommendation import create_spark

DEFAULT_PATH = 'hdfs://namenode:8020/project/testing/raw/smoke/v1'


def validate_path(path):
    uri = urlsplit(path)
    if (uri.scheme != 'hdfs' or uri.netloc != 'namenode:8020'
            or not uri.path.startswith('/project/testing/')
            or '..' in PurePosixPath(uri.path).parts or '%' in uri.path or uri.query or uri.fragment):
        raise ValueError('Smoke must stay under hdfs://namenode:8020/project/testing/')
    return path


def verify(spark, path):
    validate_path(path)
    df = spark.read.option('recursiveFileLookup', 'true').json(path)
    # Four rows detect excess input without collecting an unbounded dataset.
    rows = df.select('id', 'event', 'value').limit(4).collect()
    assert len(rows) == 3, f'Expected exactly 3 smoke records; observed {len(rows)} (capped at 4)'
    assert {(r.id, r.event, r.value) for r in rows} == {
        (1, 'sample', 10), (2, 'sample', 20), (3, 'sample', 30)
    }, rows


def main():
    path = validate_path(sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PATH)
    import nbformat
    notebook_path = Path(__file__).resolve().parents[2] / 'notebooks/02_eda_recommendation.ipynb'
    notebook = nbformat.read(notebook_path, as_version=4)
    nbformat.validate(notebook)
    for cell in notebook.cells:
        if cell.cell_type == 'code':
            compile(cell.source, '02_eda_recommendation.ipynb', 'exec')
    spark = create_spark(shuffle_partitions=2)
    try:
        verify(spark, path)
        print('PASS: active notebook syntax and exactly three isolated HDFS smoke records')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
