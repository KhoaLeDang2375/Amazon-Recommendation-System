"""Run inside spark-notebook; verifies only the three-record sample dataset."""
import sys
import nbformat
from pyspark.sql import SparkSession

notebook = nbformat.read("/workspace/notebooks/01_eda.ipynb", as_version=4)
nbformat.validate(notebook)
for cell in notebook.cells:
    if cell.cell_type == "code":
        compile(cell.source, "01_eda.ipynb", "exec")
print("PASS: notebook format and code syntax are valid")

path = sys.argv[1] if len(sys.argv) > 1 else "hdfs://namenode:8020/project/testing/raw/smoke/v1"
spark = SparkSession.builder.master("local[2]").appName("ingestion-smoke").getOrCreate()
try:
    df = spark.read.option("recursiveFileLookup", "true").json(path)
    rows = df.select("id", "event", "value").collect()
    assert len(rows) == 3, f"Expected 3 sample records, got {len(rows)}"
    assert {(r.id, r.event, r.value) for r in rows} == {
        (1, "sample", 10), (2, "sample", 20), (3, "sample", 30)
    }, rows
    print("PASS: Spark read all 3 expected JSON records from Flume output in HDFS")
finally:
    spark.stop()
