import os
import sys

import pytest
from pyspark.sql import SparkSession

# Pipeline modules live in transform/ and ingestion/, not a package —
# add them to sys.path so tests can import directly, same as the DAG does.
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "transform"))
sys.path.insert(0, os.path.join(ROOT, "ingestion"))


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .appName("retail-pipeline-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()
