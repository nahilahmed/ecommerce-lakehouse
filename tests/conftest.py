"""
Shared pytest fixtures for ShopMetrics unit tests.

The SparkSession is created once per test session (scope="session") to avoid
the overhead of spinning up a new JVM for every test file.
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("shopmetrics-unit-tests")
        .config("spark.sql.shuffle.partitions", "1")   # keeps tests fast
        .config("spark.default.parallelism", "1")
        .config("spark.ui.enabled", "false")           # no web UI during tests
        .getOrCreate()
    )
