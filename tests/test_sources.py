"""Tests for source implementations with mock SparkSession."""

import importlib
from unittest.mock import MagicMock

import pytest

pyspark = pytest.importorskip("pyspark", reason="pyspark not installed")

from spark_jobs.sources.base import get_source
from spark_jobs.sources.filesystem import FilesystemSource
from spark_jobs.sources.jdbc import JdbcSource


def _mock_spark():
    spark = MagicMock()
    spark.read.format.return_value = spark.read
    spark.read.options.return_value = spark.read
    spark.read.option.return_value = spark.read
    spark.read.load.return_value = MagicMock(name="DataFrame")
    return spark


def test_get_source_filesystem():
    source = get_source("filesystem", {"path": "/tmp/data.csv", "format": "csv"})
    assert isinstance(source, FilesystemSource)


def test_get_source_jdbc():
    source = get_source("jdbc", {"url": "jdbc:pg://host/db", "table": "t"})
    assert isinstance(source, JdbcSource)


def test_get_source_unknown():
    with pytest.raises(ValueError, match="Unknown source type"):
        get_source("foobar", {})


def test_filesystem_source_read():
    spark = _mock_spark()
    source = FilesystemSource({"path": "/data/input.parquet", "format": "parquet", "options": {"mergeSchema": "true"}})
    result = source.read(spark)
    spark.read.format.assert_called_with("parquet")
    spark.read.options.assert_called_with(mergeSchema="true")
    spark.read.load.assert_called_with("/data/input.parquet")


def test_filesystem_source_missing_path():
    with pytest.raises(ValueError, match="path"):
        FilesystemSource({"format": "csv"})


def test_jdbc_source_read():
    spark = _mock_spark()
    source = JdbcSource({"url": "jdbc:pg://host/db", "table": "orders", "driver": "org.postgresql.Driver"})
    result = source.read(spark)
    spark.read.format.assert_called_with("jdbc")


def test_jdbc_source_missing_url():
    with pytest.raises(ValueError, match="url"):
        JdbcSource({"table": "t"})


def test_jdbc_source_missing_table():
    with pytest.raises(ValueError, match="table"):
        JdbcSource({"url": "jdbc:x"})
