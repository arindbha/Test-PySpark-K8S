"""Tests for destination implementations with mock DataFrame."""

from unittest.mock import MagicMock

import pytest

pyspark = pytest.importorskip("pyspark", reason="pyspark not installed")

from spark_jobs.destinations.base import get_destination
from spark_jobs.destinations.bigquery import BigQueryDestination
from spark_jobs.destinations.gcs import GcsDestination
from spark_jobs.destinations.jdbc import JdbcDestination


def _mock_df():
    df = MagicMock()
    df.write.format.return_value = df.write
    df.write.mode.return_value = df.write
    df.write.option.return_value = df.write
    df.write.options.return_value = df.write
    df.write.partitionBy.return_value = df.write
    df.write.save.return_value = None
    return df


def test_get_destination_gcs():
    dest = get_destination("gcs", {"path": "gs://bucket/out"})
    assert isinstance(dest, GcsDestination)


def test_get_destination_bigquery():
    dest = get_destination("bigquery", {"table": "proj.ds.t"})
    assert isinstance(dest, BigQueryDestination)


def test_get_destination_jdbc():
    dest = get_destination("jdbc", {"url": "jdbc:x", "table": "t"})
    assert isinstance(dest, JdbcDestination)


def test_get_destination_unknown():
    with pytest.raises(ValueError, match="Unknown destination type"):
        get_destination("foobar", {})


def test_gcs_destination_write():
    df = _mock_df()
    dest = GcsDestination({"path": "gs://bucket/out", "format": "parquet", "mode": "overwrite"})
    dest.write(df)
    df.write.format.assert_called_with("parquet")
    df.write.mode.assert_called_with("overwrite")
    df.write.save.assert_called_with("gs://bucket/out")


def test_gcs_destination_missing_path():
    with pytest.raises(ValueError, match="path"):
        GcsDestination({"format": "parquet"})


def test_bigquery_destination_write():
    df = _mock_df()
    dest = BigQueryDestination({"table": "proj.ds.t", "temp_gcs_bucket": "staging"})
    dest.write(df)
    df.write.format.assert_called_with("bigquery")


def test_bigquery_destination_missing_table():
    with pytest.raises(ValueError, match="table"):
        BigQueryDestination({})


def test_jdbc_destination_write():
    df = _mock_df()
    dest = JdbcDestination({"url": "jdbc:x", "table": "t", "mode": "append"})
    dest.write(df)
    df.write.format.assert_called_with("jdbc")
    df.write.mode.assert_called_with("append")


def test_jdbc_destination_missing_url():
    with pytest.raises(ValueError, match="url"):
        JdbcDestination({"table": "t"})
