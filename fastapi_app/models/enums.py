"""Enumerations used across the ingestion framework."""

from __future__ import annotations

import enum


class SourceType(str, enum.Enum):
    JDBC = "jdbc"
    API = "api"
    FILESYSTEM = "filesystem"
    SFTP = "sftp"
    CUSTOM = "custom"


class DestinationType(str, enum.Enum):
    GCS = "gcs"
    BIGQUERY = "bigquery"
    JDBC = "jdbc"


class RunStatus(str, enum.Enum):
    PENDING = "pending"
    QUEUED = "queued"
    SUBMITTED = "submitted"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ExecutionMode(str, enum.Enum):
    K8S = "k8s"
    DATAPROC = "dataproc"
    LOCAL = "local"


class DeployMode(str, enum.Enum):
    CLUSTER = "cluster"
    CLIENT = "client"


class TransformType(str, enum.Enum):
    SQL = "sql"
    COLUMN_RENAME = "column_rename"
    FILTER = "filter"
    TYPE_CAST = "type_cast"
    DROP_COLUMNS = "drop_columns"
    ADD_COLUMN = "add_column"
