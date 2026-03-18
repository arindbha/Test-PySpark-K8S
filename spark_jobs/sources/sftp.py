"""SFTP source — downloads file via paramiko, then reads with Spark."""

from __future__ import annotations

import tempfile
from pathlib import Path

import paramiko
from pyspark.sql import DataFrame, SparkSession

from spark_jobs.sources.base import Source


class SftpSource(Source):
    def validate_config(self) -> None:
        for key in ("host", "remote_path"):
            if key not in self.config:
                raise ValueError(f"SftpSource requires '{key}' in config")

    def read(self, spark: SparkSession) -> DataFrame:
        host = self.config["host"]
        port = int(self.config.get("port", 22))
        username = self.config.get("username", "")
        password = self.config.get("password", "")
        key_path = self.config.get("key_path")
        remote_path = self.config["remote_path"]
        fmt = self.config.get("format", "csv")
        options = self.config.get("options", {})

        transport = paramiko.Transport((host, port))
        if key_path:
            pkey = paramiko.RSAKey.from_private_key_file(key_path)
            transport.connect(username=username, pkey=pkey)
        else:
            transport.connect(username=username, password=password)

        sftp = paramiko.SFTPClient.from_transport(transport)
        assert sftp is not None

        tmpdir = tempfile.mkdtemp(prefix="sftp_")
        local_path = str(Path(tmpdir) / Path(remote_path).name)
        sftp.get(remote_path, local_path)
        sftp.close()
        transport.close()

        return spark.read.format(fmt).options(**options).load(local_path)
