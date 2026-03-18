"""REST API source — fetches data from paginated HTTP endpoints on the driver."""

from __future__ import annotations

import requests
from pyspark.sql import DataFrame, SparkSession

from spark_jobs.sources.base import Source


class ApiSource(Source):
    def validate_config(self) -> None:
        if "url" not in self.config:
            raise ValueError("ApiSource requires 'url' in config")

    def read(self, spark: SparkSession) -> DataFrame:
        url = self.config["url"]
        headers = self.config.get("headers", {})
        params = self.config.get("params", {})
        pagination_key = self.config.get("pagination_key")
        data_key = self.config.get("data_key")
        max_pages = int(self.config.get("max_pages", 100))

        all_records: list[dict] = []
        page = 1

        while page <= max_pages:
            if pagination_key:
                params[pagination_key] = str(page)
            resp = requests.get(url, headers=headers, params=params, timeout=60)
            resp.raise_for_status()
            payload = resp.json()

            records = payload if data_key is None else payload.get(data_key, [])
            if not records:
                break
            all_records.extend(records)
            page += 1

        return spark.createDataFrame(all_records)
