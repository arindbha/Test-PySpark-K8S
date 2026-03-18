"""Application configuration loaded from environment variables."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    app_name: str = "spark-k8s-ingestion"
    app_version: str = "0.1.0"
    debug: bool = False

    k8s_namespace: str = "default"
    k8s_in_cluster: bool = True
    spark_image: str = "apache/spark:4.0.0"
    spark_service_account: str = "spark"

    gcp_project: str = ""
    gcs_temp_bucket: str = ""

    model_config = {"env_prefix": "INGESTION_"}


settings = Settings()
