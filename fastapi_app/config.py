"""Application configuration loaded from environment variables."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    app_name: str = "spark-k8s-ingestion"
    app_version: str = "0.1.0"
    debug: bool = False

    # Metadata store
    metadata_store_type: str = "filesystem"  # filesystem | bigquery
    metadata_dir: str = "./data"

    # Pipeline configs
    pipelines_dir: str = "./pipelines"

    # Kubernetes / Spark
    k8s_master_url: str = "https://kubernetes.default.svc"
    spark_image: str = "apache/spark:4.0.0"
    spark_namespace: str = "default"
    spark_service_account: str = "spark"
    spark_home: str = "/opt/spark"
    spark_file_upload_path: str = ""

    # Deploy & execution defaults
    default_deploy_mode: str = "cluster"
    default_execution_mode: str = "local"

    # GCP / BigQuery
    bq_project_id: str = ""
    bq_dataset: str = ""
    gcp_credentials_path: str = ""

    # Dataproc
    dataproc_project: str = ""
    dataproc_region: str = ""
    dataproc_cluster: str = ""

    # Concurrency
    max_concurrent_jobs: int = 10

    model_config = {"env_prefix": "INGESTION_"}


settings = Settings()
