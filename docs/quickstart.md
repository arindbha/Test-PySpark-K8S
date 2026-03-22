# Quickstart Guide

This guide walks you through setting up and running your first pipeline — locally and on Kubernetes.

## 1. Local Development Setup

### Install dependencies

```bash
# Clone the repository
git clone <repo-url> && cd Test-PySpark-K8S

# Create a virtual environment (recommended)
python -m venv .venv && source .venv/bin/activate

# Install the project with dev dependencies
pip install -e ".[dev]"
```

### Start the API server

```bash
make run-local
```

The FastAPI server starts at **http://localhost:8000**. Open http://localhost:8000/docs for the interactive Swagger UI.

### Verify it works

```bash
curl http://localhost:8000/v1/health
```

Expected response:

```json
{"status": "healthy"}
```

## 2. Define a Pipeline

Pipelines are YAML files in the `pipelines/` directory. Here is a minimal example that reads a CSV file, filters rows, and writes Parquet output:

```yaml
pipeline_name: my_first_pipeline
execution_mode: local
datasets:
  - source:
      name: users
      type: filesystem
      config:
        path: /data/input/users.csv
        format: csv
        options:
          header: "true"
          inferSchema: "true"
    destination:
      type: gcs
      config:
        path: /tmp/output/users.parquet
        format: parquet
        mode: overwrite
    transforms:
      - type: filter
        config:
          condition: "age >= 21"
spark_config:
  driver_memory: 1g
  executor_memory: 2g
  executor_instances: 1
```

Save this as `pipelines/my_first_pipeline.yaml`.

## 3. Run a Pipeline

### List available pipelines

```bash
curl http://localhost:8000/v1/pipelines
```

### Trigger a pipeline run

```bash
curl -X POST http://localhost:8000/v1/pipelines/run \
  -H "Content-Type: application/json" \
  -d '{"pipeline_name": "my_first_pipeline"}'
```

The response includes a `run_id`:

```json
{
  "run_id": "abc123-...",
  "status": "QUEUED"
}
```

### Check run status

```bash
curl http://localhost:8000/v1/pipelines/<run_id>/status
```

### Cancel a run

```bash
curl -X DELETE http://localhost:8000/v1/runs/<run_id>/cancel
```

## 4. Pipeline Features

### Runtime overrides

Override Spark config or execution mode at request time:

```bash
curl -X POST http://localhost:8000/v1/pipelines/run \
  -H "Content-Type: application/json" \
  -d '{
    "pipeline_name": "my_first_pipeline",
    "execution_mode": "k8s",
    "spark_config": {
      "executor_instances": 5
    }
  }'
```

### Batch execution

Run multiple pipelines in one call:

```bash
curl -X POST http://localhost:8000/v1/pipelines/run-batch \
  -H "Content-Type: application/json" \
  -d '{
    "pipelines": [
      {"pipeline_name": "pipeline_a"},
      {"pipeline_name": "pipeline_b"}
    ]
  }'
```

### Restart a failed run

If a pipeline fails partway through, restart it from the last checkpoint:

```bash
curl -X POST http://localhost:8000/v1/pipelines/<run_id>/restart
```

## 5. Available Sources and Destinations

### Sources

| Type         | Description                     | Key Config Fields                          |
|--------------|---------------------------------|--------------------------------------------|
| `filesystem` | Local or NAS files              | `path`, `format`, `options`                |
| `jdbc`       | SQL databases (Postgres, MySQL) | `url`, `table`, `driver`, `user`, `password` |
| `api`        | REST API endpoints              | `url`, `method`, `headers`                 |
| `sftp`       | SFTP file servers               | `host`, `port`, `username`, `password`, `path` |
| `bigquery`   | Google BigQuery tables          | `table`, `project`                         |
| `custom`     | Custom Python source class      | `class_name`, `module`                     |

### Destinations

| Type       | Description            | Key Config Fields                     |
|------------|------------------------|---------------------------------------|
| `gcs`      | Google Cloud Storage   | `path`, `format`, `mode`              |
| `bigquery` | Google BigQuery        | `table`, `temp_gcs_bucket`, `mode`    |
| `jdbc`     | SQL databases          | `url`, `table`, `driver`, `mode`      |

### Transforms

| Type            | Description                  | Config                          |
|-----------------|------------------------------|---------------------------------|
| `sql`           | SQL query on dataset         | `query` (use `__table__` placeholder) |
| `filter`        | Row filter expression        | `condition`                     |
| `column_rename` | Rename columns               | `mapping` (old → new)          |
| `type_cast`     | Cast column types            | `columns` (col → type)         |
| `drop_columns`  | Remove columns               | `columns` (list)               |
| `add_column`    | Add a computed column        | `name`, `expression`           |

## 6. Deploy to Kubernetes

### Build Docker images

```bash
make build-fastapi
make build-spark
```

Tag and push to your container registry:

```bash
docker tag spark-k8s-ingestion-api <registry>/spark-k8s-ingestion-api:latest
docker tag spark-k8s-ingestion-spark <registry>/spark-k8s-ingestion-spark:latest
docker push <registry>/spark-k8s-ingestion-api:latest
docker push <registry>/spark-k8s-ingestion-spark:latest
```

### Configure Helm values

Edit `helm/spark-ingestion/values.yaml` or create an override file:

```yaml
# custom-values.yaml
fastapi:
  image:
    repository: <registry>/spark-k8s-ingestion-api
    tag: latest
  env:
    INGESTION_DEFAULT_EXECUTION_MODE: k8s
    INGESTION_K8S_MASTER_URL: k8s://https://kubernetes.default.svc
    INGESTION_SPARK_NAMESPACE: spark-jobs

spark:
  image:
    repository: <registry>/spark-k8s-ingestion-spark
    tag: latest
```

### Deploy

```bash
# First install
helm install spark-ingestion helm/spark-ingestion/ -f custom-values.yaml

# Upgrade existing deployment
helm upgrade spark-ingestion helm/spark-ingestion/ -f custom-values.yaml
```

### Verify the deployment

```bash
kubectl get pods -l app=spark-ingestion
kubectl port-forward svc/spark-ingestion 8000:8000
curl http://localhost:8000/v1/health
```

## 7. Running Tests

```bash
# Run all tests
make test

# Run with verbose output
pytest tests/ -v

# Run a specific test file
pytest tests/test_pipelines_routes.py -v
```

## 8. Monitoring

- **Health check**: `GET /v1/health`
- **Prometheus metrics**: `GET /v1/metrics` — scrape this endpoint with your Prometheus instance
- **Run status**: `GET /v1/pipelines/{run_id}/status` — includes per-dataset status and record counts
- **K8S pod monitor**: Automatically polls pod status every 30 seconds and updates run state

## Next Steps

- Browse the [example pipeline](../pipelines/example_pipeline.yaml) for a full two-dataset configuration
- Review [helm/spark-ingestion/values.yaml](../helm/spark-ingestion/values.yaml) for all deployment options
- Check the Swagger UI at `/docs` for the full API schema
