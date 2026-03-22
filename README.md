# Spark on Kubernetes Ingestion Framework

A production-ready data ingestion platform that orchestrates Apache Spark jobs via a FastAPI REST gateway. It supports pluggable execution backends (Kubernetes, Google Dataproc, local), YAML-driven pipeline definitions, and a priority-based job queue with concurrency control.

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌────────────────────┐
│  REST API   │────▶│  Job Queue   │────▶│  Execution Engine  │
│  (FastAPI)  │     │  (priority)  │     │  K8S / Dataproc /  │
│  port 8000  │     │              │     │  Local             │
└─────────────┘     └──────────────┘     └────────────────────┘
       │                                          │
       ▼                                          ▼
┌─────────────┐                          ┌────────────────────┐
│  Metadata   │                          │  Spark Driver Pod  │
│  Store      │◀─────────────────────────│  (runner.py)       │
└─────────────┘                          └────────────────────┘
```

**Three-tier design:**

1. **API Tier** — FastAPI application handling requests, job management, and status tracking
2. **Job Queue** — In-memory async priority queue with configurable concurrency limits
3. **Execution Engines** — Pluggable backends that submit Spark jobs to Kubernetes, Dataproc, or a local process

## Features

- **6 data sources** — Filesystem, JDBC, REST API, SFTP, BigQuery, Custom Python
- **3 destinations** — Google Cloud Storage, BigQuery, JDBC
- **6 transform types** — SQL, Filter, Column Rename, Type Cast, Drop Columns, Add Column
- **YAML-driven pipelines** — declarative pipeline definitions with runtime overrides
- **Batch execution** — run multiple pipelines in a single API call
- **Restart from checkpoint** — resume failed pipelines from the last successful dataset
- **Prometheus metrics** — built-in `/v1/metrics` endpoint
- **Kubernetes monitoring** — background pod status polling with automatic state reconciliation

## Project Structure

```
├── fastapi_app/           # REST API gateway
│   ├── main.py            # App entry point with lifespan hooks
│   ├── config.py          # Pydantic settings (INGESTION_* env vars)
│   ├── models/            # Request/response models, enums
│   ├── routes/            # Endpoint handlers (pipelines, runs, health)
│   └── services/          # Execution engines, job queue, metadata, monitors
├── spark_jobs/            # Spark job framework
│   ├── runner.py          # Spark driver entrypoint
│   ├── sources/           # Data source implementations
│   ├── destinations/      # Data destination implementations
│   ├── transforms/        # Transform implementations
│   └── utils/             # Spark session factory, metadata reporter
├── docker/                # Dockerfiles for FastAPI and Spark containers
├── helm/                  # Kubernetes Helm chart
├── pipelines/             # YAML pipeline definitions
├── tests/                 # Pytest test suite
├── .github/workflows/     # CI/CD pipelines
├── Makefile               # Development tasks
└── pyproject.toml         # Project metadata and dependencies
```

## Prerequisites

- Python 3.11+
- Java 17 JRE (for Spark)
- Docker (for container builds)
- Kubernetes cluster + Helm 3 (for K8S deployment)
- Google Cloud SDK (optional, for GCP-based sources/destinations)

## Quick Start

See [docs/quickstart.md](docs/quickstart.md) for a step-by-step guide to get up and running.

**TL;DR for local development:**

```bash
pip install -e ".[dev]"
make run-local          # starts FastAPI on http://localhost:8000
```

## API Endpoints

| Method   | Path                          | Description                  |
|----------|-------------------------------|------------------------------|
| `GET`    | `/v1/health`                  | Health check                 |
| `GET`    | `/v1/metrics`                 | Prometheus metrics           |
| `GET`    | `/v1/pipelines`               | List available pipelines     |
| `POST`   | `/v1/pipelines/run`           | Run a single pipeline        |
| `POST`   | `/v1/pipelines/run-batch`     | Run multiple pipelines       |
| `GET`    | `/v1/pipelines/{run_id}/status` | Get run status             |
| `POST`   | `/v1/pipelines/{run_id}/restart` | Restart a failed run      |
| `GET`    | `/v1/runs/`                   | List runs (with filters)     |
| `DELETE` | `/v1/runs/{run_id}/cancel`    | Cancel a running job         |

## Configuration

The application is configured via environment variables prefixed with `INGESTION_`:

| Variable                        | Default       | Description                         |
|---------------------------------|---------------|-------------------------------------|
| `INGESTION_DEFAULT_EXECUTION_MODE` | `local`    | Execution backend: `k8s`, `dataproc`, `local` |
| `INGESTION_MAX_CONCURRENT_JOBS` | `10`          | Max parallel Spark jobs             |
| `INGESTION_K8S_MASTER_URL`      | —             | Kubernetes API server URL           |
| `INGESTION_SPARK_NAMESPACE`     | `default`     | K8S namespace for Spark pods        |
| `INGESTION_METADATA_STORE_TYPE` | `filesystem`  | Metadata backend: `filesystem`, `bigquery` |
| `INGESTION_GCP_CREDENTIALS_PATH`| —             | Path to GCP service account JSON    |
| `INGESTION_BQ_PROJECT_ID`       | —             | BigQuery project ID                 |

## Development

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Run tests
make test

# Run linter
make lint

# Start dev server
make run-local

# Build Docker images
make build-fastapi
make build-spark
```

## Deployment

```bash
# Build and push Docker images
make build-fastapi
make build-spark

# Deploy to Kubernetes with Helm
make helm-install

# Upgrade an existing deployment
make helm-upgrade
```

See [helm/spark-ingestion/values.yaml](helm/spark-ingestion/values.yaml) for all configurable Helm values.

## License

See [LICENSE](LICENSE) for details.
