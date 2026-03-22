# Deployment Guide: PostgreSQL to Local Filesystem

End-to-end guide for deploying a pipeline that reads from PostgreSQL and writes
to a local filesystem. Covers local development, Docker Compose, and Kubernetes
deployment.

---

## Prerequisites

| Tool       | Version | Purpose                          |
|------------|---------|----------------------------------|
| Python     | 3.11+   | FastAPI gateway & Spark jobs     |
| Java       | 17+     | Apache Spark runtime             |
| Docker     | 24+     | Container builds                 |
| Helm       | 3.12+   | Kubernetes deployment (optional) |
| kubectl    | 1.27+   | Cluster access (optional)        |
| PostgreSQL | 13+     | Source database                   |

---

## 1. Prepare the PostgreSQL Source

Create a sample database and table for testing.

```bash
# Start a local PostgreSQL instance (or use an existing one)
docker run -d \
  --name postgres-source \
  -e POSTGRES_USER=ingest_user \
  -e POSTGRES_PASSWORD=ingest_pass \
  -e POSTGRES_DB=sales_db \
  -p 5432:5432 \
  postgres:16

# Wait for PostgreSQL to be ready
sleep 3

# Seed sample data
docker exec -i postgres-source psql -U ingest_user -d sales_db <<'SQL'
CREATE TABLE IF NOT EXISTS customers (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    email       VARCHAR(200),
    city        VARCHAR(100),
    created_at  TIMESTAMP DEFAULT NOW()
);

INSERT INTO customers (name, email, city) VALUES
    ('Alice Johnson',  'alice@example.com',  'Seattle'),
    ('Bob Smith',      'bob@example.com',    'Portland'),
    ('Carol Davis',    'carol@example.com',  'San Francisco'),
    ('Dan Wilson',     'dan@example.com',    'Austin'),
    ('Eve Martinez',   'eve@example.com',    'Denver');

CREATE TABLE IF NOT EXISTS orders (
    id           SERIAL PRIMARY KEY,
    customer_id  INTEGER REFERENCES customers(id),
    amount       NUMERIC(10,2) NOT NULL,
    status       VARCHAR(20) DEFAULT 'pending',
    ordered_at   TIMESTAMP DEFAULT NOW()
);

INSERT INTO orders (customer_id, amount, status) VALUES
    (1, 250.00, 'completed'),
    (2,  75.50, 'completed'),
    (3, 180.00, 'pending'),
    (1, 320.00, 'shipped'),
    (4,  45.99, 'completed'),
    (5, 510.00, 'pending');
SQL
```

---

## 2. Define the Pipeline

Create a pipeline YAML that reads both tables from PostgreSQL and writes them as
Parquet files on the local filesystem.

Connection details are defined once in the top-level `connections` map and
referenced by name in each dataset — no need to repeat url, driver, and
credentials for every table.

```bash
cat > pipelines/postgres_to_local.yaml << 'EOF'
pipeline_name: postgres_to_local
execution_mode: local

# Define the connection once — all datasets reference it by name.
connections:
  sales_db:
    type: jdbc
    config:
      url: "jdbc:postgresql://localhost:5432/sales_db"
      driver: org.postgresql.Driver
      user: ingest_user
      password: ingest_pass

datasets:
  - source:
      name: customers
      connection: sales_db
      config:
        table: customers
    destination:
      type: gcs
      config:
        path: /data/output/customers
        format: parquet
        mode: overwrite
    transforms:
      - type: filter
        config:
          condition: "city IS NOT NULL"
      - type: add_column
        config:
          name: ingested_at
          expression: "current_timestamp()"

  - source:
      name: orders
      connection: sales_db
      config:
        table: orders
    destination:
      type: gcs
      config:
        path: /data/output/orders
        format: parquet
        mode: overwrite
        partition_by:
          - status
    transforms:
      - type: sql
        config:
          query: >
            SELECT o.*, c.name AS customer_name
            FROM __table__ o
            LEFT JOIN customers c ON o.customer_id = c.id
      - type: drop_columns
        config:
          columns:
            - customer_id

spark_config:
  driver_memory: 1g
  executor_memory: 1g
  executor_instances: 1
  extra_conf:
    spark.jars.packages: "org.postgresql:postgresql:42.7.3"
EOF
```

> **Note:** The `gcs` destination type writes to any path Spark can access —
> local filesystem, GCS, S3, or HDFS. For local paths, use an absolute path
> like `/data/output/`.

> **Connections are optional.** You can still inline `type` and full `config`
> directly on each source — existing pipelines continue to work unchanged.

---

## 3. Deploy Locally

### 3a. Install and Start the API

```bash
# Install project with dev dependencies
pip install -e ".[dev]"

# Download the PostgreSQL JDBC driver for Spark
SPARK_HOME="${SPARK_HOME:-/opt/spark}"
curl -fsSL -o "${SPARK_HOME}/jars/postgresql-42.7.3.jar" \
  "https://jdbc.postgresql.org/download/postgresql-42.7.3.jar"

# Create output directory
mkdir -p /data/output

# Start the FastAPI gateway
make run-local
```

The API is now running at `http://localhost:8000`.

### 3b. Run the Pipeline

```bash
# Verify the pipeline is discovered
curl -s http://localhost:8000/v1/pipelines | python3 -m json.tool

# Trigger the pipeline
curl -s -X POST http://localhost:8000/v1/pipelines/run \
  -H "Content-Type: application/json" \
  -d '{"pipeline_name": "postgres_to_local"}' | python3 -m json.tool
```

Sample response:

```json
{
  "run_id": "run_a1b2c3d4",
  "status": "queued",
  "message": "Pipeline postgres_to_local queued"
}
```

### 3c. Monitor Progress

```bash
# Check run status (replace with your run_id)
curl -s http://localhost:8000/v1/pipelines/run_a1b2c3d4/status | python3 -m json.tool

# List recent runs
curl -s "http://localhost:8000/v1/runs/?pipeline_name=postgres_to_local&limit=5" \
  | python3 -m json.tool
```

### 3d. Verify Output

```bash
# List generated Parquet files
ls -la /data/output/customers/
ls -la /data/output/orders/

# Inspect with Python
python3 -c "
import pandas as pd
print('=== Customers ===')
print(pd.read_parquet('/data/output/customers').to_string())
print()
print('=== Orders ===')
print(pd.read_parquet('/data/output/orders').to_string())
"
```

---

## 4. Deploy with Docker Compose

For a self-contained local deployment with PostgreSQL and the API in containers.

### 4a. Create docker-compose.yaml

```bash
cat > docker-compose.yaml << 'EOF'
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: ingest_user
      POSTGRES_PASSWORD: ingest_pass
      POSTGRES_DB: sales_db
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ingest_user -d sales_db"]
      interval: 5s
      retries: 5

  api:
    build:
      context: .
      dockerfile: docker/fastapi/Dockerfile
    ports:
      - "8000:8000"
    environment:
      INGESTION_DEFAULT_EXECUTION_MODE: local
      INGESTION_PIPELINES_DIR: /app/pipelines
      INGESTION_METADATA_DIR: /app/data
    volumes:
      - ./pipelines:/app/pipelines
      - output-data:/data/output
    depends_on:
      postgres:
        condition: service_healthy

volumes:
  pgdata:
  output-data:
EOF
```

### 4b. Update Pipeline JDBC URL for Docker Networking

When running inside Docker Compose, the PostgreSQL hostname is the service name
`postgres`, not `localhost`. Create an override pipeline or use runtime
overrides:

```bash
# Build and start
docker compose up -d --build

# Seed the database
docker exec -i $(docker compose ps -q postgres) \
  psql -U ingest_user -d sales_db <<'SQL'
CREATE TABLE IF NOT EXISTS customers (
    id SERIAL PRIMARY KEY, name VARCHAR(100), email VARCHAR(200),
    city VARCHAR(100), created_at TIMESTAMP DEFAULT NOW()
);
INSERT INTO customers (name, email, city) VALUES
    ('Alice Johnson','alice@example.com','Seattle'),
    ('Bob Smith','bob@example.com','Portland'),
    ('Carol Davis','carol@example.com','San Francisco');

CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY, customer_id INTEGER REFERENCES customers(id),
    amount NUMERIC(10,2), status VARCHAR(20) DEFAULT 'pending',
    ordered_at TIMESTAMP DEFAULT NOW()
);
INSERT INTO orders (customer_id, amount, status) VALUES
    (1, 250.00, 'completed'), (2, 75.50, 'pending'), (3, 180.00, 'shipped');
SQL

# Override the connection URL for Docker networking
curl -s -X POST http://localhost:8000/v1/pipelines/run \
  -H "Content-Type: application/json" \
  -d '{
    "pipeline_name": "postgres_to_local",
    "override_params": {
      "connections": {
        "sales_db": {
          "config": {
            "url": "jdbc:postgresql://postgres:5432/sales_db"
          }
        }
      }
    }
  }' | python3 -m json.tool
```

### 4c. View Output

```bash
# Copy output from the container volume
docker compose exec api ls -R /data/output/

# Tear down
docker compose down
```

---

## 5. Deploy on Kubernetes

### 5a. Build and Push Docker Images

```bash
export REGISTRY=your-registry.example.com  # e.g. ghcr.io/username

# Build images
docker build -t ${REGISTRY}/spark-ingestion-api:0.1.0 -f docker/fastapi/Dockerfile .
docker build -t ${REGISTRY}/spark-ingestion-spark:0.1.0 -f docker/spark/Dockerfile .

# Push
docker push ${REGISTRY}/spark-ingestion-api:0.1.0
docker push ${REGISTRY}/spark-ingestion-spark:0.1.0
```

### 5b. Deploy PostgreSQL on the Cluster

```bash
# Simple PostgreSQL StatefulSet for demo purposes
kubectl create namespace ingestion

kubectl -n ingestion create secret generic postgres-creds \
  --from-literal=POSTGRES_USER=ingest_user \
  --from-literal=POSTGRES_PASSWORD=ingest_pass

helm repo add bitnami https://charts.bitnami.com/bitnami
helm install postgres bitnami/postgresql \
  -n ingestion \
  --set auth.username=ingest_user \
  --set auth.password=ingest_pass \
  --set auth.database=sales_db \
  --set primary.persistence.size=1Gi
```

PostgreSQL will be reachable at `postgres-postgresql.ingestion.svc.cluster.local:5432`.

### 5c. Create a PersistentVolumeClaim for Output

```bash
kubectl -n ingestion apply -f - <<'EOF'
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: pipeline-output
spec:
  accessModes: [ReadWriteMany]
  resources:
    requests:
      storage: 5Gi
EOF
```

### 5d. Create the Pipeline ConfigMap

```bash
cat > pipelines/postgres_to_local_k8s.yaml << 'EOF'
pipeline_name: postgres_to_local_k8s
execution_mode: k8s

connections:
  sales_db:
    type: jdbc
    config:
      url: "jdbc:postgresql://postgres-postgresql.ingestion.svc.cluster.local:5432/sales_db"
      driver: org.postgresql.Driver
      user: ingest_user
      password: ingest_pass

datasets:
  - source:
      name: customers
      connection: sales_db
      config:
        table: customers
    destination:
      type: gcs
      config:
        path: /data/output/customers
        format: parquet
        mode: overwrite
    transforms:
      - type: add_column
        config:
          name: ingested_at
          expression: "current_timestamp()"

  - source:
      name: orders
      connection: sales_db
      config:
        table: orders
    destination:
      type: gcs
      config:
        path: /data/output/orders
        format: parquet
        mode: overwrite
        partition_by:
          - status

spark_config:
  driver_memory: 1g
  executor_memory: 1g
  executor_instances: 2
  extra_conf:
    spark.jars.packages: "org.postgresql:postgresql:42.7.3"
    spark.kubernetes.driver.volumes.persistentVolumeClaim.output.mount.path: /data/output
    spark.kubernetes.driver.volumes.persistentVolumeClaim.output.options.claimName: pipeline-output
    spark.kubernetes.executor.volumes.persistentVolumeClaim.output.mount.path: /data/output
    spark.kubernetes.executor.volumes.persistentVolumeClaim.output.options.claimName: pipeline-output
EOF
```

### 5e. Install the Helm Chart

```bash
helm install spark-ingestion helm/spark-ingestion \
  -n ingestion \
  --set image.repository=${REGISTRY}/spark-ingestion-api \
  --set image.tag=0.1.0 \
  --set sparkImage.repository=${REGISTRY}/spark-ingestion-spark \
  --set sparkImage.tag=0.1.0 \
  --set config.defaultExecutionMode=k8s \
  --set spark.namespace=ingestion \
  --set replicaCount=1
```

### 5f. Run the Pipeline

```bash
# Port-forward to the API
kubectl -n ingestion port-forward svc/spark-ingestion 8000:8000 &

# Trigger the pipeline
curl -s -X POST http://localhost:8000/v1/pipelines/run \
  -H "Content-Type: application/json" \
  -d '{"pipeline_name": "postgres_to_local_k8s"}' | python3 -m json.tool

# Monitor Spark driver pod
kubectl -n ingestion get pods -l spark-role=driver --watch
```

---

## 6. Using Runtime Overrides

You don't need separate YAML files for different environments. Override the
connection at runtime — every dataset using that connection picks up the change
automatically:

```bash
curl -s -X POST http://localhost:8000/v1/pipelines/run \
  -H "Content-Type: application/json" \
  -d '{
    "pipeline_name": "postgres_to_local",
    "override_params": {
      "connections": {
        "sales_db": {
          "config": {
            "url": "jdbc:postgresql://prod-db:5432/sales_db",
            "user": "prod_reader",
            "password": "prod_secret"
          }
        }
      }
    }
  }'
```

---

## 7. Output Formats

The `gcs` destination supports any Spark-compatible format. Change the
`format` field in the destination config:

| Format    | Config Value | Notes                        |
|-----------|-------------|------------------------------|
| Parquet   | `parquet`   | Default, columnar, compressed |
| CSV       | `csv`       | Add `header: "true"` in options |
| JSON      | `json`      | One JSON object per line      |
| ORC       | `orc`       | Alternative columnar format   |
| Avro      | `avro`      | Requires `spark-avro` package |

Example CSV output:

```yaml
destination:
  type: gcs
  config:
    path: /data/output/customers_csv
    format: csv
    mode: overwrite
    options:
      header: "true"
      delimiter: ","
```

---

## 8. Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `ClassNotFoundException: org.postgresql.Driver` | Missing JDBC driver | Add `spark.jars.packages: org.postgresql:postgresql:42.7.3` to `spark_config.extra_conf`, or download the JAR to `$SPARK_HOME/jars/` |
| `Connection refused` to PostgreSQL | Wrong host/port | Use `localhost` for local, Docker service name for Compose, K8s FQDN for cluster |
| `Permission denied` writing to `/data/output` | Directory not writable | Run `mkdir -p /data/output && chmod 777 /data/output`, or use a writable PVC in K8s |
| Pipeline stuck in `queued` | Queue worker not started | Verify the API started cleanly — check logs with `make run-local` or `kubectl logs` |
| `FATAL: password authentication failed` | Wrong credentials | Verify `user`/`password` in the pipeline YAML match the database |

---

## 9. Cleanup

```bash
# Local
docker rm -f postgres-source

# Docker Compose
docker compose down -v

# Kubernetes
helm uninstall spark-ingestion -n ingestion
helm uninstall postgres -n ingestion
kubectl -n ingestion delete pvc pipeline-output
kubectl delete namespace ingestion
```
