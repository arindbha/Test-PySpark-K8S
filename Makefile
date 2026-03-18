.PHONY: test lint run-local build-fastapi build-spark helm-install helm-upgrade

test:
	pytest tests/ -v

lint:
	ruff check . && ruff format --check .

run-local:
	uvicorn fastapi_app.main:app --reload --port 8000

build-fastapi:
	docker build -f docker/fastapi/Dockerfile -t spark-k8s-ingestion-api .

build-spark:
	docker build -f docker/spark/Dockerfile -t spark-k8s-ingestion-spark .

helm-install:
	helm install spark-ingestion helm/spark-ingestion/

helm-upgrade:
	helm upgrade spark-ingestion helm/spark-ingestion/
