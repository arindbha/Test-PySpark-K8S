"""FastAPI application for Spark K8S ingestion gateway."""

from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

from fastapi_app.config import settings
from fastapi_app.routes import health, ingestion


def create_app() -> FastAPI:
    app = FastAPI(title=settings.app_name, version=settings.app_version, debug=settings.debug)
    app.include_router(health.router)
    app.include_router(ingestion.router)

    Instrumentator().instrument(app).expose(app, endpoint="/metrics")

    return app
