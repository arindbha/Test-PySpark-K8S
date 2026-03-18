"""FastAPI application with lifespan for Spark K8S ingestion gateway."""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

from fastapi_app.config import settings
from fastapi_app.routes import health, pipelines, runs
from fastapi_app.services.bigquery_metadata import get_metadata_store
from fastapi_app.services.job_queue import JobQueue
from fastapi_app.services.k8s_monitor import K8sMonitor
from fastapi_app.services.pipeline_loader import PipelineLoader


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    metadata_store = get_metadata_store(settings)
    await metadata_store.initialize()

    pipeline_loader = PipelineLoader(settings.pipelines_dir)
    job_queue = JobQueue(metadata_store, settings)
    await job_queue.start()

    k8s_monitor = K8sMonitor(metadata_store, settings)
    await k8s_monitor.start()

    app.state.metadata_store = metadata_store
    app.state.pipeline_loader = pipeline_loader
    app.state.job_queue = job_queue
    app.state.k8s_monitor = k8s_monitor
    app.state.settings = settings

    yield

    # Shutdown
    await k8s_monitor.stop()
    await job_queue.stop()


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    debug=settings.debug,
    lifespan=lifespan,
)

app.include_router(health.router)
app.include_router(pipelines.router)
app.include_router(runs.router)

Instrumentator().instrument(app).expose(app, endpoint="/v1/metrics")
