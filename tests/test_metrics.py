"""Tests for Prometheus metrics endpoint."""

import pytest
from httpx import ASGITransport, AsyncClient

from fastapi_app import create_app


@pytest.fixture
def app():
    return create_app()


@pytest.fixture
async def client(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


async def test_metrics_endpoint_exists(client: AsyncClient):
    resp = await client.get("/metrics")
    assert resp.status_code == 200
    assert "http_request" in resp.text or "HELP" in resp.text
