import datetime
from unittest.mock import patch, MagicMock

import pytest
from starlette.middleware import Middleware
from starlette.testclient import TestClient

from isb_lib.models.export_job import ExportJob
import isb_web.export
from isb_web.auth import AuthenticateMiddleware
from isb_web.main import app


@pytest.fixture(name="client")
def client_fixture():
    client = TestClient(app)
    yield client


@pytest.fixture(autouse=True)
def setup():
    export_app = isb_web.export.export_app
    # Overridden to disable authentication during unit testing.
    new_middlewares: list[Middleware] = []
    for middleware in app.user_middleware:
        if not middleware.cls.__name__ == AuthenticateMiddleware:
            new_middlewares.append(middleware)
    export_app.user_middleware = new_middlewares
    export_app.middleware_stack = export_app.build_middleware_stack()


@patch("isb_web.export.search_solr_and_export_results")
@patch("isb_web.sqlmodel_database.save_or_update_export_job")
def test_export_create(mock_solr_query: MagicMock, mock_database: MagicMock, client: TestClient):
    response = client.get("/export/create")
    assert mock_solr_query.called
    assert mock_database.called
    assert response.status_code == 201


def test_export_create_sort_unsupported(client: TestClient):
    response = client.get("/export/create?sort=foobar")
    assert response.status_code == 415


@patch("isb_web.sqlmodel_database.export_job_with_uuid")
def test_export_status(mock_database: MagicMock, client: TestClient):
    mock_database.return_value = ExportJob()
    response = client.get("/export/status?uuid=123456")
    assert response.status_code == 201


@patch("isb_web.sqlmodel_database.export_job_with_uuid")
def test_export_status_completed(mock_database: MagicMock, client: TestClient):
    job = ExportJob()
    job.tcompleted = datetime.datetime.now()
    mock_database.return_value = job
    response = client.get("/export/status?uuid=123456")
    assert response.status_code == 200


@patch("isb_web.sqlmodel_database.export_job_with_uuid")
def test_export_statu_not_found(mock_database: MagicMock, client: TestClient):
    mock_database.return_value = None
    response = client.get("/export/status?uuid=123456")
    assert response.status_code == 404
