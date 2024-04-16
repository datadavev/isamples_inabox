import pytest
from fastapi.testclient import TestClient
from isb_lib.vocabulary import vocab_adapter
from unittest.mock import MagicMock

from isb_web.main import app
from isb_web.vocabulary import get_repository

ROOT_LABEL = "root_label"

ROOT_URI = "root_uri"


def test_vocab_adapter():
    mock_repository = _mock_repository()
    vocab_dict = vocab_adapter.uijson_vocabulary_dict("", mock_repository)
    assert vocab_dict == {ROOT_URI: {"children": [], "label": {"en": ROOT_LABEL}}}


def _configure_mock_repository(repository: MagicMock):
    root_uri = ROOT_URI
    root_label = ROOT_LABEL
    mock_term = MagicMock()
    mock_term.uri = root_uri
    mock_term.properties.get.return_value = [root_label]
    repository.read.return_value = mock_term
    repository.narrower.return_value = []


def _mock_repository():
    mock_repository = MagicMock()
    _configure_mock_repository(mock_repository)
    return mock_repository


@pytest.fixture(name="client")
def client_fixture():
    def get_repository_override():
        return _mock_repository()

    app.dependency_overrides[get_repository] = get_repository_override
    client = TestClient(app)
    yield client
    app.dependency_overrides.clear()


def test_vocabulary_fast_api_material_sample_type(client: TestClient):
    response = client.get("/vocabulary/material_sample_type")
    assert response.status_code == 200


def test_vocabulary_fast_api_material_type(client: TestClient):
    response = client.get("/vocabulary/material_type")
    assert response.status_code == 200


def test_vocabulary_fast_api_sampled_feature_type(client: TestClient):
    response = client.get("/vocabulary/sampled_feature_type")
    assert response.status_code == 200
