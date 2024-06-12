import json
import os
from unittest.mock import MagicMock

import pytest

from isamples_metadata.metadata_constants import METADATA_COMPLIES_WITH
from isb_lib.localcontexts.localcontexts_client import LocalContextsClient, local_contexts_info_for_resolved_content


@pytest.fixture
def project_detail_json() -> dict:
    with open("./test_data/localcontexts/project_detail.json") as schema:
        return json.load(schema)


@pytest.fixture
def mock_local_contexts_request(project_detail_json: dict):
    mock_request = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = project_detail_json
    mock_request.get.return_value = mock_response
    return mock_request


def test_localcontexts_project_id_for_complies_with_id():
    client = LocalContextsClient()
    project_id = client.localcontexts_project_id_for_complies_with_id("localcontexts:projects/71b32571-0176-4627-8e01-4d78818432a7")
    assert "71b32571-0176-4627-8e01-4d78818432a7" == project_id


def test_localcontext_project_id_none():
    client = LocalContextsClient()
    project_id = client.localcontexts_project_id_for_complies_with_id("foobar")
    assert project_id is None


def test_fetch_project_detail(mock_local_contexts_request: MagicMock):
    client = LocalContextsClient()
    project_info = client.project_info("123456", mock_local_contexts_request)
    assert project_info is not None
    notices = project_info.notices
    assert "https://storage.googleapis.com/local-contexts-hub.appspot.com/labels/notices/ci-attribution-incomplete.png" == notices[0].img_url
    text = "Collections and items in our institution have incomplete, inaccurate, and/or missing attribution. We are using this notice to clearly identify this material so that it can be updated, or corrected by communities of origin. Our institution is committed to collaboration and partnerships to address this problem of incorrect or missing attribution."
    assert text == notices[0].text
    assert "Moorea Biocode 1.0" == project_info.title
    assert "https://localcontextshub.org/projects/71b32571-0176-4627-8e01-4d78818432a7" == project_info.project_page


def test_local_contexts_info_for_resolved_content(mock_local_contexts_request):
    resolved_content = {METADATA_COMPLIES_WITH: ["localcontexts:projects/123456"]}
    project_info = local_contexts_info_for_resolved_content(resolved_content, mock_local_contexts_request)
    assert project_info is not None


def test_local_contexts_info_for_resolved_content_empty(mock_local_contexts_request):
    resolved_content = {}
    project_info = local_contexts_info_for_resolved_content(resolved_content, mock_local_contexts_request)
    assert project_info is None


@pytest.mark.skipif(
    os.environ.get("CI") is not None, reason="Don't run live requests in CI"
)
def test_fetch_project_detail_live_request():
    client = LocalContextsClient()
    project_info = client.project_info("71b32571-0176-4627-8e01-4d78818432a7")
    assert project_info is not None
