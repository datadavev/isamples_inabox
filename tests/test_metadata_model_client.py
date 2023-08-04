from unittest.mock import patch, MagicMock

from isamples_metadata.taxonomy.metadata_model_client import MODEL_SERVER_CLIENT, PredictionResult


@patch("isamples_metadata.taxonomy.metadata_model_client.requests.session")
def test_sesar_material_model_client(mock_request):
    expected_confidence, expected_value = _construct_mock_response(mock_request)
    result = MODEL_SERVER_CLIENT.make_sesar_material_request({}, mock_request)
    _assert_on_result(expected_confidence, expected_value, result)


@patch("isamples_metadata.taxonomy.metadata_model_client.requests.session")
def test_opencontext_material_client(mock_request):
    expected_confidence, expected_value = _construct_mock_response(mock_request)
    result = MODEL_SERVER_CLIENT.make_opencontext_material_request({}, mock_request)
    _assert_on_result(expected_confidence, expected_value, result)


@patch("isamples_metadata.taxonomy.metadata_model_client.requests.session")
def test_opencontext_sample_client(mock_request):
    expected_confidence, expected_value = _construct_mock_response(mock_request)
    result = MODEL_SERVER_CLIENT.make_opencontext_sample_request({}, mock_request)
    _assert_on_result(expected_confidence, expected_value, result)


@patch("isamples_metadata.taxonomy.metadata_model_client.requests.session")
def test_smithsonian_sampled_feature_client(mock_request):
    # Smithsonian is a different response format since the underlying model is differnet
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = ["hi"]
    mock_request.post.return_value = mock_response
    result = MODEL_SERVER_CLIENT.make_smithsonian_sampled_feature_request([], mock_request)
    assert result is not None


def _assert_on_result(expected_confidence, expected_value, result):
    assert result is not None
    prediction_result = result[0]
    assert type(result[0]) is PredictionResult
    assert prediction_result.value == expected_value
    assert prediction_result.confidence == expected_confidence


def _construct_mock_response(mock_request):
    mock_response = MagicMock()
    mock_response.status_code = 200
    expected_value = "foo"
    expected_confidence = 0.78
    mock_response.json.return_value = [{
        "value": expected_value,
        "confidence": expected_confidence
    }]
    mock_request.post.return_value = mock_response
    return expected_confidence, expected_value
