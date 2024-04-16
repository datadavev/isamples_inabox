import functools
import json
from typing import Any

import requests

from isamples_metadata.metadata_exceptions import MetadataException
from isb_web import config

cache_size = config.Settings().modelserver_lru_cache_size


class PredictionResult:
    """Class that represents the prediction result"""
    def __init__(self, value: str, confidence: float):
        """
            Initialize the class values with the predicted label and probability logit value
            :param value the predicted label
            :param confidence the probability of the prediction
        """
        self.value = value
        self.confidence = confidence


class ModelServerClient:
    base_url: str
    base_headers: dict

    def __init__(self, base_url: str, base_headers: dict = {}):
        self.base_url = base_url
        self.base_headers = base_headers

    @functools.lru_cache(maxsize=config.Settings().modelserver_lru_cache_size)
    def _make_json_request_bytes(self, url: str, data_params_bytes: bytes, rsession: requests.Session) -> Any:
        res = rsession.post(url, headers=self.base_headers, data=data_params_bytes)
        if res.status_code == 200:
            response_dict = res.json()
            return response_dict
        elif res.status_code == 409:
            # serialized exception we need to re-raise
            response_text = res.text
            raise MetadataException(response_text)
        else:
            raise Exception(f"Exception calling model server: {res.text}")

    def _make_json_request(self, url: str, data_params: dict, rsession: requests.Session) -> Any:
        data_params_bytes: bytes = json.dumps(data_params).encode("utf-8")
        return self._make_json_request_bytes(url, data_params_bytes, rsession)

    @staticmethod
    def _convert_to_prediction_result_list(result: Any) -> list[PredictionResult]:
        return [PredictionResult(prediction_dict["value"], prediction_dict["confidence"]) for prediction_dict in result]

    def _make_opencontext_request(self, source_record: dict, model_type: str, rsession: requests.Session = requests.Session()) -> list[PredictionResult]:
        params: dict = {"source_record": source_record, "type": model_type}
        url = f"{self.base_url}opencontext"
        return ModelServerClient._convert_to_prediction_result_list(self._make_json_request(url, params, rsession))

    def make_opencontext_material_request(self, source_record: dict, rsession: requests.Session = requests.Session()) -> list[PredictionResult]:
        return self._make_opencontext_request(source_record, "material", rsession)

    def make_opencontext_sample_request(self, source_record: dict, rsession: requests.Session = requests.Session()) -> list[PredictionResult]:
        return self._make_opencontext_request(source_record, "sample", rsession)

    def make_sesar_material_request(self, source_record: dict, rsession: requests.Session = requests.Session()) -> list[PredictionResult]:
        params: dict = {"source_record": source_record, "type": "material"}
        url = f"{self.base_url}sesar"
        return ModelServerClient._convert_to_prediction_result_list(self._make_json_request(url, params, rsession))

    def make_smithsonian_sampled_feature_request(self, input_strs: list[str], rsession: requests.Session = requests.Session()) -> str:
        params: dict = {"input": input_strs, "type": "context"}
        url = f"{self.base_url}smithsonian"
        return self._make_json_request(url, params, rsession)


headers = {"accept": "application/json", "User-Agent": "iSamples Integration Bot 2000"}
MODEL_SERVER_CLIENT = ModelServerClient(config.Settings().modelserver_url, headers)
