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

    # Note that the models are frozen in place and would need to be re-implemented and re-trained to use the new
    # vocbaulary URIs.  This is a heavyweight and time-consuming process.  Rather than taking that approach, simply
    # swap out the old model labels for the appropriate URIs at the call site.
    MATERIAL_CATEGORY_DICT = {
        "natural solid material": "https://w3id.org/isample/vocabulary/material/1.0/earthmaterial",
        "organic material": "https://w3id.org/isample/vocabulary/material/1.0/organicmaterial",
        "rock": "https://w3id.org/isample/vocabulary/material/1.0/rock",
        "sediment": "https://w3id.org/isample/vocabulary/material/1.0/sediment",
        "mixed soil": "https://w3id.org/isample/vocabulary/material/1.0/mixedsoilsedimentrock",
        "biogenicnonorganicmaterial": "https://w3id.org/isample/vocabulary/material/1.0/biogenicnonorganicmaterial",
        "material": "https://w3id.org/isample/vocabulary/material/1.0/material",
        "mineral": "https://w3id.org/isample/vocabulary/material/1.0/mineral",
        "biogenic non-organic material": "https://w3id.org/isample/vocabulary/material/1.0/biogenicnonorganicmaterial",
        "mat:rock": "https://w3id.org/isample/vocabulary/material/1.0/rock",
        "mat:biogenicnonorganicmaterial": "https://w3id.org/isample/vocabulary/material/1.0/biogenicnonorganicmaterial",
        "mat:anthropogenicmetal": "https://w3id.org/isample/vocabulary/material/1.0/anthropogenicmetal",
        "ocmat:ceramicclay": "https://w3id.org/isample/opencontext/material/0.1/ceramicclay",
        "not provided": "https://w3id.org/isample/vocabulary/material/1.0/material",
        "soil": "https://w3id.org/isample/vocabulary/material/1.0/soil",
        "organicmaterial": "https://w3id.org/isample/vocabulary/material/1.0/organicmaterial",
        "liquid water": "https://w3id.org/isample/vocabulary/material/1.0/liquidwater",
        "anyanthropogenicmaterial": "https://w3id.org/isample/vocabulary/material/1.0/anyanthropogenicmaterial",
        "anthropogenic metal": "https://w3id.org/isample/vocabulary/material/1.0/anthropogenicmetal",
        "gaseous material": "https://w3id.org/isample/vocabulary/material/1.0/gas",
        "anthropogenic material": "https://w3id.org/isample/vocabulary/material/1.0/anyanthropogenicmaterial",
        "anthropogenicmetal": "https://w3id.org/isample/vocabulary/material/1.0/anthropogenicmetal",
        "ocmat:organicanimalproduct": "https://w3id.org/isample/opencontext/material/0.1/organicanimalproduct",
        "biogenic non organic material": "https://w3id.org/isample/vocabulary/material/1.0/biogenicnonorganicmaterial",
        "particulate": "https://w3id.org/isample/vocabulary/material/1.0/particulate",
        "non-aqueous liquid material": "https://w3id.org/isample/vocabulary/material/1.0/nonaqueousliquid",
        "ice": "https://w3id.org/isample/vocabulary/material/1.0/anyice",
        "ocmat:plantmaterial": "https://w3id.org/isample/opencontext/material/0.1/plantmaterial"
    }

    MATERIAL_SAMPLE_DICT = {
        "other solid object": "https://w3id.org/isample/vocabulary/materialsampleobjecttype/1.0/othersolidobject",
        "container": "https://w3id.org/isample/opencontext/materialsampleobjecttype/0.1/containerobject",
        "ornament": "https://w3id.org/isample/opencontext/materialsampleobjecttype/0.1/ornament",
        "architectural element": "https://w3id.org/isample/opencontext/materialsampleobjecttype/0.1/architecturalelement",
        "organism part": "https://w3id.org/isample/vocabulary/materialsampleobjecttype/1.0/organismpart",
        "whole organism": "https://w3id.org/isample/vocabulary/materialsampleobjecttype/1.0/wholeorganism",
        "physicalspecimen": "https://w3id.org/isample/vocabulary/materialsampleobjecttype/1.0/materialsample",
        "artifact": "https://w3id.org/isample/vocabulary/materialsampleobjecttype/1.0/artifact",
        "aggregation": "https://w3id.org/isample/vocabulary/materialsampleobjecttype/1.0/genericaggregation",
        "not provided": "https://w3id.org/isample/vocabulary/materialsampleobjecttype/1.0/materialsample",
        "biologicalspecimen": "https://w3id.org/isample/vocabulary/materialsampleobjecttype/1.0/biologicalmaterialsample",
        "analytical preparation": "https://w3id.org/isample/vocabulary/materialsampleobjecttype/1.0/analyticalpreparation",
        "tile": "https://w3id.org/isample/opencontext/materialsampleobjecttype/0.1/tile",
        "whole organism specimen": "https://w3id.org/isample/vocabulary/materialsampleobjecttype/1.0/wholeorganism",
        "": "https://w3id.org/isample/vocabulary/materialsampleobjecttype/1.0/materialsample",
        "clothing": "https://w3id.org/isample/opencontext/materialsampleobjecttype/0.1/clothing",
        "fluid in container": "https://w3id.org/isample/vocabulary/materialsampleobjecttype/1.0/fluidincontainer",
        "organismproduct": "https://w3id.org/isample/vocabulary/materialsampleobjecttype/1.0/organismproduct",
        "organismpart": "https://w3id.org/isample/vocabulary/materialsampleobjecttype/1.0/organismpart",
        "experiment product": "https://w3id.org/isample/vocabulary/materialsampleobjecttype/1.0/experimentalproduct",
        "biome aggregation": "https://w3id.org/isample/vocabulary/materialsampleobjecttype/1.0/biomeaggregation",
        "biomeaggregation": "https://w3id.org/isample/vocabulary/materialsampleobjecttype/1.0/biomeaggregation",
        "domestic item": "https://w3id.org/isample/opencontext/materialsampleobjecttype/0.1/domesticitem",
        "wholeorganism": "https://w3id.org/isample/vocabulary/materialsampleobjecttype/1.0/wholeorganism",
        "biological specimen": "https://w3id.org/isample/vocabulary/materialsampleobjecttype/1.0/biologicalmaterialsample",
        "organism product": "https://w3id.org/isample/vocabulary/materialsampleobjecttype/1.0/organismproduct",
        "physical specimen": "https://w3id.org/isample/vocabulary/materialsampleobjecttype/1.0/materialsample",
    }

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
    def _convert_to_prediction_result_list(result: Any, mapped_values: dict = {}) -> list[PredictionResult]:
        prediction_results: list[PredictionResult] = []
        for prediction_dict in result:
            mapped_term = mapped_values.get(prediction_dict["value"], prediction_dict["value"])
            prediction_results.append(PredictionResult(mapped_term, prediction_dict["confidence"]))
        return prediction_results

    def _make_opencontext_request(self, source_record: dict, model_type: str,
                                  rsession: requests.Session = requests.Session(), mapped_values: dict = {}) -> list[PredictionResult]:
        params: dict = {"source_record": source_record, "type": model_type}
        url = f"{self.base_url}opencontext"
        return ModelServerClient._convert_to_prediction_result_list(self._make_json_request(url, params, rsession), mapped_values)

    def make_opencontext_material_request(self, source_record: dict, rsession: requests.Session = requests.Session()) -> list[PredictionResult]:
        return self._make_opencontext_request(source_record, "material", rsession, ModelServerClient.MATERIAL_CATEGORY_DICT)

    def make_opencontext_sample_request(self, source_record: dict, rsession: requests.Session = requests.Session()) -> list[PredictionResult]:
        return self._make_opencontext_request(source_record, "sample", rsession, ModelServerClient.MATERIAL_SAMPLE_DICT)

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
