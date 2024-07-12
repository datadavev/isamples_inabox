import math

import pytest
import requests
import typing
import os
import json

from isb_web.isb_solr_query import solr_last_mod_date_for_ids


@pytest.fixture
def rsession():
    return requests.session()


@pytest.fixture
def solr_url():
    solr_url = os.getenv("INPUT_SOLR_URL")
    if solr_url is None:
        solr_url = "https://central.isample.xyz/isamples_central/thing/select"
    return solr_url


def headers():
    return {
        "accept": "application/json",
        "User-Agent": "iSamples SOLR Integration Bot 2000",
    }


authority_ids = [
    "SESAR",
    "OPENCONTEXT",
    "GEOME",
    "SMITHSONIAN",
]


def _send_solr_query(
    rsession: requests.Session, solr_url: str, query: str
) -> typing.List[typing.Dict]:
    params: dict = {
        "start": 0,
        "limit": 10,
        "fl": "*",
        "q": query,
    }
    res = rsession.get(solr_url, headers=headers(), params=params)
    response_dict = res.json()
    docs = response_dict.get("response").get("docs")
    return docs


def _value_in_search_text(value: str, doc: typing.Dict) -> bool:
    appears_in_search_text = False
    for searchText in doc["searchText"]:
        if value.lower() in searchText.lower():
            appears_in_search_text = True
            break
    return appears_in_search_text


def search_text_query(search_text: str) -> str:
    return f"searchText:({search_text})"


@pytest.mark.parametrize("authority_id", authority_ids)
def test_solr_query_by_source(
    rsession: requests.Session, solr_url: str, authority_id: str
):
    docs = _send_solr_query(rsession, solr_url, search_text_query(authority_id))
    for doc in docs:
        appears_in_search_text = _value_in_search_text(authority_id, doc)
        assert appears_in_search_text


opencontext_projects = [
    "Çatalhöyük Zooarchaeology",
    "Petra Great Temple Excavations",
    "Excavations at Polis",
    "Avkat Archaeological Project",
    "Kenan Tepe",
    "Giza Botanical Database",
]


@pytest.mark.parametrize("project_label", opencontext_projects)
def test_opencontext_projects(
    rsession: requests.Session, solr_url: str, project_label: str
):
    docs = _send_solr_query(rsession, solr_url, search_text_query(project_label))
    for doc in docs:
        for word in project_label.split():
            appears_in_search_text = _value_in_search_text(word, doc)
            assert appears_in_search_text
            assert doc["source"] == "OPENCONTEXT"


geome_search_terms = [
    "SYMBIOCODE",
    "INDO_PIRE",
    "PEER_2016",
    "gastropoda",
]


@pytest.mark.parametrize("geome_search_term", geome_search_terms)
def test_geome_search_terms(
    rsession: requests.Session, solr_url: str, geome_search_term: str
):
    docs = _send_solr_query(rsession, solr_url, search_text_query(geome_search_term))
    for doc in docs:
        for word in geome_search_term.split():
            appears_in_search_text = _value_in_search_text(word, doc)
            assert appears_in_search_text
            assert doc["source"] == "GEOME"


geographic_search_terms = [
    "New Zealand",
    "United States",
    "Moorea",
    "Bali",
    "Italy",
]


@pytest.mark.parametrize("geographic_search_term", geographic_search_terms)
def test_geographic_search_terms(
    rsession: requests.Session, solr_url: str, geographic_search_term: str
):
    docs = _send_solr_query(rsession, solr_url, search_text_query(geographic_search_term))
    for doc in docs:
        appears_in_search_text = _value_in_search_text(geographic_search_term, doc)
        assert appears_in_search_text


def _transformed_json_to_test_tuples() -> list[tuple]:
    """
    We start with this structure:

      "IGSN:ODP02CV44": {
        "material": {
          "values": [
            "Natural Solid Material"
          ],
          "confidence": [
            -1
          ]
        }
      }

    We want to end up with this structure:

    solr_test_values = [
        (
            "IGSN:NHB002GWT",
            {
                "hasContextCategory": ["Earth interior"],
                "hasMaterialCategory": ["Mineral"],
                "hasSpecimenCategory": ["Other solid object"]
            }
        )
    ]
    """
    transformed_json: list[tuple] = []

    with open("sesar_material_test_records.json", "r") as schema_json_file:
        test_model_values_dict = json.load(schema_json_file)
        for key, value in test_model_values_dict.items():
            current_tuple = (
                key,
                {
                    "hasMaterialCategory": value.get("material").get("values"),
                    "hasMaterialCategoryConfidence": value.get("material").get("confidence")
                }
            )
            transformed_json.append(current_tuple)
    return transformed_json


@pytest.mark.skipif(os.environ.get("CI") is not None, reason="Only run this test manually, not in CI.")
@pytest.mark.parametrize("id,params", _transformed_json_to_test_tuples())
def test_solr_integration_test(rsession: requests.Session, solr_url: str, id: str, params: dict):
    solr_query = f"id:\"{id}\""
    docs = _send_solr_query(rsession, solr_url, solr_query)
    assert len(docs) > 0
    test_doc = docs[0]
    for key, value in params.items():
        other_array = test_doc.get(key)
        assert other_array is not None
        index = 0
        for item in value:
            if type(item) is float:
                assert math.isclose(other_array[index], item, abs_tol=0.001)
            else:
                assert test_doc.get(key) == value
            index += 1


@pytest.mark.skipif(os.environ.get("CI") is not None, reason="Only run this test manually, not in CI.")
def test_solr_last_mod_date_for_ids(rsession: requests.Session, solr_url: str):
    last_mod = solr_last_mod_date_for_ids(["ark:/21547/CYR2envbio09_975", "ark:/21547/CYR2envbio09_976"])
    print(f"last mod is {last_mod}")
