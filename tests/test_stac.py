import os
import pytest
import json
import isb_lib.stac
import typing
from jsonschema import validate

SOLR_items = [
    "./test_data/isb_core_solr_documents/ark-21547-Dob2MVZ_HERP_266812.json",
    "./test_data/isb_core_solr_documents/ark-21547-BHP2CFR_368.json",
]


@pytest.fixture
def stac_item_schema_json() -> typing.Dict:
    with open("./test_data/stac_schema/item.json") as schema:
        return json.load(schema)


@pytest.fixture
def stac_collection_schema_json() -> typing.Dict:
    with open("./test_data/stac_schema/collection.json") as schema:
        return json.load(schema)


@pytest.mark.skipif(
    os.environ.get("CI") is not None, reason="Don't run live requests in CI"
)
@pytest.mark.parametrize("solr_file_path", SOLR_items)
def test_stac_item_valid(solr_file_path, stac_item_schema_json):
    with open(solr_file_path) as source_file:
        solr_document = json.load(source_file)
        stac_item = isb_lib.stac.stac_item_from_solr_dict(
            solr_document, "http://isamples.org/stac/", "http://isamples.org/thing/"
        )
        assert stac_item is not None
        validate(instance=stac_item, schema=stac_item_schema_json)


@pytest.mark.skipif(
    os.environ.get("CI") is not None, reason="Don't run live requests in CI"
)
@pytest.mark.parametrize("solr_file_path", SOLR_items)
def test_stac_collection_valid(solr_file_path, stac_collection_schema_json):
    with open(solr_file_path) as source_file:
        solr_document = json.load(source_file)
        stac_collection = isb_lib.stac.stac_collection_from_solr_dicts(
            [solr_document], False, 0, 1, None
        )
        assert stac_collection is not None
        validate(instance=stac_collection, schema=stac_collection_schema_json)


def _contains_next_link(links: typing.List[typing.Dict]) -> bool:
    next_links = [link for link in links if link["rel"] == "next"]
    return len(next_links) > 0


def test_stac_collection_pagination():
    with open(SOLR_items[0]) as source_file_zero:
        solr_document_zero = json.load(source_file_zero)
        # verify that the first "collection" has a pointer to the next
        stac_collection = isb_lib.stac.stac_collection_from_solr_dicts([solr_document_zero], True, 0, 1, None)
        links = stac_collection["links"]
        assert _contains_next_link(links) is True
    with open(SOLR_items[1]) as source_file_one:
        solr_document_one = json.load(source_file_one)
        # verify that the second "collection" has no pointer to the next
        stac_collection = isb_lib.stac.stac_collection_from_solr_dicts([solr_document_one], False, 0, 1, None)
        links = stac_collection["links"]
        assert _contains_next_link(links) is False
