import json
import os.path
import uuid

import petl
import pytest
from jsonschema.validators import validate
from petl import Table

from isamples_metadata.metadata_constants import METADATA_SAMPLE_IDENTIFIER
from isb_lib.utilities.solr_result_transformer import SolrResultTransformer, TargetExportFormat

SOLR_items = [
    "./test_data/solr_results/test_solr_results.json",
]


def _test_path() -> str:
    return f"./test_data/solr_results/solr_results_{uuid.uuid4()}"


def _solr_result_table(solr_file_path: str) -> Table:
    with open(solr_file_path, "r") as file:
        solr_result_dict: dict = json.load(file)
        result_set: dict = solr_result_dict.get("result-set", {})
        if result_set is not None:
            docs: list[dict] = result_set.get("docs", {})
            table = petl.fromdicts(docs)
            return table
        else:
            raise ValueError(f"Didn't find expected solr results in file at {solr_file_path}")


@pytest.mark.parametrize("solr_file_path", SOLR_items)
def test_solr_result_transformer_csv(solr_file_path: str):
    table = _solr_result_table(solr_file_path)
    dest_path_no_extension = _test_path()
    solr_result_transformer = SolrResultTransformer(table, TargetExportFormat.CSV, dest_path_no_extension, False)
    dest_path = solr_result_transformer.transform()
    assert os.path.exists(dest_path)
    initial_size = os.path.getsize(dest_path)
    # write in append mode
    appending_transformer = SolrResultTransformer(table, TargetExportFormat.CSV, dest_path_no_extension, True)
    dest_path_2 = appending_transformer.transform()
    assert dest_path == dest_path_2
    appended_size = os.path.getsize(dest_path_2)
    assert appended_size > initial_size


@pytest.fixture
def isamples_schema_json() -> dict:
    with open("./test_data/json_schema/iSamplesSchemaCore1.0.json") as schema:
        return json.load(schema)


def _validate_dest_paths(dest_paths: list[str], isamples_schema_json: dict):
    for dest_path in dest_paths:
        assert os.path.exists(dest_path)
        assert os.path.getsize(dest_path) > 0
    with open(dest_path, "r") as output_file:
        # verify each line is a valid json object
        for json_line in output_file.readlines():
            json_dict = json.loads(json_line)
            assert json_dict is not None
            assert json_dict.get(METADATA_SAMPLE_IDENTIFIER) is not None
            validate(instance=json_dict, schema=isamples_schema_json)

@pytest.mark.parametrize("solr_file_path", SOLR_items)
def test_solr_result_transformer_jsonl(solr_file_path: str, isamples_schema_json: dict):
    table = _solr_result_table(solr_file_path)
    dest_path_no_extension = _test_path()
    solr_result_transformer = SolrResultTransformer(table, TargetExportFormat.JSONL, dest_path_no_extension, False)
    dest_paths = solr_result_transformer.transform()
    _validate_dest_paths(dest_paths, isamples_schema_json)


@pytest.mark.parametrize("solr_file_path", SOLR_items)
def test_solr_result_transformer_jsonl_multiple_files(solr_file_path: str, isamples_schema_json: dict):
    table = _solr_result_table(solr_file_path)
    dest_path_no_extension = os.path.join(os.getcwd(), "sitemap")
    solr_result_transformer = SolrResultTransformer(table, TargetExportFormat.JSONL, dest_path_no_extension, False, "2023-02-03T20:06:26.031Z", 2)
    dest_paths = solr_result_transformer.transform()
    _validate_dest_paths(dest_paths, isamples_schema_json)