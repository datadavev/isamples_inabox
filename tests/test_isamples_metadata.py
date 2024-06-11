import datetime
import json
import csv
from typing import Optional
from unittest.mock import patch

import pytest
import typing
import re

from jsonschema.validators import validate

import isamples_metadata.GEOMETransformer
from isamples_metadata import Transformer
from isamples_metadata.SESARTransformer import SESARTransformer
from isamples_metadata.GEOMETransformer import GEOMETransformer, GEOMEChildTransformer
from isamples_metadata.OpenContextTransformer import OpenContextTransformer
from isamples_metadata.SmithsonianTransformer import SmithsonianTransformer
from isamples_metadata.metadata_constants import METADATA_COMPLIES_WITH
from isamples_metadata.taxonomy.metadata_model_client import ModelServerClient, PredictionResult
from isb_lib import geome_adapter, sesar_adapter
from isb_lib.geome_adapter import GEOMEItem
from isb_lib.models.thing import Thing

# Set this to True in order to actually compare example vs. actual run.
# Otherwise, we'll assert that the transformer ran and had an id in the dictionary
from isb_lib.sesar_adapter import SESARItem

ASSERT_ON_OUTPUT = False
WRITE_OUTPUT_FILES = True


def _run_transformer(
    isamples_path,
    source_path,
    transformer_class,
    transformer=None,
    last_updated_time_str=None
):
    with open(source_path) as source_file:
        source_record = json.load(source_file)
        if transformer is None:
            transformer = transformer_class(source_record)
        transformed_to_isamples_record = transformer.transform(True)
        if last_updated_time_str is not None:
            assert transformer.last_updated_time() == last_updated_time_str
        _assert_transformed_dictionary(isamples_path, transformed_to_isamples_record)
        h3_key = "producedBy_samplingSite_location_h3_0"
        assert h3_key in transformed_to_isamples_record
        transformed_to_isamples_record_no_h3 = transformer.transform(False)
        assert h3_key not in transformed_to_isamples_record_no_h3


def _assert_transformed_dictionary(
    isamples_path: str, transformed_to_isamples_record: typing.Dict
):
    with open(isamples_path) as isamples_file:
        assert transformed_to_isamples_record.get("@id") is not None
        validate(instance=transformed_to_isamples_record, schema=isamples_schema_json())
        if ASSERT_ON_OUTPUT:
            isamples_record = json.load(isamples_file)
            assert transformed_to_isamples_record == isamples_record

    if WRITE_OUTPUT_FILES:
        with open(isamples_path, "w") as json_file:
            json.dump(transformed_to_isamples_record, json_file, indent=4)
            print(f"Dumped iSamples file to {isamples_path}")


SESAR_test_values = [
    (
        "./test_data/SESAR/raw/EOI00002Hjson-ld.json",
        "./test_data/SESAR/test/iSamplesEOI00002HBasic.json",
        "2014-02-18 09:32:01",
    ),
    (
        "./test_data/SESAR/raw/IEEJR000Mjson-ld.json",
        "./test_data/SESAR/test/iSamplesIEEJR000MBasic.json",
        "2021-01-19 04:32:44",
    ),
    (
        "./test_data/SESAR/raw/IE22301MWjson-ld.json",
        "./test_data/SESAR/test/iSamplesIE22301MWBasic.json",
        "2015-11-30 11:27:59"
    )
]


@pytest.mark.parametrize("sesar_source_path,isamples_path,timestamp", SESAR_test_values)
def test_sesar_dicts_equal(sesar_source_path, isamples_path, timestamp):
    _run_transformer(
        isamples_path, sesar_source_path, SESARTransformer, None, timestamp
    )


@pytest.mark.parametrize(
    "sesar_source_path,isamples_path,timestamp", SESAR_test_values
)
@pytest.mark.skip(reason="This functionality will go away once we fully move to SESAR iSB")
def test_sesar_as_core_record(
    sesar_source_path, isamples_path, timestamp
):
    with open(sesar_source_path) as source_file:
        source_record = json.load(source_file)
        test_thing = Thing()
        test_thing.authority_id = SESARItem.AUTHORITY_ID
        test_thing.resolved_content = source_record
        solr_doc = sesar_adapter.reparseAsCoreRecord(test_thing)
        if source_record.get("description").get("parentIdentifier") is not None:
            # child should have a relation pointing back to the parent
            child_doc = solr_doc[0]
            relations = child_doc.get("relations")
            assert len(relations) == 1
            assert relations[0].get("relation_target") is not None
            assert relations[0].get("relation_type") == "subsample"


GEOME_test_values = [
    (
        "./test_data/GEOME/raw/ark-21547-Car2PIRE_0334.json",
        "./test_data/GEOME/test/ark-21547-Car2PIRE_0334-test.json",
        datetime.datetime(year=1978, month=11, day=22, hour=12, minute=34),
        "1978-11-22T12:34:00",
    ),
    (
        "./test_data/GEOME/raw/ark-21547-CgZ2PEER_7055.json",
        "./test_data/GEOME/test/ark-21547-CgZ2PEER_7055-test.json",
        datetime.datetime(year=1978, month=11, day=22, hour=12, minute=34),
        "1978-11-22T12:34:00",
    ),
    (
        "./test_data/GEOME/raw/ark-21547-DRW2LACM-DISCO-16924.json",
        "./test_data/GEOME/test/ark-21547-DRW2LACM-DISCO-16924-test.json",
        datetime.datetime(year=1978, month=11, day=22, hour=12, minute=34),
        "1978-11-22T12:34:00",
    ),
]


@pytest.mark.parametrize(
    "geome_source_path,isamples_path,last_mod,last_mod_str", GEOME_test_values
)
def test_geome_dicts_equal(
    geome_source_path, isamples_path, last_mod: datetime.datetime, last_mod_str: str
):
    with open(geome_source_path) as source_file:
        source_record = json.load(source_file)
        transformer = GEOMETransformer(source_record, last_mod)
        _run_transformer(
            isamples_path, geome_source_path, None, transformer, last_mod_str
        )


@pytest.mark.parametrize(
    "geome_source_path,isamples_path,last_mod,last_mod_str", GEOME_test_values
)
def test_geome_as_core_record(
    geome_source_path, isamples_path, last_mod: datetime.datetime, last_mod_str: str
):
    with open(geome_source_path) as source_file:
        source_record = json.load(source_file)
        test_thing = Thing()
        test_thing.authority_id = GEOMEItem.AUTHORITY_ID
        test_thing.resolved_content = source_record
        solr_docs = geome_adapter.reparseAsCoreRecord(test_thing)
        if len(solr_docs) > 1:
            # second one should be a child record
            # and the child should have a relation pointing back to the parent
            child_doc = solr_docs[1]
            relations = child_doc.get("relatedResource_isb_core_id")
            assert relations is not None and len(relations) == 1


GEOME_child_test_values = [
    (
        "./test_data/GEOME/raw/ark-21547-Car2PIRE_0334.json",
        "./test_data/GEOME/test/ark-21547-Car2PIRE_0334-child-test.json",
        datetime.datetime(year=1978, month=11, day=22, hour=12, minute=34),
        "1978-11-22T12:34:00",
    )
]


@pytest.mark.parametrize(
    "geome_source_path,isamples_path,last_mod,last_mod_str", GEOME_child_test_values
)
def test_geome_child_dicts_equal(
    geome_source_path, isamples_path, last_mod: datetime.datetime, last_mod_str: str
):
    with open(geome_source_path) as source_file:
        source_record = json.load(source_file)
        transformer = GEOMETransformer(source_record, last_mod)
        child_transformer = transformer.child_transformers[0]
        transformed_to_isamples_record = child_transformer.transform()
        _assert_transformed_dictionary(isamples_path, transformed_to_isamples_record)
        assert last_mod_str == child_transformer.last_updated_time()
        # Also make sure the child picks up the localContextsId from the parent
        complies_with = transformed_to_isamples_record.get(METADATA_COMPLIES_WITH)
        assert complies_with is not None
        assert complies_with[0] == "localcontexts:projects/123456"


# test the special logic in GEOME to grab the proper transformer
def test_geome_transformer_for_identifier():
    test_file_path = "./test_data/GEOME/raw/ark-21547-Car2PIRE_0334.json"
    with open(test_file_path) as source_file:
        source_record = json.load(source_file)
        transformer = (
            isamples_metadata.GEOMETransformer.geome_transformer_for_identifier(
                "ark:/21547/Car2PIRE_0334", source_record, None, None
            )
        )
        assert type(transformer) is GEOMETransformer
        child_transformer = (
            isamples_metadata.GEOMETransformer.geome_transformer_for_identifier(
                "ark:/21547/Cat2INDO106431.1", source_record, None, None
            )
        )
        assert type(child_transformer) is GEOMEChildTransformer


OPENCONTEXT_test_values = [
    (
        "./test_data/OpenContext/raw/ark-28722-k2b85cg1p.json",
        "./test_data/OpenContext/test/ark-28722-k2b85cg1p-test.json",
        "2017-02-09T02:41:00Z",
    ),
    (
        "./test_data/OpenContext/raw/ark-28722-k26d5xr5z.json",
        "./test_data/OpenContext/test/ark-28722-k26d5xr5z-test.json",
        "2017-02-09T02:38:47Z",
    ),
    (
        "./test_data/OpenContext/raw/ark-28722-k2vq31x46.json",
        "./test_data/OpenContext/test/ark-28722-k2vq31x46-test.json",
        "2017-02-09T02:42:21Z",
    ),
    (
        "./test_data/OpenContext/raw/ark-28722-k26h4xk1f.json",
        "./test_data/OpenContext/test/ark-28722-k26h4xk1f-test.json",
        "2022-10-23T07:15:31Z",
    ),
    (
        "./test_data/OpenContext/raw/opencontext-new-format.json",
        "./test_data/OpenContext/test/opencontext-new-format-test.json",
        "2023-07-31T11:45:05Z"
    )
]


@pytest.mark.parametrize(
    "open_context_source_path,isamples_path,timestamp", OPENCONTEXT_test_values
)
def test_open_context_dicts_equal(open_context_source_path, isamples_path, timestamp):
    fake_prediction = [PredictionResult("Any anthropogenic material", 1.0)]
    with patch.object(ModelServerClient, "make_opencontext_material_request", return_value=fake_prediction):
        _run_transformer(
            isamples_path, open_context_source_path, OpenContextTransformer, None, timestamp
        )


def _get_record_with_id(record_id: str) -> typing.Dict:
    raw_csv = "./test_data/Smithsonian/DwC raw/DwC_occurrence_10.csv"
    with open(raw_csv, newline="") as csv_file:
        csv_reader = csv.reader(csv_file, delimiter="\t")
        column_headers = []
        for i, current_values in enumerate(csv_reader):
            if i == 0:
                column_headers = current_values
                continue
            # Otherwise iterate over the keys and make source JSON
            current_record: Optional[dict] = {}
            for index, key in enumerate(column_headers):
                if key == "id":
                    if record_id not in current_values[index]:
                        current_record = None
                        break
                if len(key) > 0:
                    current_record[key] = current_values[index]  # type: ignore
            if current_record is not None:
                return current_record
        print("Error, didn't find record with id: %s", record_id)
    return {}


SMITHSONIAN_test_values = [
    "./test_data/Smithsonian/DwC test/ark-65665-30000cb27-702b-4d34-ac24-3e46e14d5519-test.json",
    "./test_data/Smithsonian/DwC test/ark-65665-30000d403-f44f-498c-b7e3-ca1df52a2391-test.json",
    "./test_data/Smithsonian/DwC test/ark-65665-30002e5e4-91a3-4343-9519-2aab489dfbfd-test.json",
    "./test_data/Smithsonian/DwC test/ark-65665-30003a155-444f-4add-9ec0-48bd2631237e-test.json",
    "./test_data/Smithsonian/DwC test/ark-65665-30004d383-9b25-4cfd-840d-a720361ec77e-test.json",
]


@pytest.mark.parametrize("isamples_path", SMITHSONIAN_test_values)
def test_smithsonian_dicts_equal(isamples_path):
    with patch.object(ModelServerClient, "make_smithsonian_sampled_feature_request", return_value="Atmosphere"):
        id_piece = re.search(r"-([^-]+)-test", isamples_path).group(1)
        source_dict = _get_record_with_id(id_piece)
        # create the transformer from the specified row in the source .csv
        transformer = SmithsonianTransformer(source_dict)
        transformed_to_isamples_record = transformer.transform()
        _assert_transformed_dictionary(isamples_path, transformed_to_isamples_record)


def test_geo_to_h3():
    h3 = Transformer.geo_to_h3(32.253460, -110.911789)
    assert h3 is not None


def test_geo_to_h3_none():
    h3 = Transformer.geo_to_h3(None, None)
    assert h3 is None


def test_geome_geo_to_h3():
    test_file_path = "./test_data/GEOME/raw/ark-21547-Car2PIRE_0334.json"
    with open(test_file_path) as source_file:
        source_record = json.load(source_file)
        h3 = GEOMETransformer.geo_to_h3(source_record)
        assert "8f65534b37a2c0d" == h3


def test_open_context_extract_getty():
    test_file_path = "./test_data/OpenContext/raw/ark-28722-k26h4xk1f.json"
    with open(test_file_path) as source_file:
        source_record = json.load(source_file)
        transformer = OpenContextTransformer(source_record)
        keywords = transformer._extract_getty_keywords()
        assert 1 == len(keywords)
        glass_keyword = keywords[0]
        assert "glass (material)" == glass_keyword.value
        assert "https://vocab.getty.edu/aat/300010797" == glass_keyword.uri
        keyword_dict = glass_keyword.metadata_dict()
        assert keyword_dict is not None


def test_open_context_project_fields():
    test_file_path = "./test_data/OpenContext/raw/ark-28722-k2qv3rz30.json"
    with open(test_file_path) as source_file:
        source_record = json.load(source_file)
        transformer = OpenContextTransformer(source_record)
        produced_by_label = transformer.produced_by_label()
        assert "Avkat Archaeological Project" == produced_by_label
        produced_by_description = transformer.produced_by_description()
        assert "http://opencontext.org/projects/02b55e8c-e9b1-49e5-8edf-0afeea10e2be" == produced_by_description


def test_open_context_place_names():
    test_file_path = "./test_data/OpenContext/raw/ark-28722-k2qv3rz30.json"
    with open(test_file_path) as source_file:
        source_record = json.load(source_file)
        transformer = OpenContextTransformer(source_record)
        place_names = transformer.sampling_site_place_names()
        assert ["Asia", "Turkey", "Avkat Survey Area", "SU Group 10", "S1001"] == place_names


def isamples_schema_json() -> typing.Dict:
    with open("./test_data/json_schema/iSamplesSchemaCore1.0.json") as schema:
        return json.load(schema)
