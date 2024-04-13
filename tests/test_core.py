import os
import click.core
import pytest
import isb_lib.core
import json
import requests

from isamples_metadata.metadata_constants import METADATA_KEYWORDS
from isb_lib.core import things_main

TEST_LIVE_SERVER = 0

iterator_testcases = [
    (1, 1),
    (55, 55),
    (999, 999),
    (1000, 1000),
    (1001, 1000),
    (2020, 1000),
]


@pytest.mark.parametrize("max_entries,expected_outcome", iterator_testcases)
def test_IdentifierIterator(max_entries, expected_outcome):
    itr = isb_lib.core.IdentifierIterator(max_entries=max_entries)
    cnt = 0
    for e in itr:
        cnt += 1
    assert cnt == expected_outcome


def _try_to_add_solr_doc(core_doc_str):
    core_doc = json.loads(core_doc_str)
    solr_dict = isb_lib.core._coreRecordAsSolrDoc(core_doc)
    if TEST_LIVE_SERVER:
        isb_lib.core.solrAddRecords(requests.session(), [solr_dict], url="http://localhost:8983/api/collections/isb_core_records/")
    return solr_dict


def test_coreRecordAsSolrDoc():
    core_doc_str = """
{
    "$schema": "iSamplesSchemaCore1.0.json",
    "@id": "metadata/21547/Cat2INDO106431.1",
    "label": "INDO106431.1",
    "sample_identifier": "ark:/21547/Cat2INDO106431.1",
    "description": "",
    "has_context_category": [
        {
            "label": "Marine environment",
            "identifier": "https://w3id.org/isample/vocabulary/sampledfeature/0.9/sf:marinewaterbody"
        }
    ],
    "has_context_category_confidence": [
        1.0
    ],
    "has_material_category": [
        {
            "label": "Organic material",
            "identifier": "https://w3id.org/isample/vocabulary/material/0.9/mat:organicmaterial"
        }
    ],
    "has_material_category_confidence": [
        1.0
    ],
    "has_specimen_category": [
        {
            "label": "Organism part",
            "identifier": "https://w3id.org/isample/vocabulary/specimentype/0.9/spec:organismpart"
        }
    ],
    "has_specimen_category_confidence": [
        1.0
    ],
    "informal_classification": [
        "Gastropoda"
    ],
    "keywords": [
        {
            "keyword": "Aceh"
        },
        {
            "keyword": "Sumatra"
        },
        {
            "keyword": "Indonesia"
        },
        {
            "keyword": "Asia"
        },
        {
            "keyword": "Mollusca",
            "scheme_name": "Taxon: phylum"
        },
        {
            "keyword": "Gastropoda",
            "scheme_name": "Taxon: class"
        }
    ],
    "produced_by": {
        "@id": "ark:/21547/Car2PIRE_0334",
        "label": "tissue subsample from PIRE_0334",
        "description": "tissueCatalogNumber: http://n2t.net/ark:/21547/Q2INDO106431.1",
        "has_feature_of_interest": "",
        "responsibility": [],
        "result_time": "",
        "sampling_site": {
            "description": "Shallow, coastal reef. Apparent exposure to current, Porites dominated. Less impacted bleaching site, high recruitment, 12 m.",
            "label": "Pulau Seulako",
            "sample_location": {
                "elevation": "12 m",
                "latitude": 5.8943,
                "longitude": 95.25293
            },
            "place_name": [
                {
                    "keyword": "Pulau Seulako"
                },
                {
                    "keyword": "Aceh"
                },
                {
                    "keyword": "Sumatra"
                },
                {
                    "keyword": "Indonesia"
                },
                {
                    "keyword": "Asia"
                }
            ]
        }
    },
    "registrant": {
        "name": "Chris Meyer"
    },
    "sampling_purpose": "genomic analysis",
    "curation": {
        "label": "",
        "description": "",
        "access_constraints": "",
        "curation_location": "tissueWell: C06, tissuePlate: 03_ACEH_080916",
        "responsibility": [
            {
                "role": "curator",
                "name": "No Voucher"
            }
        ]
    },
    "related_resource": [
        {
            "label": "parent sample PIRE_0334",
            "target": "ark:/21547/Car2PIRE_0334",
            "relationship": "derived_from"
        }
    ],
    "authorized_by": [],
    "complies_with": [],
    "producedBy_samplingSite_location_h3_0": "8065fffffffffff",
    "producedBy_samplingSite_location_h3_1": "81643ffffffffff",
    "producedBy_samplingSite_location_h3_2": "826557fffffffff",
    "producedBy_samplingSite_location_h3_3": "83642cfffffffff",
    "producedBy_samplingSite_location_h3_4": "8465535ffffffff",
    "producedBy_samplingSite_location_h3_5": "8565534bfffffff",
    "producedBy_samplingSite_location_h3_6": "8665534b7ffffff",
    "producedBy_samplingSite_location_h3_7": "8765534b3ffffff",
    "producedBy_samplingSite_location_h3_8": "8865534b37fffff",
    "producedBy_samplingSite_location_h3_9": "8965534b37bffff",
    "producedBy_samplingSite_location_h3_10": "8a65534b37a7fff",
    "producedBy_samplingSite_location_h3_11": "8b65534b37a2fff",
    "producedBy_samplingSite_location_h3_12": "8c65534b37a2dff",
    "producedBy_samplingSite_location_h3_13": "8d65534b37a2c3f",
    "producedBy_samplingSite_location_h3_14": "8e65534b37a2c0f"
}
    """
    solr_dict = _try_to_add_solr_doc(core_doc_str)
    assert "producedBy_samplingSite_location_ll" in solr_dict
    assert "producedBy_samplingSite_location_h3_0" in solr_dict
    assert "producedBy_samplingSite_location_h3_15" in solr_dict


def test_coreRecordAsSolrDoc2():
    core_doc_str = """
{
    "$schema": "iSamplesSchemaCore1.0.json",
    "@id": "metadata/28722/k26h4xk1f",
    "label": "Object Reg. 697",
    "sample_identifier": "ark:/28722/k26h4xk1f",
    "description": "'early bce/ce': -800.0 | 'late bce/ce': 1000.0 | 'updated': 2022-10-23T07:15:31Z | 'Consists of': glass (material)",
    "has_context_category": [
        {
            "label": "Site of past human activities",
            "identifier": "https://w3id.org/isample/vocabulary/sampledfeature/0.9/sf:pasthumanoccupationsite"
        }
    ],
    "has_context_category_confidence": [
        1.0
    ],
    "has_material_category": [
        {
            "label": "Any anthropogenic material",
            "identifier": "https://w3id.org/isample/vocabulary/material/0.9/mat:anyanthropogenicmaterial"
        }
    ],
    "has_material_category_confidence": [
        1.0
    ],
    "has_specimen_category": [
        {
            "label": "Physical specimen",
            "identifier": "https://w3id.org/isample/vocabulary/specimen/0.9/spec:physicalspecimen"
        }
    ],
    "has_specimen_category_confidence": [
        1.0
    ],
    "informal_classification": [],
    "keywords": [
        {
            "keyword": "glass (material)",
            "keyword_uri": "https://vocab.getty.edu/aat/300010797",
            "scheme_name": "Getty Art & Architecture Thesaurus"
        }
    ],
    "produced_by": {
        "@id": "",
        "label": "Excavations at Polis",
        "description": "https://opencontext.org/projects/766d9fd5-2175-41e3-b7c9-7eba6777f1f0",
        "has_feature_of_interest": "",
        "responsibility": [
            {
                "role": "creator",
                "name": "Joanna Smith"
            }
        ],
        "result_time": "2017-01-30T22:57:28Z",
        "sampling_site": {
            "description": "",
            "label": "Europe/Cyprus/Polis Chrysochous/E.F2:R09",
            "sample_location": {
                "elevation": "",
                "latitude": null,
                "longitude": null
            },
            "place_name": [
                "Europe",
                "Cyprus",
                "Polis Chrysochous",
                "E.F2:R09"
            ]
        }
    },
    "registrant": {
        "name": ""
    },
    "sampling_purpose": "",
    "curation": {
        "label": "",
        "description": "",
        "access_constraints": "",
        "curation_location": "",
        "responsibility": []
    },
    "related_resource": [],
    "authorized_by": [],
    "complies_with": [],
    "producedBy_samplingSite_location_h3_0": "803ffffffffffff",
    "producedBy_samplingSite_location_h3_1": "813f7ffffffffff",
    "producedBy_samplingSite_location_h3_2": "823f4ffffffffff",
    "producedBy_samplingSite_location_h3_3": "833f4dfffffffff",
    "producedBy_samplingSite_location_h3_4": "843f4d9ffffffff",
    "producedBy_samplingSite_location_h3_5": "853f4d8bfffffff",
    "producedBy_samplingSite_location_h3_6": "863f4d8a7ffffff",
    "producedBy_samplingSite_location_h3_7": "873f4d8a5ffffff",
    "producedBy_samplingSite_location_h3_8": "883f4d8a59fffff",
    "producedBy_samplingSite_location_h3_9": "893f4d8a583ffff",
    "producedBy_samplingSite_location_h3_10": "8a3f4d8a5837fff",
    "producedBy_samplingSite_location_h3_11": "8b3f4d8a5830fff",
    "producedBy_samplingSite_location_h3_12": "8c3f4d8a5830bff",
    "producedBy_samplingSite_location_h3_13": "8d3f4d8a5830abf",
    "producedBy_samplingSite_location_h3_14": "8e3f4d8a5830a8f"
}
    """
    solr_dict = _try_to_add_solr_doc(core_doc_str)
    assert "producedBy_samplingSite_location_latlon" not in solr_dict


def test_core_record_as_solr_doc_3():
    core_doc_str = """
{
  "$schema": "iSamplesSchemaCore1.0.json",
  "@id": "metadata/28722/k2tb16c3r",
  "label": "Object 9019A (5)",
  "sample_identifier": "ark:/28722/k2tb16c3r",
  "description": "'early bce/ce': -200.0 | 'late bce/ce': 360.0 | 'updated': 2023-10-06T06:32:42Z | 'Consists of': rock (inorganic material)",
  "has_context_category": [
    {
      "label": "Site of past human activities",
      "identifier": "https://w3id.org/isample/vocabulary/sampledfeature/0.9/sf:pasthumanoccupationsite"
    }
  ],
  "has_context_category_confidence": [
    1
  ],
  "has_material_category": [
    {
      "label": "Anthropogenic material",
      "identifier": "https://w3id.org/isample/vocabulary/material/0.9/mat:otheranthropogenicmaterial"
    },
    {
      "label": "Anthropogenic metal",
      "identifier": "https://w3id.org/isample/vocabulary/material/0.9/mat:otheranthropogenicmaterial"
    },
    {
      "label": "Natural solid material",
      "identifier": "https://w3id.org/isample/vocabulary/material/0.9/mat:otheranthropogenicmaterial"
    }
  ],
  "has_material_category_confidence": [
    0.3525295853614807,
    0.32352039217948914,
    0.22828949987888336
  ],
  "has_specimen_category": [
    {
      "label": "physicalspecimen",
      "identifier": "https://w3id.org/isample/vocabulary/material/0.9/mat:otheranthropogenicmaterial"
    }
  ],
  "has_specimen_category_confidence": [
    1
  ],
  "informal_classification": [],
  "keywords": [
    {
      "keyword": {
        "id": "https://vocab.getty.edu/aat/300247919",
        "label": "fossils"
      },
      "keyword_uri": "",
      "scheme_name": "Getty Art & Architecture Thesaurus"
    },
    {
      "keyword": "Archaeology"
    },
    {
      "keyword": "Architecture"
    },
    {
      "keyword": "Religion"
    },
    {
      "keyword": "Nabateans"
    },
    {
      "keyword": "Chronology, Roman"
    },
    {
      "keyword": "Hellenistic Greek, Roman Republic"
    }
  ],
  "produced_by": {
    "@id": "",
    "label": "",
    "description": "",
    "has_feature_of_interest": "",
    "responsibility": [
      {
        "role": "creator",
        "name": "Martha Sharp Joukowsky"
      }
    ],
    "result_time": "2007-11-11T00:00:00Z",
    "sampling_site": {
      "description": "https://opencontext.org/subjects/d9ae02e5-c3f1-41d0-eb3a-39798f63f6c4",
      "label": "Petra Great Temple",
      "sample_location": {
        "elevation": "",
        "latitude": 30.3287,
        "longitude": 35.4421
      },
      "place_name": []
    }
  },
  "registrant": {
    "name": ""
  },
  "sampling_purpose": "",
  "curation": {
    "label": "",
    "description": "",
    "access_constraints": "",
    "curation_location": "",
    "responsibility": []
  },
  "related_resource": [],
  "authorized_by": [],
  "complies_with": [],
  "producedBy_samplingSite_location_h3_0": "803ffffffffffff",
  "producedBy_samplingSite_location_h3_1": "813e7ffffffffff",
  "producedBy_samplingSite_location_h3_2": "823e6ffffffffff",
  "producedBy_samplingSite_location_h3_3": "833e6dfffffffff",
  "producedBy_samplingSite_location_h3_4": "843e6ddffffffff",
  "producedBy_samplingSite_location_h3_5": "853e6dcbfffffff",
  "producedBy_samplingSite_location_h3_6": "863e6dca7ffffff",
  "producedBy_samplingSite_location_h3_7": "873e6dca5ffffff",
  "producedBy_samplingSite_location_h3_8": "883e6dca51fffff",
  "producedBy_samplingSite_location_h3_9": "893e6dca50bffff",
  "producedBy_samplingSite_location_h3_10": "8a3e6dca5017fff",
  "producedBy_samplingSite_location_h3_11": "8b3e6dca5012fff",
  "producedBy_samplingSite_location_h3_12": "8c3e6dca50121ff",
  "producedBy_samplingSite_location_h3_13": "8d3e6dca50120bf",
  "producedBy_samplingSite_location_h3_14": "8e3e6dca50120b7"
}
"""
    solr_dict = _try_to_add_solr_doc(core_doc_str)
    assert "fossils" in solr_dict.get(METADATA_KEYWORDS)


def _load_test_file_into_solr_doc(file_path: str) -> dict:
    with open(file_path, "r") as source_file:
        source_record = source_file.read()
        solr_doc = _try_to_add_solr_doc(source_record)
    return solr_doc


def test_vocabulary_included():
    solr_doc = _load_test_file_into_solr_doc("./test_data/GEOME/test/ark-21547-Car2PIRE_0334-child-test.json")
    context_categories = solr_doc.get("hasContextCategory")
    assert context_categories is not None
    assert context_categories[0] == "https://w3id.org/isample/vocabulary/sampledfeature/1.0/anysampledfeature"
    material_categories = solr_doc.get("hasMaterialCategory")
    assert material_categories is not None
    assert material_categories[0] == "https://w3id.org/isample/vocabulary/material/1.0/organicmaterial"
    specimen_categories = solr_doc.get("hasSpecimenCategory")
    assert specimen_categories is not None
    assert specimen_categories[0] == "https://w3id.org/isample/vocabulary/specimentype/1.0/organismpart"
    keywords = solr_doc.get("keywords")
    assert keywords is not None
    assert keywords[0] == "Aceh"


def test_produced_by_fields():
    solr_doc = _load_test_file_into_solr_doc("./test_data/GEOME/test/ark-21547-Car2PIRE_0334-test.json")
    produced_by_responsibility = solr_doc.get("producedBy_responsibility")
    assert produced_by_responsibility is not None
    assert produced_by_responsibility[0] == "Collector:Aji Wahyu Anggoro"
    registrant = solr_doc.get("registrant")
    assert registrant is not None
    assert registrant == "Chris Meyer"
    curation_responsibility = solr_doc.get("curation_responsibility")
    assert curation_responsibility is not None
    assert curation_responsibility[0] == "curator:No Voucher"
    sampling_site_place_name = solr_doc.get("producedBy_samplingSite_placeName")
    assert sampling_site_place_name is not None
    assert sampling_site_place_name[0] == "Pulau Seulako"


def test_produced_by_fields_opencontext():
    solr_doc = _load_test_file_into_solr_doc("./test_data/OpenContext/test/ark-28722-k26d5xr5z-test.json")
    produced_by_responsibility = solr_doc.get("producedBy_responsibility")
    assert produced_by_responsibility is not None
    assert produced_by_responsibility[0] == "creator:Martha Sharp Joukowsky"


def test_produced_by_fields_smithsonian():
    solr_doc = _load_test_file_into_solr_doc("./test_data/Smithsonian/DwC test/ark-65665-30000cb27-702b-4d34-ac24-3e46e14d5519-test.json")
    produced_by_responsibility = solr_doc.get("producedBy_responsibility")
    assert produced_by_responsibility is not None
    assert produced_by_responsibility[0] == "recorded by:R. Causse"


def test_convert_all_test_files_to_solr_doc():
    dirs = ["./test_data/Smithsonian/DwC test", "./test_data/OpenContext/test", "./test_data/GEOME/test"]
    for dir in dirs:
        for root, _, files in os.walk(dir):
            for file in files:
                if file.endswith(".json"):
                    # Create the full path to the JSON file
                    json_file_path = os.path.join(root, file)
                    print(f"checking {json_file_path}")
                    solr_doc = _load_test_file_into_solr_doc(json_file_path)
                    assert solr_doc is not None


def test_date_year_only():
    date_str = "1985"
    datetime = isb_lib.core.parsed_date(date_str)
    assert datetime is not None
    assert datetime.day == 1
    assert datetime.year == 1985


def test_date_year_month_day():
    date_str = "1947-08-06"
    datetime = isb_lib.core.parsed_date(date_str)
    assert datetime is not None
    assert datetime.day == 6
    assert datetime.month == 8
    assert datetime.year == 1947


def test_date_year_month():
    date_str = "2020-07"
    datetime = isb_lib.core.parsed_date(date_str)
    assert datetime is not None
    assert datetime.month == 7
    assert datetime.year == 2020
    # default to the first of the month since it wasn't in the original
    assert datetime.day == 1


def test_date_with_time():
    date_str = "2019-12-08 15:54:00"
    datetime = isb_lib.core.parsed_date(date_str)
    assert datetime is not None
    assert datetime.year == 2019
    assert datetime.month == 12
    assert datetime.day == 8
    assert datetime.hour == 15
    assert datetime.minute == 54
    assert datetime.second == 0
    assert datetime.tzinfo.zone == 'UTC'


def test_isamples_date():
    datetime = isb_lib.core.parsed_datetime_from_isamples_format("2020-07-16T11:25:16Z")
    assert datetime is not None
    assert datetime.year == 2020
    assert datetime.month == 7
    assert datetime.day == 16
    assert datetime.hour == 11
    assert datetime.minute == 25
    assert datetime.second == 16


def test_isamples_date_with_ms():
    datetime = isb_lib.core.parsed_datetime_from_isamples_format("2020-07-16T11:25:16.123Z")
    assert datetime is not None
    assert datetime.year == 2020
    assert datetime.month == 7
    assert datetime.day == 16
    assert datetime.hour == 11
    assert datetime.minute == 25
    assert datetime.second == 16


def test_things_main():
    things_main(click.core.Context(click.core.Command("test")), None, None)
