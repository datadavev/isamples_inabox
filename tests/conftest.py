import json

import pytest

from isamples_metadata.vocabularies import vocabulary_mapper
from isamples_metadata.vocabularies.vocabulary_mapper import ControlledVocabulary
from isb_lib.vocabulary import vocab_adapter
from isb_web.vocabulary import SAMPLEDFEATURE_URI, MATERIAL_URI, MATERIALSAMPLEOBJECTTYPE_URI


def _construct_controlled_vocabulary(filename: str, prefix: str) -> ControlledVocabulary:
    with open(f"./test_data/controlled_vocabulary_uijson/{filename}") as source_file:
        uijson = json.load(source_file)
        return ControlledVocabulary(uijson, prefix)


@pytest.fixture(scope="session", autouse=True)
def controlled_vocabularies():
    sampled_feature_type_vocabulary = _construct_controlled_vocabulary("sampled_feature_type.json", "sf")
    vocab_adapter.VOCAB_CACHE[SAMPLEDFEATURE_URI] = sampled_feature_type_vocabulary._uijson_dict
    vocabulary_mapper.SAMPLED_FEATURE_TYPE = sampled_feature_type_vocabulary
    material_sample_object_type_vocabulary = _construct_controlled_vocabulary("material_sample_object_type.json", "spec")
    vocab_adapter.VOCAB_CACHE[MATERIALSAMPLEOBJECTTYPE_URI] = material_sample_object_type_vocabulary._uijson_dict
    vocabulary_mapper.SPECIMEN_TYPE = material_sample_object_type_vocabulary
    material_type_vocabulary = _construct_controlled_vocabulary("material_type.json", "mat")
    vocab_adapter.VOCAB_CACHE[MATERIAL_URI] = material_type_vocabulary._uijson_dict
    vocabulary_mapper.MATERIAL_TYPE = material_type_vocabulary
