from typing import Optional, Any

from isamples_metadata.metadata_constants import METADATA_LABEL, METADATA_IDENTIFIER
from isb_lib.vocabulary import vocab_adapter
from isb_web.vocabulary import SAMPLEDFEATURE_URI, PHYSICALSPECIMEN_URI, MATERIAL_URI

"""
Note that this module operates on a CSV-derived form of the vocabulary sourced at
https://github.com/isamplesorg/vocabularies/tree/develop/src
"""


# Inherit from dict in order to make this class JSON serializable
class VocabularyTerm(dict):
    def __init__(self, key: Optional[str], label: str, uri: Optional[str]):
        self.key = key
        self.label = label
        self.uri = uri
        super().__init__(self.metadata_dict())

    def metadata_dict(self) -> dict[str, str]:
        metadata_dict = {
            METADATA_LABEL: self.label
        }
        if self.uri is not None:
            metadata_dict[METADATA_IDENTIFIER] = self.uri
        return metadata_dict


class ControlledVocabulary:
    def __init__(self, uijson_dict: dict[str, Any], key_prefix: str):
        self.vocabulary_terms_by_key = {}
        self.vocabulary_terms_by_label = {}
        self._is_first = True
        self._process_uijson_dict(uijson_dict, key_prefix)

    def _process_uijson_dict(self, uijson_dict: dict[str, Any], key_prefix: str):
        for dict_key, value in uijson_dict.items():
            # structure looks like this:
            """
                "https://w3id.org/isample/vocabulary/material/1.0/material":
                {
                    "label":
                    {
                        "en": "Material"
                    },
                    "children":
                    [
            """
            uri = dict_key
            label = value.get("label").get("en")
            last_piece_of_uri = dict_key.rsplit("/", 1)[-1]
            term_key = f"{key_prefix}:{last_piece_of_uri}"
            term = VocabularyTerm(term_key, label, uri)
            self.vocabulary_terms_by_key[term_key] = term
            self.vocabulary_terms_by_label[label] = term
            if self._is_first:
                self._root_term = term
                self._is_first = False
            for child in value.get("children"):
                self._process_uijson_dict(child, key_prefix)

    def root_term(self) -> VocabularyTerm:
        return self._root_term

    def term_for_key(self, key: str) -> VocabularyTerm:
        return self.vocabulary_terms_by_key.get(key, VocabularyTerm(None, key, None))

    def term_for_label(self, label: str) -> VocabularyTerm:
        return self.vocabulary_terms_by_label.get(label, VocabularyTerm(None, label, None))


SPECIMEN_TYPE = None
MATERIAL_TYPE = None
SAMPLED_FEATURE_TYPE = None


def specimen_type() -> ControlledVocabulary:
    global SPECIMEN_TYPE
    if SPECIMEN_TYPE is None:
        uijson = vocab_adapter.VOCAB_CACHE.get(PHYSICALSPECIMEN_URI)
        SPECIMEN_TYPE = ControlledVocabulary(uijson, "spec")
    return SPECIMEN_TYPE


def material_type() -> ControlledVocabulary:
    global MATERIAL_TYPE
    if MATERIAL_TYPE is None:
        uijson = vocab_adapter.VOCAB_CACHE.get(MATERIAL_URI)
        MATERIAL_TYPE = ControlledVocabulary(uijson, "mat")
    return MATERIAL_TYPE


def sampled_feature_type() -> ControlledVocabulary:
    global SAMPLED_FEATURE_TYPE
    if SAMPLED_FEATURE_TYPE is None:
        uijson = vocab_adapter.VOCAB_CACHE.get(SAMPLEDFEATURE_URI)
        SAMPLED_FEATURE_TYPE = ControlledVocabulary(uijson, "sf")
    return SAMPLED_FEATURE_TYPE
