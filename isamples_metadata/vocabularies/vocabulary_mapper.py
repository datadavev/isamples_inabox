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
        self._key_prefix = key_prefix
        self._is_first = True
        self._process_uijson_dict(uijson_dict)

    def _term_key_for_label(self, label: str):
        return f"{self._key_prefix}:{label}"

    def _process_uijson_dict(self, uijson_dict: dict[str, Any]):
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
            term_key = self._term_key_for_label(last_piece_of_uri)
            term = VocabularyTerm(term_key, label, uri)
            # There's a mix of callers that use both namespaced and non-namespaced keys to look terms up.
            # We should support both, e.g. "biogenicnonorganicmaterial" and "spec:biogenicnonorganicmaterial"
            self.vocabulary_terms_by_key[term_key.lower()] = term
            self.vocabulary_terms_by_key[last_piece_of_uri] = term
            self.vocabulary_terms_by_label[label.lower()] = term
            if self._is_first:
                self._root_term = term
                self._is_first = False
            for child in value.get("children"):
                self._process_uijson_dict(child)

    def root_term(self) -> VocabularyTerm:
        return self._root_term

    def term_for_key(self, key: str) -> VocabularyTerm:
        term = self.vocabulary_terms_by_key.get(key.lower())
        if term is None:
            term = self.vocabulary_terms_by_label.get(self._term_key_for_label(key.lower()))
        return term

    def term_for_label(self, label: str) -> VocabularyTerm:
        return self.vocabulary_terms_by_label.get(label.lower())


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
