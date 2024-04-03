import os
import csv
from pathlib import Path
from typing import Optional, Any

from isamples_metadata.metadata_constants import METADATA_LABEL, METADATA_IDENTIFIER

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
            self.vocabulary_terms_by_label[label] = label
            for child in value.get("children"):
                self._process_uijson_dict(child, key_prefix)


    def term_for_key(self, key: str) -> VocabularyTerm:
        return self.vocabulary_terms_by_key.get(key, VocabularyTerm(None, key, None))

    def term_for_label(self, label: str) -> VocabularyTerm:
        return self.vocabulary_terms_by_label.get(label, VocabularyTerm(None, label, None))


# parent_dir = Path(__file__).parent
# MATERIAL_TYPE = ControlledVocabulary(os.path.join(parent_dir, "materialType.txt"), "https://w3id.org/isample/vocabulary/material/0.9/")
# SAMPLED_FEATURE = ControlledVocabulary(os.path.join(parent_dir, "sampledfeature.txt"), "https://w3id.org/isample/vocabulary/sampledfeature/0.9")
# SPECIMEN_TYPE = ControlledVocabulary(os.path.join(parent_dir, "specimenType.txt"), "https://w3id.org/isample/vocabulary/specimentype/0.9")
# print()