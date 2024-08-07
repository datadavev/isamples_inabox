"""Module for formatting vocabulary terms in a format suitable for iSamples UI ingestion, e.g.

{
  "https://w3id.org/isample/vocabulary/materialsampleobjecttype/1.0/materialsample": {
    "label": {
      "en": "Material sample"
    },
    "children": [
    …
    ]
}
"""
import logging
from typing import Optional

from term_store import TermRepository
from term_store.db import Term

VOCAB_CACHE: dict = {}


def _read_descendants(term: Term, repository: TermRepository, all_uris: set) -> Optional[dict]:
    if term.uri in all_uris:
        return None
    term_dict = {}
    label = term.properties.get("labels")
    if label is not None:
        label = label[0]
    else:
        label = term.name
    children: list[dict] = []
    term_dict[term.uri] = {
        "label": {
            "en": label
        },
        "children": children
    }
    all_uris.add(term.uri)
    descendants = repository.narrower(term.uri)
    for descendant in descendants:
        child_dict = _read_descendants(descendant, repository, all_uris)
        if child_dict is not None:
            children.append(child_dict)
    return term_dict


def uijson_vocabulary_dict(top_level_uri: str, repository: TermRepository) -> dict:
    cached_result = VOCAB_CACHE.get(top_level_uri)
    if cached_result is not None:
        return cached_result
    root_term = repository.read(top_level_uri)
    if root_term is None:
        logging.warning(f"Expected to find root term with uri {top_level_uri}, found None instead.")
        return {}
    else:
        all_uris = set(root_term.uri)
        full_dict = _read_descendants(root_term, repository, all_uris)
        if full_dict is not None:
            VOCAB_CACHE[top_level_uri] = full_dict
            return full_dict
        else:
            return {}
