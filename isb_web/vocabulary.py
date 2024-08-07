import logging
from typing import Optional

import term_store
from term_store import TermRepository
from fastapi import APIRouter, Depends
from sqlmodel import Session
from isb_lib.vocabulary import vocab_adapter
from isb_web.sqlmodel_database import SQLModelDAO



SAMPLEDFEATURE_URI = "https://w3id.org/isample/vocabulary/sampledfeature/1.0/anysampledfeature"
MATERIAL_URI = "https://w3id.org/isample/vocabulary/material/1.0/material"
MATERIALSAMPLEOBJECTTYPE_URI = "https://w3id.org/isample/vocabulary/materialsampleobjecttype/1.0/materialsample"

router = APIRouter(prefix="/vocabulary")
dao: Optional[SQLModelDAO] = None
logging.basicConfig(level=logging.DEBUG)
_L = logging.getLogger("metrics")


def get_session():
    with dao.get_session() as session:
        yield session


def get_repository(session: Session = Depends(get_session)) -> TermRepository:
    return term_store.get_repository(session)


@router.get("/material_sample_object_type", tags=["vocabularies"])
def material_sample_object_type(repository: TermRepository = Depends(get_repository)) -> dict:
    return vocab_adapter.uijson_vocabulary_dict(MATERIALSAMPLEOBJECTTYPE_URI, repository)


@router.get("/material_type", tags=["vocabularies"])
def material_type(repository: TermRepository = Depends(get_repository)) -> dict:
    return vocab_adapter.uijson_vocabulary_dict(MATERIAL_URI, repository)


@router.get("/sampled_feature_type", tags=["vocabularies"])
def sampled_feature_type(repository: TermRepository = Depends(get_repository)) -> dict:
    return vocab_adapter.uijson_vocabulary_dict(SAMPLEDFEATURE_URI, repository)
