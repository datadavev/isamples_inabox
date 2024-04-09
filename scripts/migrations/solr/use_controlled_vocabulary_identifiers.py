import logging
from typing import Optional

import click
import requests
from sqlmodel import Session

import isb_lib.core
import isb_web.config
import isb_lib.sesar_adapter
from isamples_metadata.solr_field_constants import SOLR_HAS_MATERIAL_CATEGORY, SOLR_HAS_SPECIMEN_CATEGORY, \
    SOLR_HAS_CONTEXT_CATEGORY
from isb_web.isb_solr_query import ISBCoreSolrRecordIterator

TOP_LEVEL_SPECIMEN_URI = "https://w3id.org/isample/vocabulary/specimentype/1.0/physicalspecimen"
TOP_LEVEL_CONTEXT_URI = "https://w3id.org/isample/vocabulary/sampledfeature/1.0/anysampledfeature"
TOP_LEVEL_MATERIAL_URI = "https://w3id.org/isample/vocabulary/material/1.0/material"


MATERIAL_CATEGORY_DICT = {
    "natural solid material": "https://w3id.org/isample/vocabulary/material/1.0/earthmaterial",
    "organic material": "https://w3id.org/isample/vocabulary/material/1.0/organicmaterial",
    "rock": "https://w3id.org/isample/vocabulary/material/1.0/rock",
    "sediment": "https://w3id.org/isample/vocabulary/material/1.0/sediment",
    "mixed soil": "https://w3id.org/isample/vocabulary/material/1.0/mixedsoilsedimentrock",
    "biogenicnonorganicmaterial": "https://w3id.org/isample/vocabulary/material/1.0/biogenicnonorganicmaterial",
    "material": "https://w3id.org/isample/vocabulary/material/1.0/material",
    "mineral": "https://w3id.org/isample/vocabulary/material/1.0/mineral",
    "biogenic non-organic material": "https://w3id.org/isample/vocabulary/material/1.0/biogenicnonorganicmaterial",
    "mat:rock": "https://w3id.org/isample/vocabulary/material/1.0/rock",
    "mat:biogenicnonorganicmaterial": "https://w3id.org/isample/vocabulary/material/1.0/biogenicnonorganicmaterial",
    "mat:anthropogenicmetal": "https://w3id.org/isample/vocabulary/material/1.0/anthropogenicmetal",
    "ocmat:ceramicclay": "https://w3id.org/isample/opencontext/material/0.1/ceramicclay",
    "not provided": "https://w3id.org/isample/vocabulary/material/1.0/material",
    "soil": "https://w3id.org/isample/vocabulary/material/1.0/soil",
    "organicmaterial": "https://w3id.org/isample/vocabulary/material/1.0/organicmaterial",
    "liquid water": "https://w3id.org/isample/vocabulary/material/1.0/liquidwater",
    "anyanthropogenicmaterial": "https://w3id.org/isample/vocabulary/material/1.0/anyanthropogenicmaterial",
    "anthropogenic metal": "https://w3id.org/isample/vocabulary/material/1.0/anthropogenicmetal",
    "gaseous material": "https://w3id.org/isample/vocabulary/material/1.0/gas",
    "anthropogenic material": "https://w3id.org/isample/vocabulary/material/1.0/anyanthropogenicmaterial",
    "anthropogenicmetal": "https://w3id.org/isample/vocabulary/material/1.0/anthropogenicmetal",
    "ocmat:organicanimalproduct": "https://w3id.org/isample/opencontext/material/0.1/organicanimalproduct",
    "biogenic non organic material": "https://w3id.org/isample/vocabulary/material/1.0/biogenicnonorganicmaterial",
    "particulate": "https://w3id.org/isample/vocabulary/material/1.0/particulate",
    "non-aqueous liquid material": "https://w3id.org/isample/vocabulary/material/1.0/nonaqueousliquid",
    "ice": "https://w3id.org/isample/vocabulary/material/1.0/anyice",
    "ocmat:plantmaterial": "https://w3id.org/isample/opencontext/material/0.1/plantmaterial"
}

CONTEXT_CATEGORY_DICT = {
    "not provided": "https://w3id.org/isample/vocabulary/sampledfeature/1.0/anysampledfeature",
    "site of past human activities": "https://w3id.org/isample/vocabulary/sampledfeature/1.0/pasthumanoccupationsite",
    "earth interior": "https://w3id.org/isample/vocabulary/sampledfeature/1.0/earthinterior",
    "animalia": "https://w3id.org/isample/biology/biosampledfeature/1.0/Animalia",
    " ": "https://w3id.org/isample/vocabulary/sampledfeature/1.0/anysampledfeature",
    "subaerial surface environment": "https://w3id.org/isample/vocabulary/sampledfeature/1.0/subaerialsurfaceenvironment",
    "marine environment": "https://w3id.org/isample/vocabulary/sampledfeature/1.0/marinewaterbody",
    "plantae": "https://w3id.org/isample/biology/biosampledfeature/1.0/Plantae",
    "marine water body bottom": "https://w3id.org/isample/vocabulary/sampledfeature/1.0/marinewaterbodybottom",
    "terrestrial water body": "https://w3id.org/isample/vocabulary/sampledfeature/1.0/terrestrialwaterbody",
    "fungi": "https://w3id.org/isample/biology/biosampledfeature/1.0/Fungi",
    "marine water body": "https://w3id.org/isample/vocabulary/sampledfeature/1.0/waterbody",
    "lake, river or stream bottom": "https://w3id.org/isample/vocabulary/sampledfeature/1.0/lakeriverstreambottom",
    "subsurface fluid reservoir": "https://w3id.org/isample/vocabulary/sampledfeature/1.0/subsurfacefluidreservoir",
    "marine biome": "https://w3id.org/isample/vocabulary/sampledfeature/1.0/marinewaterbody",
    "chromista": "",
    "subaerial terrestrial biome": "https://w3id.org/isample/vocabulary/sampledfeature/1.0/subaerialsurfaceenvironment",
    "bacteria": "https://w3id.org/isample/biology/biosampledfeature/1.0/bacteria",
    "protozoa": "https://w3id.org/isample/biology/biosampledfeature/1.0/protozoa",
    "active human occupation site": "https://w3id.org/isample/vocabulary/sampledfeature/1.0/activehumanoccupationsite",
    "lake river or stream bottom": "https://w3id.org/isample/vocabulary/sampledfeature/1.0/lakeriverstreambottom"
}

SPECIMEN_CATEGORY_DICT = {
    "other solid object": "https://w3id.org/isample/vocabulary/specimentype/1.0/othersolidobject",
    "container": "https://w3id.org/isample/opencontext/specimentype/0.1/containerobject",
    "ornament": "https://w3id.org/isample/opencontext/specimentype/0.1/ornament",
    "architectural element": "https://w3id.org/isample/opencontext/specimentype/0.1/architecturalelement",
    "organism part": "https://w3id.org/isample/vocabulary/specimentype/1.0/organismpart",
    "whole organism": "https://w3id.org/isample/vocabulary/specimentype/1.0/wholeorganism",
    "physicalspecimen": "https://w3id.org/isample/vocabulary/specimentype/1.0/physicalspecimen",
    "artifact": "https://w3id.org/isample/vocabulary/specimentype/1.0/artifact",
    "aggregation": "https://w3id.org/isample/vocabulary/specimentype/1.0/genericaggregation",
    "not provided": "https://w3id.org/isample/vocabulary/specimentype/1.0/physicalspecimen",
    "biologicalspecimen": "https://w3id.org/isample/vocabulary/specimentype/1.0/biologicalspecimen",
    "analytical preparation": "https://w3id.org/isample/vocabulary/specimentype/1.0/analyticalpreparation",
    "tile": "https://w3id.org/isample/opencontext/specimentype/0.1/tile",
    "whole organism specimen": "https://w3id.org/isample/vocabulary/specimentype/1.0/wholeorganism",
    "": "https://w3id.org/isample/vocabulary/specimentype/1.0/physicalspecimen",
    "clothing": "https://w3id.org/isample/opencontext/specimentype/0.1/clothing",
    "fluid in container": "https://w3id.org/isample/vocabulary/specimentype/1.0/fluidincontainer",
    "organismproduct": "https://w3id.org/isample/vocabulary/specimentype/1.0/organismproduct",
    "organismpart": "https://w3id.org/isample/vocabulary/specimentype/1.0/organismpart",
    "experiment product": "https://w3id.org/isample/vocabulary/specimentype/1.0/experimentalproduct",
    "biome aggregation": "https://w3id.org/isample/vocabulary/specimentype/1.0/biomeaggregation",
    "biomeaggregation": "https://w3id.org/isample/vocabulary/specimentype/1.0/biomeaggregation",
    "domestic item": "https://w3id.org/isample/opencontext/specimentype/0.1/domesticitem",
    "wholeorganism": "https://w3id.org/isample/vocabulary/specimentype/1.0/wholeorganism",
    "biological specimen": "https://w3id.org/isample/vocabulary/specimentype/1.0/biologicalspecimen",
    "organism product": "https://w3id.org/isample/vocabulary/specimentype/1.0/organismproduct",
    "physical specimen": "https://w3id.org/isample/vocabulary/specimentype/1.0/physicalspecimen"
}


@click.command()
@click.pass_context
def main(ctx):
    solr_url = isb_web.config.Settings().solr_url
    db_url = isb_web.config.Settings().database_url
    isb_lib.core.things_main(ctx, db_url, solr_url)
    # session = SQLModelDAO(db_url).get_session()
    convert_to_controlled_vocabulary_identifiers(solr_url, None)


def convert_to_controlled_vocabulary_identifiers(solr_url: str, session: Session):
    total_records = 0
    batch_size = 100
    current_mutated_batch = []
    rsession = requests.session()
    iterator = ISBCoreSolrRecordIterator(
        rsession, "-(_nest_path_:*) AND -(source:GEOME)", batch_size, 0, "id asc"
    )
    for record in iterator:
        mutated_record = mutate_record(record)
        if mutated_record is not None:
            current_mutated_batch.append(mutated_record)
        if len(current_mutated_batch) == batch_size:
            save_mutated_batch(current_mutated_batch, rsession, solr_url)
            current_mutated_batch = []
        total_records += 1
    if len(current_mutated_batch) > 0:
        # handle the remainder
        save_mutated_batch(current_mutated_batch, rsession, solr_url)
    logging.info(f"Finished iterating, visited {total_records} records")


def save_mutated_batch(current_mutated_batch, rsession, solr_url):
    logging.info(f"Going to save {len(current_mutated_batch)} records")
    isb_lib.core.solrAddRecords(rsession, current_mutated_batch, solr_url)
    isb_lib.core.solrCommit(rsession, solr_url)
    logging.info(f"Just saved {len(current_mutated_batch)} records")


def mutate_record(record: dict) -> Optional[dict]:
    # Do whatever work is required to mutate the record to update things…
    record_copy = record.copy()
    # Remove old problematic fields
    current_materials: list[str] = record.pop(SOLR_HAS_MATERIAL_CATEGORY)
    current_specimens: list[str] = record.pop(SOLR_HAS_SPECIMEN_CATEGORY)
    current_contexts: list[str] = record.pop(SOLR_HAS_CONTEXT_CATEGORY)

    new_materials = []
    for label in current_materials:
        if label.startswith("https://w3id.org"):
            # we've already touched it, bail
            return None
        identifier = MATERIAL_CATEGORY_DICT.get(label.lower())
        if identifier is None:
            logging.error(f"Unable to look up material value for label {label}")
        else:
            new_materials.append(identifier)
    record_copy[SOLR_HAS_MATERIAL_CATEGORY] = new_materials

    new_specimens = []
    for label in current_specimens:
        if label.startswith("https://w3id.org"):
            # we've already touched it, bail
            return None
        identifier = SPECIMEN_CATEGORY_DICT.get(label.lower())
        if identifier is None:
            logging.error(f"Unable to look up specimen value for label {label}")
        else:
            new_specimens.append(identifier)
    record_copy[SOLR_HAS_SPECIMEN_CATEGORY] = new_specimens

    new_contexts = []
    for label in current_contexts:
        if label.startswith("https://w3id.org"):
            # we've already touched it, bail
            return None
        identifier = CONTEXT_CATEGORY_DICT.get(label.lower())
        if identifier is None:
            logging.error(f"Unable to look up context value for label {label}")
        else:
            new_contexts.append(identifier)
    record_copy[SOLR_HAS_CONTEXT_CATEGORY] = new_contexts

    return record_copy


"""
Converts solr controlled vocabulary values from the libraries to the identifiers
"""
if __name__ == "__main__":
    main()
