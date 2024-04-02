import logging
from typing import Optional

import click
import requests
import term_store
from sqlmodel import Session

import isb_lib.core
import isb_web.config
import isb_lib.sesar_adapter
from isamples_metadata.Transformer import geo_to_h3, Transformer
from isb_lib.vocabulary import vocab_adapter
from isb_web.isb_solr_query import ISBCoreSolrRecordIterator
from isb_web.sqlmodel_database import SQLModelDAO

TOP_LEVEL_SPECIMEN_URI = "https://w3id.org/isample/vocabulary/specimentype/1.0/physicalspecimen"
TOP_LEVEL_CONTEXT_URI = "https://w3id.org/isample/vocabulary/sampledfeature/1.0/anysampledfeature"
TOP_LEVEL_MATERIAL_URI = "https://w3id.org/isample/vocabulary/material/1.0/material"




MATERIAL_CATEGORY_DICT = {
    "natural solid material": "https://w3id.org/isample/vocabulary/material/1.0/earthmaterial",
    "organic material":"https://w3id.org/isample/vocabulary/material/1.0/organicmaterial",
    "rock":"https://w3id.org/isample/vocabulary/material/1.0/rock",
    "sediment":"https://w3id.org/isample/vocabulary/material/1.0/sediment",
    "mixed soil":"https://w3id.org/isample/vocabulary/material/1.0/mixedsoilsedimentrock",
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
    session = SQLModelDAO(db_url).get_session()
    convert_to_controlled_vocabulary_identifiers(solr_url, session)


def convert_to_controlled_vocabulary_identifiers(solr_url: str, session: Session):
    repository = term_store.get_repository(session)
    specimen_dict = vocab_adapter.uijson_vocabulary_dict(TOP_LEVEL_SPECIMEN_URI, repository)
    context_dict = vocab_adapter.uijson_vocabulary_dict(TOP_LEVEL_CONTEXT_URI, repository)
    material_dict = vocab_adapter.uijson_vocabulary_dict(TOP_LEVEL_MATERIAL_URI, repository)
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


def _uri_for_label(label: str, solr_field: str) -> str:
    if label == "" or label == Transformer.NOT_PROVIDED:
        if solr_field == "hasMaterialCategory":
            return TOP_LEVEL_MATERIAL_URI
        elif solr_field == "hasSpecimenCategory":
            return TOP_LEVEL_SPECIMEN_URI
        else:
            return TOP_LEVEL_CONTEXT_URI
    else:


def mutate_record(record: dict) -> Optional[dict]:
    # Do whatever work is required to mutate the record to update things…
    record_copy = record.copy()
    # Remove old problematic fields
    current_material = record.pop("hasMaterialCategory")
    current_specimen = record.pop("hasSpecimenCategory")
    current_context = record.pop("hasContextCategory")

    if current_context is None or current_context == Transformer.NOT_PROVIDED
        record["hasMaterialCategory"] =

    for index in range(0, 16):
        h3_at_resolution = geo_to_h3(
            record.get("producedBy_samplingSite_location_latitude"),
            record.get("producedBy_samplingSite_location_longitude"),
            index,
        )
        field_name = f"producedBy_samplingSite_location_h3_{index}"
        record_copy[field_name] = h3_at_resolution
    return record_copy


"""
Converts solr controlled vocabulary values from the libraries to the identifiers
"""
if __name__ == "__main__":
    main()
