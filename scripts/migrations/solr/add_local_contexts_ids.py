import logging
from typing import Optional

import click
import click_config_file
import requests
from sqlmodel import Session

import isb_lib.core
import isb_web.config
import isb_lib.sesar_adapter
from isamples_metadata.GEOMETransformer import GEOMETransformer
from isb_lib.models.thing import Thing
from isb_web.isb_solr_query import ISBCoreSolrRecordIterator
from isb_web.sqlmodel_database import SQLModelDAO, paged_things_with_ids


@click.command()
@click_config_file.configuration_option(config_file_name="isb.cfg")
@click.pass_context
def main(ctx):
    solr_url = isb_web.config.Settings().solr_url
    db_url = isb_web.config.Settings().database_url
    isb_lib.core.things_main(ctx, db_url, solr_url)
    session = SQLModelDAO((ctx.obj["db_url"]), echo=True).get_session()
    geome_things_with_local_contexts_ids = fetch_geome_things_with_local_contexts_id(session)
    add_local_contexts_ids(solr_url, geome_things_with_local_contexts_ids)


def add_local_contexts_ids(solr_url: str, geome_things: dict[str, Thing]):
    total_records = 0
    batch_size = 50000
    current_mutated_batch = []
    rsession = requests.session()
    iterator = ISBCoreSolrRecordIterator(
        rsession, "source:GEOME", batch_size, 0, "id asc"
    )
    for record in iterator:
        identifier = record.get("id")
        if identifier not in geome_things:
            continue
        geome_thing = geome_things.get(identifier)
        mutated_record = mutate_record(record, geome_thing)  # type: ignore
        if mutated_record is not None:
            current_mutated_batch.append(mutated_record)
        if len(current_mutated_batch) == 1:
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


def mutate_record(record: dict, geome_thing: Thing) -> Optional[dict]:
    assert geome_thing.resolved_content is not None
    local_contexts_id = geome_thing.resolved_content.get("localContextsId")
    if local_contexts_id is None:
        return None
    else:
        complies_with_list = GEOMETransformer.complies_with_list_for_local_contexts_id(local_contexts_id)
        record_copy = record.copy()
        record_copy["compliesWith"] = complies_with_list
        return record_copy


def fetch_geome_things_with_local_contexts_id(session: Session) -> dict[str, Thing]:
    geome_things_with_local_contexts_ids: dict[str, Thing] = {}
    all_geome_things = paged_things_with_ids(session, "GEOME", limit=1000000000000)
    for current_geome_thing in all_geome_things:
        assert current_geome_thing.resolved_content is not None
        local_contexts_id = current_geome_thing.resolved_content.get("localContextsId")
        if local_contexts_id is not None:
            assert current_geome_thing.id is not None
            geome_things_with_local_contexts_ids[current_geome_thing.id] = current_geome_thing
    return geome_things_with_local_contexts_ids


"""
Populates local_context_ids for existing GEOME records
"""
if __name__ == "__main__":
    main()
