import logging
from typing import Optional

import click
import click_config_file
import requests
from sqlmodel import select, Session

import isb_lib.core
import isb_web.config
import isb_lib.sesar_adapter
from isb_lib.models.thing import Thing
from isb_web.isb_solr_query import ISBCoreSolrRecordIterator
from isb_web.sqlmodel_database import SQLModelDAO


id_map = []


@click.command()
@click_config_file.configuration_option(config_file_name="isb.cfg")
@click.pass_context
def main(ctx):
    global id_map
    solr_url = isb_web.config.Settings().solr_url
    db_url = isb_web.config.Settings().database_url
    isb_lib.core.things_main(ctx, db_url, solr_url)
    session = SQLModelDAO((ctx.obj["db_url"]), echo=True).get_session()
    id_map = thing_id_to_h3(session)
    add_h3_values(solr_url)


def add_h3_values(solr_url: str):
    total_records = 0
    batch_size = 50000
    current_mutated_batch = []
    rsession = requests.session()
    iterator = ISBCoreSolrRecordIterator(
        rsession, "-(producedBy_samplingSite_location_h3:*) AND -(_nest_path_:*)", batch_size, 0, "id asc"
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
    record_id = record["id"]
    if record_id not in id_map:
        return None
    record_copy = record.copy()
    record_copy["producedBy_samplingSite_location_h3"] = id_map[record_id]
    if "h3" in record_copy:
        record_copy.pop("h3")
    return record_copy


def thing_id_to_h3(session: Session) -> dict:
    thing_select = (
        select(Thing.id, Thing.h3)
        .filter(Thing.h3 is not None)  # type: ignore
    )
    results = session.exec(thing_select).all()
    id_to_h3 = {}
    for row in results:
        id_to_h3[row[0]] = row[1]
    return id_to_h3


"""
Adds h3 values to an existing solr index that may have been partially initialized (looking at you, mars…)
"""
if __name__ == "__main__":
    main()
