import concurrent.futures
import datetime
import json
import typing
from concurrent.futures import ThreadPoolExecutor
from typing import Iterator

import click
import requests

import isamples_metadata
import isb_lib
import isb_lib.core
import isb_web
import isb_web.config
import logging

from isamples_metadata.Transformer import Transformer
from isamples_metadata.metadata_constants import METADATA_SAMPLE_IDENTIFIER, METADATA_PRODUCED_BY, \
    METADATA_SAMPLING_SITE, METADATA_SAMPLE_LOCATION, METADATA_LATITUDE, METADATA_LONGITUDE
from isb_lib.core import MEDIA_JSONL
from isb_lib.models.thing import Thing
from isb_lib.sitemaps.sitemap_fetcher import (
    SitemapIndexFetcher,
    SitemapFileFetcher,
    ThingsJSONLinesFetcher,
)
from isb_web import sqlmodel_database
from isb_web.sqlmodel_database import (
    SQLModelDAO,
    all_thing_identifiers
)

__NUM_THINGS_FETCHED = 0


@click.command()
@click.pass_context
@click.option(
    "-u",
    "--url",
    type=str,
    default=None,
    help="The URL to the sitemap index file to consume",
)
@click.option(
    "-a",
    "--authority",
    type=str,
    default=None,
    help="The authority used for storing the retrieved records",
)
@click.option(
    "-i",
    "--ignore_last_modified",
    is_flag=True,
    help="Whether to ignore the last modified date and do a full rebuild",
)
@click.option(
    "-b",
    "--batch_size",
    default=1,
    help="The max number of JSON lines files to fetch at a time",
)
@click.option(
    "-f",
    "--file",
    default=None,
    help="If specified, the name of the sitemap file to ingest",
)
@click.option(
    "-s",
    "--start",
    default=-1,
    help="If specified, the start index of the sitemap files to ingest",
)
def main(ctx, url: str, authority: str, ignore_last_modified: bool, batch_size: int, file: str, start: int):
    logging.info(f"Started sitemap consumption at {datetime.datetime.now()}")
    solr_url = isb_web.config.Settings().solr_url
    rsession = requests.session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=5, pool_maxsize=5)
    rsession.mount("http://", adapter)
    rsession.mount("https://", adapter)
    db_url = isb_web.config.Settings().database_url
    db_session = SQLModelDAO(db_url).get_session()
    if authority is not None:
        authority = authority.upper()
    isb_lib.core.things_main(ctx, db_url, solr_url, "INFO")
    if ignore_last_modified:
        last_updated_date = None
    else:
        # Smithsonian's dump has dates marked in the future.  So, Smithsonian will never update.  For the purposes
        # of iSamples Central, this is actually ok as we don't have an automated import pipeline for Smithsonian.
        # Once the Smithsonian gets an automated import time in place, we'll need to address this somehow.
        # https://github.com/isamplesorg/isamples_inabox/issues/110
        last_updated_date = sqlmodel_database.last_time_thing_created(
            db_session, authority
        )
    thing_ids_to_pks = all_thing_identifiers(db_session, authority)
    logging.info(
        f"Going to fetch records for authority {authority} with updated date > {last_updated_date}"
    )
    fetch_sitemap_files(authority, last_updated_date, thing_ids_to_pks, rsession, url, db_session, file, start)
    logging.info(f"Completed at {datetime.datetime.now()}.  Fetched {__NUM_THINGS_FETCHED} things total.")


def construct_thing_futures(
    thing_futures: list,
    sitemap_file_iterator: Iterator,
    sitemap_file_url: str,
    rsession: requests.Session,
    thing_executor: ThreadPoolExecutor
) -> bool:
    constructed_all_futures_for_sitemap_file = False
    while not constructed_all_futures_for_sitemap_file:
        try:
            thing_json_lines_url = next(sitemap_file_iterator)
            things_fetcher = ThingsJSONLinesFetcher(
                thing_json_lines_url, sitemap_file_url, rsession
            )
            things_future = thing_executor.submit(things_fetcher.fetch_things)
            thing_futures.append(things_future)
        except StopIteration:
            constructed_all_futures_for_sitemap_file = True
    return constructed_all_futures_for_sitemap_file


def fetch_sitemap_files(authority, last_updated_date, thing_ids: typing.Dict[str, int], rsession, url, db_session,
                        file: str, start: int):
    sitemap_index_fetcher = SitemapIndexFetcher(
        url, authority, last_updated_date, rsession
    )
    # fetch the index file, and iterate over the individual sitemap files serially so we preserve order
    sitemap_index_fetcher.fetch_index_file()
    num_files = 0
    for url in sitemap_index_fetcher.urls_to_fetch:
        num_files += 1
        if file is not None and file not in url:
            # there's a specific sitemap file specified, and this file isn't it, so continue on our way
            # we expect file to be something like "sitemap-81.xml"
            continue
        if num_files < start:
            # we've specified a start file (e.g. 81) and we are less than the start (e.g. sitemap-1.xml), so continue
            continue
        sitemap_file_fetcher = SitemapFileFetcher(
            url, authority, last_updated_date, rsession
        )
        sitemap_file_fetcher.fetch_sitemap_file()
        sitemap_file_iterator = sitemap_file_fetcher.url_iterator()
        thing_futures: list = []
        # max_workers is 1 so that we may execute one request at a time in order to avoid overwhelming the server
        # We still gain benefits as the next things request executes while the local processing is occurring
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as thing_executor:
            construct_thing_futures(
                thing_futures,
                sitemap_file_iterator,
                sitemap_file_fetcher.url,
                rsession,
                thing_executor
            )
            # Then read out results and save to the database after the queue is filled to capacity.
            # Provided there are more urls in the iterator, return to the top of the loop to fill the queue again
            for thing_fut in concurrent.futures.as_completed(thing_futures):
                time_fetched = datetime.datetime.now()
                things_fetcher = thing_fut.result()
                if (
                    things_fetcher is not None
                    and things_fetcher.json_dicts is not None
                ):
                    global __NUM_THINGS_FETCHED
                    __NUM_THINGS_FETCHED += len(things_fetcher.json_dicts)
                    logging.info(
                        f"About to process {len(things_fetcher.json_dicts)} things"
                    )
                    current_existing_things_batch = []
                    current_new_things_batch = []

                    for json_dict in things_fetcher.json_dicts:
                        now = datetime.datetime.now()
                        json_lines_url = things_fetcher.json_lines_url
                        thing_dict, thing_identifier = _json_line_to_thing_dict(json_dict, json_lines_url, now,
                                                                                time_fetched)

                        if thing_identifier in thing_ids.keys():
                            # existing row in the db, for the update to work we need to insert the pk into the dict
                            thing_dict["primary_key"] = thing_ids[thing_identifier]
                            current_existing_things_batch.append(thing_dict)
                        else:
                            thing_dict["tcreated"] = now
                            current_new_things_batch.append(thing_dict)
                    db_session.bulk_insert_mappings(
                        mapper=Thing,
                        mappings=current_new_things_batch,
                        return_defaults=False,
                    )
                    db_session.bulk_update_mappings(
                        mapper=Thing, mappings=current_existing_things_batch
                    )
                    db_session.commit()
                    logging.info(
                        f"Just processed {len(things_fetcher.json_dicts)} things"
                    )
                else:
                    logging.error(f"Error fetching thing for {things_fetcher.url}")
                thing_futures.remove(thing_fut)


def _json_line_to_thing_dict(json_dict: dict, json_lines_url: str, now: datetime.datetime, time_fetched: datetime.datetime) -> tuple[dict, str]:
    thing_dict: dict[str, typing.Any] = {}
    thing_identifier = json_dict[METADATA_SAMPLE_IDENTIFIER]
    thing_dict["resolved_content"] = json_dict
    thing_dict["tstamp"] = now
    thing_dict["identifiers"] = json.dumps([thing_identifier])
    thing_dict["id"] = thing_identifier
    thing_dict["authority_id"] = json_dict["source_collection"]
    thing_dict["resolved_url"] = json_lines_url
    thing_dict["resolved_status"] = 200
    thing_dict["tresolved"] = time_fetched
    thing_dict["resolved_media_type"] = MEDIA_JSONL
    thing_dict["tcreated"] = json_dict["last_modified_time"]
    produced_by = json_dict.get(METADATA_PRODUCED_BY)
    if produced_by is not None:
        sampling_site = produced_by.get(METADATA_SAMPLING_SITE)
        if sampling_site is not None:
            sample_location = sampling_site.get(METADATA_SAMPLE_LOCATION)
            if sample_location is not None:
                h3 = isamples_metadata.Transformer.geo_to_h3(sample_location.get(METADATA_LATITUDE),
                                                             sample_location.get(METADATA_LONGITUDE),
                                                             Transformer.DEFAULT_H3_RESOLUTION)
                if h3 is not None:
                    thing_dict["h3"] = h3
    return thing_dict, thing_identifier


if __name__ == "__main__":
    main()
