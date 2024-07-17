import concurrent.futures
import datetime
import json
import typing
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from typing import Iterator

import click
import requests

import isb_lib
import isb_lib.core
import isb_web
import isb_web.config
import logging
import re

from isb_lib.models.thing import Thing
from isb_lib.sitemaps.sitemap_fetcher import (
    SitemapIndexFetcher,
    SitemapFileFetcher,
    ThingFetcher,
    ThingsJSONLinesFetcher,
)
from isb_web import sqlmodel_database
from isb_web.sqlmodel_database import (
    SQLModelDAO,
    all_thing_identifiers,
    thing_identifiers_from_resolved_content,
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
    fetch_sitemap_files(
        authority,
        last_updated_date,
        thing_ids_to_pks,
        rsession,
        url,
        db_session,
        batch_size,
        file,
        start
    )
    logging.info(f"Completed at {datetime.datetime.now()}.  Fetched {__NUM_THINGS_FETCHED} things total.")


def thing_fetcher_for_url(thing_url: str, rsession) -> ThingFetcher:
    # At this point, we need to massage the URLs a bit, the sitemap publishes them like so:
    # https://mars.cyverse.org/thing/ark:/21547/DxI2SKS002?full=false&amp;format=core
    # We need to change full to true to get all the metadata, as well as the original format
    parsed_url = urllib.parse.urlparse(thing_url)
    parsed_url = parsed_url._replace(query="full=true&format=original")
    thing_fetcher = ThingFetcher(parsed_url.geturl(), rsession)
    logging.info(f"Constructed ThingFetcher for {parsed_url.geturl()}")
    return thing_fetcher


THING_URL_REGEX = re.compile(r"(.*)/thing/([^?]+)?")


def _group_from_thing_url_regex(thing_url: str, group: int) -> typing.Optional[str]:
    match = THING_URL_REGEX.match(thing_url)
    if match is None:
        logging.critical(f"Didn't find match in thing URL {thing_url}")
        return None
    else:
        group_str = match.group(group)
        return group_str


def thing_identifier_from_thing_url(thing_url: str) -> typing.Optional[str]:
    # At this point, we need to massage the URLs a bit, the sitemap publishes them like so:
    # https://mars.cyverse.org/thing/ark:/21547/DxI2SKS002?full=false&amp;format=core
    # We need to change full to true to get all the metadata, as well as the original format
    return _group_from_thing_url_regex(thing_url, 2)


def pre_thing_host_url(thing_url: str) -> typing.Optional[str]:
    # At this point, we need to parse out the the URL a bit, the sitemap publishes them like so:
    # https://mars.cyverse.org/thing/ark:/21547/DxI2SKS002?full=false&amp;format=core
    # We need to grab the part of the URL before thing (https://mars.cyverse.org/) and change it to
    # https://mars.cyverse.org/things to do the bulk fetch
    return _group_from_thing_url_regex(thing_url, 1)


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


def fetch_sitemap_files(
    authority,
    last_updated_date,
    thing_ids: typing.Dict[str, int],
    rsession,
    url,
    db_session,
    batch_size: int,
    file: str,
    start: int
):
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
                        thing_dict = {}
                        thing_identifier = json_dict["sample_identifier"]
                        thing_dict["resolved_content"] = json_dict
                        now = datetime.datetime.now()
                        thing_dict["tstamp"] = now
                        thing_dict["identifiers"] = json.dumps([thing_identifier])
                        thing_dict["id"] = thing_identifier
                        thing_dict["authority_id"] = json_dict["source_collection"]
                        thing_dict["resolved_url"] = things_fetcher.json_lines_url
                        thing_dict["resolved_status"] = 200
                        thing_dict["tresolved"] = time_fetched
                        thing_dict["resolved_media_type"] = "application/jsonl"
                        # TODO: h3
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


if __name__ == "__main__":
    main()
