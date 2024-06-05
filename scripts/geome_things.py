import datetime
import logging
import time
import requests
import sqlalchemy
import sqlalchemy.orm
import sqlalchemy.exc
import isb_lib.core
import isb_lib.geome_adapter
import asyncio
import concurrent.futures
import click
import click_config_file

from isamples_metadata.GEOMETransformer import GEOMETransformer
from isb_lib import geome_adapter
from isb_lib.core import MEDIA_JSON
from isb_lib.geome_adapter import GEOMEIdentifierIterator
from isb_lib.models.thing import Thing
from isb_web import sqlmodel_database
from isb_web.sqlmodel_database import SQLModelDAO, get_thing_with_id, save_thing, all_thing_primary_keys, \
    DatabaseBulkUpdater, get_things_with_ids
from typing import Optional

CONCURRENT_DOWNLOADS = 10
BACKLOG_SIZE = 40


def getLogger():
    return logging.getLogger("main")


def wrapLoadThing(ark: str, tc: datetime.datetime, existing_thing: Optional[Thing] = None):
    """Return request information to assist future management"""
    try:
        return ark, tc, isb_lib.geome_adapter.loadThing(ark, tc, existing_thing)
    except Exception:
        pass
    return ark, tc, None


def countThings(session):
    """Return number of things already collected in database"""
    cnt = session.query(Thing).count()
    return cnt


async def _loadGEOMEEntries(session, max_count, start_from=None):  # noqa: C901 -- need to examine computational complexity
    L = getLogger()
    futures = []
    working = {}
    ids = isb_lib.geome_adapter.GEOMEIdentifierIterator(
        max_entries=countThings(session) + max_count, date_start=start_from
    )
    # i = 0
    # for id in ids:
    #    print(f"{i:05} {id}")
    #    i += 1
    #    if i > max_count:
    #        break
    # print(f"Counted total of {i}", i)
    # return

    total_requested = 0
    total_completed = 0
    more_work = True
    num_prepared = BACKLOG_SIZE  # Number of jobs to prepare for execution
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=CONCURRENT_DOWNLOADS
    ) as executor:
        while more_work:
            # populate the futures list with work until the list is full
            # or there is no more work to get.
            while (
                len(futures) < BACKLOG_SIZE
                and total_requested < max_count
                and num_prepared > 0
            ):
                try:
                    _id = next(ids)
                    identifier = _id[0]
                    existing_thing = get_thing_with_id(session, identifier)
                    if existing_thing is not None:
                        logging.debug("Already have %s at %s", identifier, _id[1])
                        future = executor.submit(wrapLoadThing, identifier, _id[1], existing_thing)
                    else:
                        future = executor.submit(wrapLoadThing, identifier, _id[1])
                    futures.append(future)
                    working[identifier] = 0
                    total_requested += 1
                except StopIteration:
                    L.info("Reached end of identifier iteration.")
                    num_prepared = 0
                if total_requested >= max_count:
                    num_prepared = 0
            L.debug("%s", working)
            try:
                for fut in concurrent.futures.as_completed(futures, timeout=1):
                    identifier, tc, _thing = fut.result()
                    futures.remove(fut)
                    if _thing is not None:
                        try:
                            save_thing(session, _thing)
                        except sqlalchemy.exc.IntegrityError:
                            session.rollback()
                            logging.error("Item already exists: %s", _id[0])
                        # for _rel in _related:
                        #    try:
                        #        session.add(_rel)
                        #        session.commit()
                        #    except sqlalchemy.exc.IntegrityError as e:
                        #        L.debug(e)
                        working.pop(identifier)
                        total_completed += 1
                    else:
                        if working.get(identifier, 0) < 3:
                            if identifier not in working:
                                working[identifier] = 1
                            else:
                                working[identifier] += 1
                            L.info(
                                "Failed to retrieve %s. Retry = %s",
                                identifier,
                                working[identifier],
                            )
                            future = executor.submit(wrapLoadThing, identifier, tc)
                            futures.append(future)
                        else:
                            L.error("Too many retries on %s", identifier)
                            working.pop(identifier)
            except concurrent.futures.TimeoutError:
                # L.info("No futures to process")
                pass
            if len(futures) == 0 and num_prepared == 0:
                more_work = False
            if total_completed >= max_count:
                more_work = False
            L.info(
                "requested, completed, current = %s, %s, %s",
                total_requested,
                total_completed,
                len(futures),
            )


def loadGEOMEEntries(session, max_count, start_from=None):
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(
        _loadGEOMEEntries(session, max_count, start_from=start_from)
    )
    loop.run_until_complete(future)


@click.group()
@click.option(
    "-d", "--db_url", default=None, help="SQLAlchemy database URL for storage"
)
@click.option(
    "-s", "--solr_url", default=None, help="Solr index URL"
)
@click.option(
    "-v", "--verbosity", default="INFO", help="Specify logging level", show_default=True
)
@click_config_file.configuration_option(config_file_name="sesar.cfg")
@click.pass_context
def main(ctx, db_url, solr_url, verbosity):
    isb_lib.core.things_main(ctx, db_url, solr_url, verbosity)


@main.command("load")
@click.option(
    "-m",
    "--max_records",
    type=int,
    default=1000,
    help="Maximum records to load, -1 for all",
)
@click.pass_context
def loadRecords(ctx, max_records):
    L = getLogger()
    L.info("loadRecords, max = %s", max_records)
    if max_records == -1:
        max_records = 999999999

    session = SQLModelDAO((ctx.obj["db_url"])).get_session()
    try:
        max_created = sqlmodel_database.last_time_thing_created(
            session, isb_lib.geome_adapter.GEOMEItem.AUTHORITY_ID
        )
        logging.info("Oldest = %s", max_created)
        time.sleep(1)
        loadGEOMEEntries(session, max_records, start_from=max_created)
    finally:
        session.close()


@main.command("reparse")
@click.pass_context
def reparseRecords(ctx):
    raise NotImplementedError("reparseRecords")

    def _yieldRecordsByPage(qry, pk):
        nonlocal session
        offset = 0
        page_size = 5000
        while True:
            q = qry
            rec = None
            n = 0
            for rec in q.order_by(pk).offset(offset).limit(page_size):
                n += 1
                yield rec
            if n == 0:
                break
            offset += page_size

    L = getLogger()
    batch_size = 50
    L.info("reparseRecords with batch size: %s", batch_size)
    session = SQLModelDAO((ctx.obj["db_url"])).get_session()
    try:
        i = 0
        qry = session.query(Thing)
        pk = Thing.id
        for thing in _yieldRecordsByPage(qry, pk):
            itype = thing.item_type
            isb_lib.geome_adapter.reparseThing(thing, and_relations=False)
            L.info("%s: reparse %s, %s -> %s", i, thing.id, itype, thing.item_type)
            i += 1
            if i % batch_size == 0:
                session.commit()
        # don't forget to commit the remainder!
        session.commit()
    finally:
        session.close()


@main.command("relations")
@click.pass_context
def reparseRelations(ctx):
    def _yieldRecordsByPage(qry, pk):
        nonlocal session
        offset = 0
        page_size = 5000
        while True:
            q = qry
            rec = None
            n = 0
            for rec in q.order_by(pk).offset(offset).limit(page_size):
                n += 1
                yield rec
            if n == 0:
                break
            offset += page_size

    L = getLogger()
    rsession = requests.session()
    batch_size = 1000
    L.info("reparseRecords with batch size: %s", batch_size)
    session = SQLModelDAO(ctx.obj["db_url"]).get_session()
    allkeys = set()
    try:
        i = 0
        n = 0
        qry = session.query(Thing).filter(
            Thing.authority_id
            == isb_lib.geome_adapter.GEOMEItem.AUTHORITY_ID
        )
        pk = Thing.id
        relations = []
        for thing in _yieldRecordsByPage(qry, pk):
            batch = isb_lib.geome_adapter.reparseRelations(thing)
            relations = relations + batch
            for relation in relations:
                allkeys.add(relation["id"])
            _rel_len = len(relations)
            n += len(batch)
            if i % 25 == 0:
                L.info(
                    "%s: relations id:%s num_rel:%s, total:%s", i, thing.id, _rel_len, n
                )
            if _rel_len > batch_size:
                isb_lib.core.solrAddRecords(rsession, relations, "http://localhost:8983/solr/isb_rel/")
                relations = []
            i += 1
        # don't forget to add the remainder!
        isb_lib.core.solrAddRecords(rsession, relations, "http://localhost:8983/solr/isb_rel/")
        L.info("%s: relations num_rel:%s, total:%s", i, len(relations), n)
        print(f"Total keys= {len(allkeys)}")
        isb_lib.core.solrCommit(rsession, "http://localhost:8983/solr/isb_rel/")
    finally:
        session.close()


@main.command("reload")
@click.option(
    "-s",
    "--status",
    "status_code",
    type=int,
    default=500,
    help="HTTP status of records to reload",
)
@click.pass_context
def reloadRecords(ctx, status_code):
    raise NotImplementedError("reloadRecords")
    L = getLogger()
    L.info("reloadRecords, status_code = %s", status_code)
    session = SQLModelDAO(ctx.obj["db_url"]).get_session()
    try:
        pass

    finally:
        session.close()


@main.command("populate_isb_core_solr")
@click.option(
    "-I", "--ignore_last_modified", is_flag=True, help="Whether to ignore the last modified date and do a full rebuild"
)
@click.pass_context
def populateIsbCoreSolr(ctx, ignore_last_modified: bool):
    logger = getLogger()
    db_url = ctx.obj["db_url"]
    solr_url = ctx.obj["solr_url"]
    if ignore_last_modified:
        max_solr_updated_date = None
    else:
        max_solr_updated_date = isb_lib.core.solr_max_source_updated_time(
            url=solr_url,
            authority_id=isb_lib.geome_adapter.GEOMEItem.AUTHORITY_ID,
        )
    logger.info(f"Going to index Things with tcreated > {max_solr_updated_date}")
    solr_importer = isb_lib.core.CoreSolrImporter(
        db_url=db_url,
        authority_id=isb_lib.geome_adapter.GEOMEItem.AUTHORITY_ID,
        db_batch_size=1000,
        solr_batch_size=1000,
        solr_url=solr_url,
        min_time_created=max_solr_updated_date
    )
    allkeys = solr_importer.run_solr_import(isb_lib.geome_adapter.reparseAsCoreRecord)
    logger.info(f"Total keys= {len(allkeys)}")


@main.command("harvest_local_contexts")
@click.option(
    "-b", "--batch_size", type=int, default=10000, help="Batch size for database commits"
)
@click.pass_context
def harvest_local_contexts(cts, batch_size: int):
    session = SQLModelDAO(cts.obj["db_url"]).get_session()
    primary_keys_by_id = all_thing_primary_keys(session, geome_adapter.GEOMEItem.AUTHORITY_ID)
    bulk_updater = DatabaseBulkUpdater(session, geome_adapter.GEOMEItem.AUTHORITY_ID, batch_size, MEDIA_JSON, primary_keys_by_id)
    identifier_iterator = GEOMEIdentifierIterator()
    for project_dict in identifier_iterator.listProjects():
        thing_identifiers_for_project = []
        local_contexts_id = project_dict.get("localcontextsId")
        if local_contexts_id is not None:
            # If the project has a localcontextsId, we need to propagate down to all records in the project
            print(f"Found localcontextsId for project {project_dict}")
            project_id = project_dict.get("projectId")
            count = 0
            for record in identifier_iterator.recordsInProject(project_id, identifier_iterator._record_type, local_contexts_id):
                count += 1
                identifier = record[0].get("bcid", None)
                thing_identifiers_for_project.append(identifier)
            # Now refetch all the things with specified identifier and stuff in the localcontextsId into resolved_content
            things_for_project = get_things_with_ids(session, thing_identifiers_for_project)
            for thing in things_for_project:
                thing.resolved_content["localContextsId"] = local_contexts_id
                bulk_updater.add_thing(thing.resolved_content, thing.id, thing.resolved_url, thing.resolved_status,
                                       thing.h3, thing.tcreated)

            #
            project_title = project_dict.get("projectTitle")
            print(f"{count} records in project {project_title}")
            bulk_updater.finish()
        else:
            pass



if __name__ == "__main__":
    main()
