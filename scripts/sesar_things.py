import logging
import time
import requests
import sqlalchemy
import sqlalchemy.orm
import sqlalchemy.exc
import igsn_lib
import isb_lib.core
import isb_lib.sesar_adapter
import igsn_lib.time
import asyncio
import concurrent.futures
import click
import click_config_file
import typing

from isb_lib.models.thing import Thing
from isb_web import sqlmodel_database
from isb_web.sqlmodel_database import SQLModelDAO, save_thing
from isamples_metadata.SESARTransformer import fullIgsn
from typing import Optional

CONCURRENT_DOWNLOADS = 10
BACKLOG_SIZE = 40


def getLogger():
    return logging.getLogger("main")


def wrapLoadThing(igsn, tc, existing_thing: Optional[Thing] = None):
    """Return request information to assist future management"""
    try:
        return igsn, tc, isb_lib.sesar_adapter.loadThing(igsn, tc, existing_thing)
    except Exception:
        pass
    return igsn, tc, None


def countThings(session):
    """Return number of things already collected in database"""
    cnt = session.query(Thing).count()
    return cnt


async def _loadSesarEntries(session, max_count, start_from=None, manual_ids: Optional[typing.List[typing.List[str]]] = None):  # noqa: C901 -- need to examine computational complexity
    L = getLogger()
    futures: list = []
    working = {}
    if manual_ids is not None:
        ids = iter(manual_ids)
    else:
        ids = isb_lib.sesar_adapter.SESARIdentifiersSitemap(
            max_entries=countThings(session) + max_count, date_start=start_from
        )
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
                    igsn = igsn_lib.normalize(_id[0])
                    existing_thing = sqlmodel_database.get_thing_with_id(session, fullIgsn(igsn))
                    if existing_thing is not None:
                        logging.info("Already have %s at %s", igsn, existing_thing)
                        future = executor.submit(wrapLoadThing, igsn, _id[1], existing_thing)
                    else:
                        future = executor.submit(wrapLoadThing, igsn, _id[1])
                    futures.append(future)
                    working[igsn] = 0
                    total_requested += 1
                except StopIteration:
                    L.info("Reached end of identifier iteration.")
                    num_prepared = 0
                if total_requested >= max_count:
                    num_prepared = 0
            L.debug("%s", working)
            try:
                for fut in concurrent.futures.as_completed(futures, timeout=1):
                    igsn, tc, _thing = fut.result()
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
                        working.pop(igsn)
                        total_completed += 1
                    else:
                        if working.get(igsn, 0) < 3:
                            if igsn not in working:
                                working[igsn] = 1
                            else:
                                working[igsn] += 1
                            L.info(
                                "Failed to retrieve %s. Retry = %s", igsn, working[igsn]
                            )
                            future = executor.submit(wrapLoadThing, igsn, tc)
                            futures.append(future)
                        else:
                            L.error("Too many retries on %s", igsn)
                            working.pop(igsn)
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


def loadSesarEntries(session, max_count, start_from=None, manual_ids: Optional[typing.List[typing.List[str]]] = None):
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(
        _loadSesarEntries(session, max_count, start_from=start_from, manual_ids=manual_ids)
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
    session = SQLModelDAO(ctx.obj["db_url"]).get_session()
    try:
        oldest_record = sqlmodel_database.last_time_thing_created(
            session, isb_lib.sesar_adapter.SESARItem.AUTHORITY_ID
        )
        logging.info("Oldest = %s", oldest_record)
        time.sleep(1)
        loadSesarEntries(session, max_records, start_from=oldest_record)
    finally:
        session.close()


@main.command("manual_load")
@click.option(
    "-f",
    "--filename",
    type=str,
    help="The filename with the manual IGSN list to load.  Format should be .csv with first column as IGSN and second column as time created.",
)
@click.pass_context
def manual_load_records(ctx, filename):
    session = SQLModelDAO(ctx.obj["db_url"]).get_session()
    try:
        igsn_list = []
        with open(filename, "r") as igsn_file:
            for line in igsn_file:
                igsn_tc = line.split(",")
                igsn_list.append(igsn_tc)
        time.sleep(1)
        loadSesarEntries(session, 999999999, None, igsn_list)
    finally:
        session.close()


@main.command("reparse")
@click.pass_context
def reparseRecords(ctx):
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
    session = SQLModelDAO(ctx.obj["db_url"]).get_session()
    try:
        i = 0
        qry = session.query(Thing)
        pk = Thing.id
        for thing in _yieldRecordsByPage(qry, pk):
            itype = thing.item_type
            isb_lib.sesar_adapter.reparseThing(thing)
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
    L = getLogger()
    rsession = requests.session()
    batch_size = 5000
    L.info("reparseRecords with batch size: %s", batch_size)
    session = SQLModelDAO(ctx.obj["db_url"]).get_session()
    allkeys = set()
    try:
        i = 0
        n = 0
        relations = []
        thing_iterator = isb_lib.core.ThingRecordIterator(
            session,
            authority_id=isb_lib.sesar_adapter.SESARItem.AUTHORITY_ID,
            page_size=batch_size,
            offset=0,
        )
        for thing in thing_iterator.yieldRecordsByPage():
            batch = isb_lib.sesar_adapter.reparseRelations(thing, as_solr=True)
            relations = relations + batch
            for r in relations:
                allkeys.add(r["id"])
            _rel_len = len(relations)
            n += len(batch)
            if i % 25 == 0:
                L.info(
                    "%s: relations id:%s num_rel:%s, total:%s", i, thing.id, _rel_len, n
                )
            if _rel_len > batch_size:
                isb_lib.core.solrAddRecords(
                    rsession, relations, "http://localhost:8983/solr/isb_rel/"
                )
                relations = []
            i += 1
        isb_lib.core.solrAddRecords(
            rsession, relations, "http://localhost:8983/solr/isb_rel/"
        )
        isb_lib.core.solrCommit(rsession, "http://localhost:8983/solr/isb_rel/")
        print(f"Total keys= {len(allkeys)}")
        # verify records
        # for verifying that all records were added to solr
        # found = 0
        # for _id in allkeys:
        #    res = rsession.get(f"http://localhost:8983/solr/isb_rel/get?id={_id}").json()
        #    if res.get("doc",{}).get("id") == _id:
        #        found = found +1
        #    else:
        #        print(f"Missed: {_id}")
        # print(f"Found = {found}")
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
    L = getLogger()
    L.info("reloadRecords, status_code = %s", status_code)
    raise NotImplementedError("reloadRecords")
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
    L = getLogger()
    db_url = ctx.obj["db_url"]
    solr_url = ctx.obj["solr_url"]
    if ignore_last_modified:
        max_solr_updated_date = None
    else:
        max_solr_updated_date = isb_lib.core.solr_max_source_updated_time(
            url=solr_url,
            authority_id=isb_lib.sesar_adapter.SESARItem.AUTHORITY_ID,
        )
    L.info(f"Going to index Things with tcreated > {max_solr_updated_date}")
    solr_importer = isb_lib.core.CoreSolrImporter(
        db_url=db_url,
        authority_id=isb_lib.sesar_adapter.SESARItem.AUTHORITY_ID,
        db_batch_size=1000,
        solr_batch_size=1000,
        solr_url=solr_url,
        min_time_created=max_solr_updated_date
    )
    allkeys = solr_importer.run_solr_import(isb_lib.sesar_adapter.reparseAsCoreRecord)
    L.info(f"Total keys= {len(allkeys)}")


if __name__ == "__main__":
    main()
