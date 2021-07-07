import logging
import click
import click_config_file
import asyncio
import isb_lib.core
import isb_lib.opencontext_adapter
import igsn_lib.models
import concurrent.futures
import heartrate
import sqlalchemy
import sqlalchemy.orm
import sqlalchemy.exc

LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "WARN": logging.WARNING,
    "ERROR": logging.ERROR,
    "FATAL": logging.CRITICAL,
    "CRITICAL": logging.CRITICAL,
}
LOG_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
LOG_FORMAT = "%(asctime)s %(name)s:%(levelname)s: %(message)s"
BACKLOG_SIZE = 40


def get_logger():
    return logging.getLogger("main")

def wrap_load_thing(thing_dict, tc):
    """Return request information to assist future management"""
    try:
        return tc, isb_lib.opencontext_adapter.load_thing(thing_dict)
    except:
        pass
    return tc, None


async def _load_open_context_entries(session, max_count, start_from):
    L = get_logger()
    futures = []
    ids = isb_lib.opencontext_adapter.OpenContextIdentifierIterator(
        max_entries=10, date_start=start_from
    )

    num_ids = 0
    for _id in ids:
        L.info("got next id from open context %s", _id)
        num_ids += 1
        try:
            res = (
                session.query(igsn_lib.models.thing.Thing.id)
                    .filter_by(id=_id["uri"])
                    .one()
            )
            logging.info("Already have %s", _id["uri"])
        except sqlalchemy.orm.exc.NoResultFound:
            wrap_load_thing(_id, None)

    L.info("total num ids %d", num_ids)

    # total_requested = 0
    # total_completed = 0
    # more_work = True
    # num_prepared = BACKLOG_SIZE  # Number of jobs to prepare for execution
    # with concurrent.futures.ThreadPoolExecutor(
    #     max_workers=1
    # ) as executor:
    #     while more_work:
    #         while (
    #             len(futures) < BACKLOG_SIZE
    #             and total_requested < max_count
    #             and num_prepared > 0
    #         ):
    #             _id = next(ids)
    #             print("got next id from open context %s", _id)


def load_open_context_entries(session, max_count, start_from=None):
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(
        _load_open_context_entries(session, max_count, start_from=start_from)
    )
    loop.run_until_complete(future)


def getDBSession(db_url):
    engine = igsn_lib.models.getEngine(db_url)
    igsn_lib.models.createAll(engine)
    session = igsn_lib.models.getSession(engine)
    return session


@click.group()
@click.option(
    "-d", "--db_url", default=None, help="SQLAlchemy database URL for storage"
)
@click.option(
    "-v", "--verbosity", default="DEBUG", help="Specify logging level", show_default=True
)
@click.option(
    "-H", "--heart_rate", is_flag=True, help="Show heartrate diagnositcs on 9999"
)
@click_config_file.configuration_option(config_file_name="/Users/mandeld/iSamples/isamples_inabox/isb.cfg")
@click.pass_context
def main(ctx, db_url, verbosity, heart_rate):
    ctx.ensure_object(dict)
    verbosity = verbosity.upper()
    logging.basicConfig(
        level=LOG_LEVELS.get(verbosity, logging.DEBUG),
        format=LOG_FORMAT,
        datefmt=LOG_DATE_FORMAT,
    )
    L = get_logger()
    if verbosity not in LOG_LEVELS.keys():
        L.warning("%s is not a log level, set to INFO", verbosity)

    L.info("Using database at: %s", db_url)
    ctx.obj["db_url"] = db_url
    if heart_rate:
        heartrate.trace(browser=True)


@main.command("load")
@click.option(
    "-m",
    "--max_records",
    type=int,
    default=1000,
    help="Maximum records to load, -1 for all",
)
@click.pass_context
def load_records(ctx, max_records):
    L = get_logger()
    session = getDBSession(ctx.obj["db_url"])
    L.info("loadRecords: %s", str(session))
    # ctx.obj["db_url"] = db_url
    load_open_context_entries(session, 0, None)


if __name__ == "__main__":
    main()