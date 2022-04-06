import datetime
import typing

import sqlalchemy
import logging
from typing import Optional, List

from sqlalchemy import Index, update
from sqlalchemy.exc import ProgrammingError
from sqlmodel import SQLModel, create_engine, Session, select
from sqlmodel.sql.expression import SelectOfScalar

import isb_lib
from isb_lib.models.thing import Thing, ThingIdentifier
from isb_web.schemas import ThingPage


class SQLModelDAO:
    def __init__(self, db_url: str, echo: bool = False):
        # This is a strange initializer, but FastAPI wants us to construct the object before we know we
        # want to use it.  So, separate out the object construction from the database connection.
        # In unit tests, this ends up getting swapped out and unused, which is the source of the confusion.
        if db_url is not None:
            self.connect_sqlmodel(db_url, echo)
        else:
            self.engine = None

    # Utility method to attempt to create an index but catch an exception if it already exists
    def _create_index(self, index: Index):
        try:
            index.create(self.engine)
        except ProgrammingError:
            pass

    def connect_sqlmodel(self, db_url: str, echo: bool = False):
        self.engine = create_engine(db_url, echo=echo)
        SQLModel.metadata.create_all(self.engine)
        # There doesn't appear to be a SQLModel-native way of creating those, so fall back to SQLAlchemy
        id_resolved_status_authority_id_idx = Index(
            "_id_resolved_status_authority_id_idx",
            Thing.primary_key,
            Thing.resolved_status,
            Thing.authority_id,
        )
        # These index creations will throw if they already exist -- there's nothing to do in that case
        self._create_index(id_resolved_status_authority_id_idx)

        authority_id_tcreated_idx = Index(
            "authority_id_tcreated_idx", Thing.authority_id, Thing.tcreated
        )
        self._create_index(authority_id_tcreated_idx)

        item_type_status_idx = Index(
            "item_type_status_idx", Thing.item_type, Thing.resolved_status
        )
        self._create_index(item_type_status_idx)

        resolved_status_authority_id_idx = Index(
            "resolved_status_authority_id_idx",
            Thing.resolved_status,
            Thing.authority_id,
        )
        self._create_index(resolved_status_authority_id_idx)

        # guid_thing_id_idx = Index(
        #     "guid_thing_id_idx", ThingIdentifier.guid, ThingIdentifier.thing_id
        # )
        # self._create_index(guid_thing_id_idx)

    def get_session(self) -> Session:
        return Session(self.engine)


def read_things_summary(
    session: Session,
    offset: int,
    limit: int = 100,
    status: int = 200,
    authority: Optional[str] = None,
) -> tuple[int, int, List[ThingPage]]:
    # Fetch summary records of Things (but not the full content), suitable for paging in an API
    count_statement = session.query(Thing)
    if authority is not None:
        count_statement = count_statement.filter(Thing.authority_id == authority)
    count_statement = count_statement.filter(Thing.resolved_status == status)
    overall_count = count_statement.count()
    if limit > 0:
        overall_pages = overall_count / limit
    else:
        overall_pages = 0
    things_statement = select(
        Thing.primary_key,
        Thing.id,
        Thing.authority_id,
        Thing.tcreated,
        Thing.resolved_status,
        Thing.resolved_url,
        Thing.resolve_elapsed,
    )
    if authority is not None:
        things_statement = things_statement.filter(Thing.authority_id == authority)
    things_statement = things_statement.filter(Thing.resolved_status == status)
    if offset > 0:
        things_statement = things_statement.offset(offset)
    if limit > 0:
        things_statement = things_statement.limit(limit)
    things_results = session.exec(things_statement)
    return overall_count, overall_pages, things_results.all()


def get_thing_with_id(session: Session, identifier: str) -> Optional[Thing]:
    statement = (
        select(Thing).filter(Thing.id == identifier).order_by(Thing.primary_key.asc())
    )
    result = session.exec(statement).first()
    if result is None:
        # Fall back to querying the Identifiers table
        identifiers_statement = select(Thing).where(
            Thing.identifiers.like(f"%{identifier}%")
        )
        result = session.exec(identifiers_statement).first()
    return result


def get_things_with_ids(session: Session, identifiers: list[str]) -> list[Thing]:
    statement = select(Thing).where(Thing.id.in_(identifiers))
    things = session.exec(statement).all()
    for thing in things:
        try:
            identifiers.remove(thing.id)
        except ValueError:
            pass
    if len(identifiers) > 0:
        # didn't find all the things, so we need to do an additional query against the remainder
        statement = select(Thing).where(Thing.identifiers.in_(identifiers))
        things_by_identifier = session.exec(statement).all()
        things.extend(things_by_identifier)
    return things


def get_thing_identifiers_for_thing(session: Session, thing_id: int) -> list[str]:
    statement = select(Thing.identifiers).where(Thing.primary_key == thing_id)
    session_exec = session.exec(statement)
    allrows = session_exec.all()
    thing_identifiers = []
    for row in allrows:
        thing_identifiers.append(row[0])
    return thing_identifiers


def last_time_thing_created(
    session: Session, authority_id: str
) -> Optional[datetime.datetime]:
    # A bit of a hack to work around postgres perf issues.  Limit the number of records to examine by including a
    # time created date in the qualifier.
    one_year_ago = datetime.datetime(
        year=datetime.date.today().year - 1, month=1, day=1
    )
    created_select = select(Thing.tcreated)
    if authority_id is not None:
        created_select = created_select.filter(Thing.authority_id == authority_id)
    created_select = (
        created_select.filter(Thing.tcreated >= one_year_ago)
        .limit(1)
        .order_by(Thing.tcreated.desc())
    )
    result = session.exec(created_select).first()
    return result


def _base_thing_select(
    authority: Optional[str] = None,
    status: int = 200,
    limit: int = 100,
    offset: int = 0,
    min_id: int = 0,
) -> SelectOfScalar[Thing]:
    thing_select = select(Thing).filter(Thing.resolved_status == status)
    if authority is not None:
        thing_select = thing_select.filter(Thing.authority_id == authority)
    if offset > 0:
        thing_select = thing_select.offset(offset)
    if limit > 0:
        thing_select = thing_select.limit(limit)
    if min_id > 0:
        thing_select = thing_select.filter(Thing.primary_key > min_id)
    return thing_select


def paged_things_with_ids(
    session: Session,
    authority: Optional[str] = None,
    status: int = 200,
    limit: int = 100,
    offset: int = 0,
    min_time_created: datetime.datetime = None,
    min_id: int = 0,
) -> List[Thing]:
    thing_select = _base_thing_select(authority, status, limit, offset, min_id)

    if min_time_created is not None:
        thing_select = thing_select.filter(Thing.tcreated >= min_time_created)
    thing_select = thing_select.order_by(Thing.primary_key.asc())
    return session.exec(thing_select).all()


def things_for_sitemap(
    session: Session,
    authority: Optional[str] = None,
    status: int = 200,
    limit: int = 100,
    offset: int = 0,
    min_tstamp: datetime.datetime = None,
    min_id: int = 0,
) -> List[Thing]:
    """Returns a list of Things suitable for generating a sitemap.

    In order to allow older unchanged records to not be refetched, order by the timestamp to support automatic diffing
    """
    thing_select = _base_thing_select(authority, status, limit, offset, min_id)
    if min_tstamp is not None:
        thing_select = thing_select.filter(Thing.tstamp >= min_tstamp)
    thing_select = thing_select.order_by(Thing.tstamp.asc(), Thing.primary_key.asc())
    return session.exec(thing_select).all()


def get_thing_meta(session: Session):
    dbq = session.query(
        sqlalchemy.sql.label("status", Thing.resolved_status),
        sqlalchemy.sql.label("count", sqlalchemy.func.count(Thing.resolved_status)),
    ).group_by(Thing.resolved_status)
    meta = {"status": dbq.all()}
    dbq = session.query(
        sqlalchemy.sql.label("authority", Thing.authority_id),
        sqlalchemy.sql.label("count", sqlalchemy.func.count(Thing.authority_id)),
    ).group_by(Thing.authority_id)
    meta["authority"] = dbq.all()
    return meta


def get_sample_types(session: Session):
    dbq = (
        session.query(
            sqlalchemy.sql.label("item_type", Thing.item_type),
            sqlalchemy.sql.label("count", sqlalchemy.func.count(Thing.item_type)),
        )
        .filter(Thing.resolved_status == 200)
        .group_by(Thing.item_type)
    )
    return dbq.all()


def _insert_geome_identifiers(thing: Thing):
    # For now, we will fail all requests for parent IDs, because events appear in multiple samples
    # and would violate referential integrity if we made pointers to children from the event ID
    # parent = thing.resolved_content.get("parent")
    # if parent is not None:
    #     event_ark = parent["bcid"]
    #     if event_ark in arks_to_bp:
    #         print()
    #     event_identifier = ThingIdentifier(guid=event_ark, thing_id=thing.primary_key)
    #     session.add(event_identifier)
    children = thing.resolved_content.get("children")
    if children is not None:
        for child in children:
            child_ark = child["bcid"]
            thing.insert_thing_identifier_if_not_present(child_ark)


def _insert_open_context_identifiers(thing: Thing):
    citation_uri = thing.resolved_content["citation uri"]
    if citation_uri is not None and type(citation_uri) is str:
        open_context_uri = isb_lib.normalized_id(citation_uri)
        thing.insert_thing_identifier_if_not_present(open_context_uri)


def _insert_standard_identifier(thing: Thing):
    thing.insert_thing_identifier_if_not_present(thing.id)


def insert_identifiers(thing: Thing):
    if thing.authority_id == "GEOME":
        _insert_geome_identifiers(thing)
    elif thing.authority_id == "OPENCONTEXT":
        _insert_open_context_identifiers(thing)
    _insert_standard_identifier(thing)


def save_thing(session: Session, thing: Thing):
    logging.debug("Going to add thing to session")
    session.add(thing)
    logging.debug("Added thing to session")
    session.commit()
    logging.debug("committed session")
    insert_identifiers(thing)
    logging.debug("going to insert identifiers")
    session.commit()


def save_or_update_thing(session: Session, thing: Thing):
    try:
        save_thing(session, thing)
    except sqlalchemy.exc.IntegrityError as e:
        session.rollback()
        logging.info(f"Thing already exists {thing.id}, will recreate record")
        logging.info(
            f"Thing already exists with primary key {thing.primary_key}, will update record"
        )
        existing_thing = get_thing_with_id(session, thing.id)
        thing_identifiers = (
            []
        )  # get_thing_identifiers_for_thing(session, existing_thing.primary_key)
        existing_thing.identifiers = thing_identifiers
        if existing_thing is None:
            logging.error(
                f"Error attempting to save existing thing that doesn't exist: {thing.id}, exception: {e}"
            )
        else:
            existing_thing.take_values_from_other_thing(thing)
            try:
                save_thing(session, existing_thing)
            except sqlalchemy.exc.IntegrityError as integrity_error:
                session.rollback()
                logging.error(
                    f"Got error attempting to save existing thing {thing.id}, exception: {integrity_error}"
                )


def all_thing_identifier_objects(
    session: Session, min_id: int, batch_size: int
) -> list[ThingIdentifier]:
    thing_identifiers_select = (
        select(ThingIdentifier)
        .filter(ThingIdentifier.thing_id >= min_id)
        .order_by(ThingIdentifier.thing_id.asc())
        .limit(batch_size)
    )
    thing_identifiers = session.exec(thing_identifiers_select).all()
    return thing_identifiers


def things_with_null_identifiers(session: Session) -> list[Thing]:
    # noinspection PyComparisonWithNone
    things_select = select(Thing).filter(Thing.identifiers == None)
    things = session.exec(things_select).all()
    return things


def all_thing_identifiers(session: Session) -> set[str]:
    thing_identifiers_select = select(Thing.identifiers)
    thing_identifiers = session.execute(thing_identifiers_select).fetchall()
    thing_identifiers_set = set()
    for row in thing_identifiers:
        thing_identifiers_set.update(row[0])
    return thing_identifiers_set


def mark_thing_not_found(session: Session, thing_id: str, resolved_url: str):
    """In case we get an error fetching a thing, mark it as not found in the database"""
    existing_thing = get_thing_with_id(session, thing_id)
    if existing_thing is None:
        thing = Thing()
        thing.id = thing_id
        thing.resolved_status = 404
        thing.tresolved = datetime.datetime.now()
        thing.resolved_url = resolved_url
        save_thing(session, thing)
    else:
        session.execute(
            update(isb_lib.models.thing.Thing)
            .where(isb_lib.models.thing.Thing.id == thing_id)
            .values(resolved_status=404)
            .values(resolved_url=resolved_url)
        )
