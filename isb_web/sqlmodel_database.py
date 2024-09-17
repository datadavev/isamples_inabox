import datetime
import typing
import json
import uuid

import igsn_lib.time
import sqlalchemy
import logging
from typing import Optional, List

from isb_lib.identifiers.noidy.n2tminter import N2TMinter
from isb_lib.models.export_job import ExportJob
from isb_lib.models.namespace import Namespace
from sqlalchemy import Index, update, func
from sqlalchemy.exc import ProgrammingError
from sqlmodel import SQLModel, create_engine, Session, select
from sqlmodel.sql.expression import SelectOfScalar

import isb_lib
from isb_lib.models.person import Person
from isb_lib.models.taxonomy_name import TaxonomyName
from isb_lib.models.thing import Thing, ThingIdentifier, Point
from isb_web.schemas import ThingPage


DRAFT_RESOLVED_URL = "DRAFT"
DRAFT_AUTHORITY_ID = "DRAFT"
DRAFT_RESOLVED_STATUS = -1


class DatabaseBulkUpdater:
    def __init__(self, db_session: Session, authority_id: str, batch_size: int, resolved_media_type: str, primary_keys_by_id: Optional[dict]):
        self.db_session = db_session
        self.authority_id = authority_id
        self.batch_size = batch_size
        self.resolved_media_type = resolved_media_type
        self.current_new_things_batch = []
        self.current_existing_things_batch = []
        self.num_inserts = 0
        self.num_updates = 0
        self.unique_ids = set()
        if primary_keys_by_id is not None:
            self.primary_keys_by_id = primary_keys_by_id
        else:
            self.primary_keys_by_id = {}

    def add_thing(self, resolved_content: dict, thing_id: str, resolved_url: str, resolved_status: int, h3: str, t_created: Optional[datetime.datetime] = None):
        tstamp = datetime.datetime.now()
        if t_created is None:
            t_created = tstamp
        thing_dict = {
            "resolved_content": resolved_content,
            "id": thing_id,
            "tstamp": tstamp,
            "tcreated": t_created,
            "item_type": "sample",
            "authority_id": self.authority_id,
            "resolved_url": resolved_url,
            "resolved_status": resolved_status,
            "tresolved": tstamp,
            "resolve_elapsed": 0,
            "resolved_media_type": self.resolved_media_type,
            "identifiers": json.dumps([thing_id]),
            "h3": h3
        }
        if thing_id in self.primary_keys_by_id:
            thing_dict["primary_key"] = self.primary_keys_by_id[thing_id]
            self.current_existing_things_batch.append(thing_dict)
        else:
            self.current_new_things_batch.append(thing_dict)
        self.unique_ids.add(thing_id)
        if (len(self.current_new_things_batch) + len(self.current_existing_things_batch)) == self.batch_size:
            self._save_to_db()

    def finish(self):
        print("About to finish import…")
        self._save_to_db()
        print(f"Finished at {datetime.datetime.now()}.")

    def _save_to_db(self):
        self.num_inserts += len(self.current_new_things_batch)
        self.num_updates += len(self.current_existing_things_batch)
        print(f"\n\nInserting into the database because we've hit the batch size of {self.batch_size}")
        if len(self.current_new_things_batch) > 0:
            self.db_session.bulk_insert_mappings(
                mapper=Thing,
                mappings=self.current_new_things_batch,
                return_defaults=False,
            )
        if len(self.current_existing_things_batch) > 0:
            self.db_session.bulk_update_mappings(
                mapper=Thing, mappings=self.current_existing_things_batch
            )
        self.db_session.commit()
        print(f"\n\nSave complete.  Have inserted {self.num_inserts} rows, updated {self.num_updates} rows, seen {len(self.unique_ids)} unique ids.")
        self.current_new_things_batch = []
        self.current_existing_things_batch = []


class SQLModelDAO:
    def __init__(self, db_url: Optional[str], echo: bool = False):
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
        # Disable this to try and avoid the weird postgresql pause we were seeing in production
        if False:
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


def random_things_with_authority(session: Session, authority: str, count: int) -> list[Thing]:
    statement = select(Thing).where(Thing.authority_id == authority).limit(count).order_by(func.random())
    return session.exec(statement).all()


def get_things_with_ids(session: Session, identifiers: list[str]) -> list[Thing]:
    statement = select(Thing).where(Thing.id.in_(identifiers))
    things = session.exec(statement).all()
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
    min_time_created: Optional[datetime.datetime] = None,
    min_id: int = 0,
) -> List[Thing]:
    thing_select = _base_thing_select(authority, status, limit, offset, min_id)

    if min_time_created is not None:
        thing_select = thing_select.filter(Thing.tcreated > min_time_created)
    thing_select = thing_select.order_by(Thing.primary_key.asc())
    return session.exec(thing_select).all()


def things_for_sitemap(
    session: Session,
    authority: Optional[str] = None,
    status: int = 200,
    limit: int = 100,
    offset: int = 0,
    min_tstamp: Optional[datetime.datetime] = None,
    min_id: int = 0,
) -> List[tuple[str, datetime]]:
    """Returns a list of Things suitable for generating a sitemap.

    In order to allow older unchanged records to not be refetched, order by the timestamp to support automatic diffing
    """
    thing_select = _base_thing_select(authority, status, limit, offset, min_id)
    thing_select = thing_select.with_only_columns(Thing.id, Thing.tstamp, Thing.resolved_content)
    if min_tstamp is not None:
        thing_select = thing_select.filter(Thing.tstamp >= min_tstamp)
    thing_select = thing_select.order_by(Thing.tstamp.asc(), Thing.primary_key.asc())
    things_result = session.execute(thing_select)
    return things_result.all()


def things_by_authority_count(session: Session) -> list[tuple]:
    dbq = session.query(
        sqlalchemy.sql.label("authority", Thing.authority_id),
        sqlalchemy.sql.label("count", sqlalchemy.func.count(Thing.authority_id)),
    ).group_by(Thing.authority_id)
    return dbq.all()


def things_by_authority_count_dict(session: Session) -> dict[str: int]:
    tuples = things_by_authority_count(session)
    things_dict = {}
    for tuple in tuples:
        if tuple[0] is not None:
            things_dict[tuple[0]] = tuple[1]
    return things_dict


def get_thing_meta(session: Session):
    dbq = session.query(
        sqlalchemy.sql.label("status", Thing.resolved_status),
        sqlalchemy.sql.label("count", sqlalchemy.func.count(Thing.resolved_status)),
    ).group_by(Thing.resolved_status)
    meta = {"status": dbq.all()}
    meta["authority"] = things_by_authority_count(session)
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


def geome_identifiers_from_resolved_content(resolved_content: typing.Dict) -> list[str]:
    children = resolved_content.get("children")
    identifiers = []
    if children is not None:
        for child in children:
            child_ark = child["bcid"]
            identifiers.append(child_ark)
    return identifiers


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
    identifiers = geome_identifiers_from_resolved_content(thing.resolved_content)
    for identifier in identifiers:
        thing.insert_thing_identifier_if_not_present(identifier)


def opencontext_identifiers_from_resolved_content(resolved_content: typing.Dict) -> list[str]:
    identifiers = []
    citation_uri = resolved_content["citation uri"]
    if citation_uri is not None and type(citation_uri) is str:
        identifiers.append(isb_lib.normalized_id(citation_uri))
    return identifiers


def _insert_open_context_identifiers(thing: Thing):
    citation_uri = thing.resolved_content["citation uri"]
    if citation_uri is not None and type(citation_uri) is str:
        open_context_uri = isb_lib.normalized_id(citation_uri)
        thing.insert_thing_identifier_if_not_present(open_context_uri)


def _standard_identifier(thing: Thing) -> str:
    return thing.id


def thing_identifiers_from_resolved_content(authority_id: str, resolved_content: typing.Dict) -> list[str]:
    identifiers = []
    if authority_id == "GEOME":
        identifiers += geome_identifiers_from_resolved_content(resolved_content)
    elif authority_id == "OPENCONTEXT":
        identifiers += opencontext_identifiers_from_resolved_content(resolved_content)
    return identifiers


def insert_identifiers(thing: Thing):
    identifiers = thing_identifiers_from_resolved_content(thing.authority_id, thing.resolved_content)
    identifiers.append(_standard_identifier(thing))
    for identifier in identifiers:
        thing.insert_thing_identifier_if_not_present(identifier)


def save_thing(session: Session, thing: Thing):
    insert_identifiers(thing)
    logging.debug("Going to add thing to session")
    session.add(thing)
    logging.debug("Added thing to session")
    session.commit()
    logging.debug("committed session")


def save_draft_thing_with_id(session: Session, draft_id: str) -> Thing:
    draft_thing = Thing()
    draft_thing.id = draft_id
    draft_thing.resolved_status = DRAFT_RESOLVED_STATUS
    draft_thing.authority_id = DRAFT_AUTHORITY_ID
    draft_thing.resolved_url = DRAFT_RESOLVED_URL
    draft_thing.tcreated = datetime.datetime.now()
    save_thing(session, draft_thing)
    return draft_thing


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
    # Note that SQLAlchemy needs things to be written this way but it triggers flake8 so manually override
    things_select = select(Thing).filter(Thing.identifiers == None)  # noqa: E711
    things = session.exec(things_select).all()
    return things


def all_thing_identifiers(session: Session, authority: typing.Optional[str] = None) -> typing.Dict[str, int]:
    thing_identifiers_select = select(Thing.primary_key, Thing.identifiers)
    if authority is not None:
        thing_identifiers_select = thing_identifiers_select.where(Thing.authority_id == authority)
    thing_identifiers = session.execute(thing_identifiers_select).fetchall()
    thing_identifiers_dict = {}
    for row in thing_identifiers:
        for identifier in row[1]:
            thing_identifiers_dict[identifier] = row[0]
    return thing_identifiers_dict


def all_thing_primary_keys(session: Session, authority: typing.Optional[str] = None) -> typing.Dict[str, int]:
    thing_pk_select = select(Thing.primary_key, Thing.id)
    if authority is not None:
        thing_pk_select = thing_pk_select.where(Thing.authority_id == authority)
    thing_pks = session.execute(thing_pk_select).fetchall()
    thing_pks_dict = {}
    for row in thing_pks:
        thing_pks_dict[row[1]] = row[0]
    return thing_pks_dict


def h3_values_without_points(session: Session, all_h3_values: set) -> list:
    h3_select = select(Point.h3).distinct().where(Point.h3.in_(list(all_h3_values)))
    h3_with_points_set = set(session.exec(h3_select).all())
    return all_h3_values - h3_with_points_set


def h3_to_height(session: Session) -> typing.Dict:
    h3_select = select(Point.h3, Point.height).filter(Point.height != None)  # noqa: E711
    point_rows = session.execute(h3_select).fetchall()
    point_dict = {}
    for row in point_rows:
        point_dict[row[0]] = row[1]
    return point_dict


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


def save_person_with_orcid_id(session: Session, orcid_id: str) -> Person:
    person = Person()
    person.orcid_id = orcid_id
    session.add(person)
    session.commit()
    return person


def all_orcid_ids(session: Session) -> list[str]:
    orcid_ids_select = select(Person.orcid_id)
    orcid_id_rows = session.execute(orcid_ids_select).fetchall()
    orcid_ids = []
    for row in orcid_id_rows:
        orcid_ids.append(row[0])
    return orcid_ids


def save_or_update_namespace(session: Session, namespace: Namespace) -> Namespace:
    now = igsn_lib.time.dtnow()
    if namespace.primary_key is None:
        namespace.tcreated = now
    namespace.tstamp = now
    session.add(namespace)
    session.commit()
    return namespace


def namespace_with_shoulder(session: Session, shoulder: str) -> Optional[Namespace]:
    namespace_select = select(Namespace).where(Namespace.shoulder == shoulder)
    result = session.exec(namespace_select)
    return result.first()


def mint_identifiers_in_namespace(session: Session, namespace: Namespace, num_identifiers: int) -> list[str]:
    if namespace is None:
        raise ValueError("Namespace cannot be None.  Identifiers will not be minted.")
    minter = N2TMinter(namespace.shoulder)
    if namespace.minter_state is not None:
        minter.fromDict(namespace.minter_state)
    identifiers = []
    for identifier in minter.mint(num_identifiers):
        identifiers.append(identifier)
    namespace.minter_state = minter.asDict()
    namespace.tstamp = datetime.datetime.now()
    session.add(namespace)
    session.commit()
    return identifiers


THING_EXPORT_FIELD_LIST = "id, tstamp, tcreated, item_type, authority_id, resolved_url, resolved_status, tresolved, resolve_elapsed, resolved_content, resolved_media_type, _id, identifiers, h3"


def dump_things_with_ids_to_file(session: Session, identifiers: list[str], file_path: str):
    quoted_identifiers = [f"'{identifier}'" for identifier in identifiers]
    sql = f"copy (select {THING_EXPORT_FIELD_LIST} from thing where id in ({','.join(quoted_identifiers)})) to '{file_path}'"
    session.execute(sql)


def load_things_from_file(session: Session, file_path: str):
    sql = f"copy thing ({THING_EXPORT_FIELD_LIST}) from '{file_path}'"
    session.execute(sql)
    session.commit()


def save_taxonomy_name(session: Session, name: TaxonomyName, commit: bool = False):
    session.add(name)
    if commit:
        session.commit()


def taxonomy_name_to_kingdom_map(session: Session) -> dict:
    name_select = select(TaxonomyName.name, TaxonomyName.kingdom)
    name_rows = session.execute(name_select).fetchall()
    name_dict = {}
    for row in name_rows:
        name_dict[row[0]] = row[1]
    return name_dict


def kingdom_for_taxonomy_name(session: Session, name: str) -> Optional[str]:
    kingdom_select = select(TaxonomyName.kingdom).where(TaxonomyName.name == name)
    return session.exec(kingdom_select).first()


def save_or_update_export_job(session: Session, export_job: ExportJob) -> ExportJob:
    now = igsn_lib.time.dtnow()
    if export_job.primary_key is None:
        export_job.tcreated = now
        export_job.uuid = str(uuid.uuid4())
    session.add(export_job)
    session.commit()
    return export_job


def export_job_with_uuid(session: Session, uuid: str) -> Optional[ExportJob]:
    export_job_select = select(ExportJob).where(ExportJob.uuid == uuid)
    result = session.exec(export_job_select)
    return result.first()
