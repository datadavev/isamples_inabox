import datetime
import random

import pytest

from isb_lib.models.export_job import ExportJob
from isb_lib.models.namespace import Namespace
from sqlalchemy.orm.attributes import flag_modified
from sqlmodel import Session, SQLModel, create_engine
from sqlmodel.pool import StaticPool

from isb_lib.core import ThingRecordIterator
from isb_lib.models.taxonomy_name import TaxonomyName
from isb_lib.models.thing import Thing, Point
from isb_web.sqlmodel_database import (
    get_thing_with_id,
    read_things_summary,
    last_time_thing_created,
    paged_things_with_ids,
    save_thing,
    things_for_sitemap,
    mark_thing_not_found,
    save_or_update_thing,
    get_things_with_ids, insert_identifiers, all_thing_identifiers, get_thing_identifiers_for_thing,
    h3_values_without_points, h3_to_height, all_thing_primary_keys, save_draft_thing_with_id, save_person_with_orcid_id,
    all_orcid_ids, mint_identifiers_in_namespace, save_or_update_namespace, save_taxonomy_name,
    taxonomy_name_to_kingdom_map, kingdom_for_taxonomy_name, get_thing_meta, things_by_authority_count_dict,
    save_or_update_export_job, export_job_with_uuid,
)
from test_utils import _add_some_things


@pytest.fixture(name="session")
def session_fixture():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        echo=True
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


def test_get_thing_with_id_no_things(session: Session):
    should_be_none = get_thing_with_id(session, "12345")
    assert should_be_none is None


def test_get_thing_with_id_thing(session: Session):
    test_id = "123456"
    new_thing = Thing(
        id=test_id,
        authority_id="test",
        resolved_url="http://foo.bar",
        resolved_status=200,
        resolved_content={"foo": "bar"},
    )
    session.add(new_thing)
    session.commit()
    shouldnt_be_none = get_thing_with_id(session, test_id)
    assert shouldnt_be_none is not None
    assert shouldnt_be_none.primary_key is not None
    assert test_id == shouldnt_be_none.id


def test_read_things_no_things(session: Session):
    count, pages, data = read_things_summary(session, 0, 0)
    assert 0 == count
    assert 0 == pages
    assert len(data) == 0


def test_read_things_with_things(session: Session):
    new_thing = Thing(
        id="123456",
        authority_id="test",
        resolved_url="http://foo.bar",
        resolved_status=200,
        resolved_content={"foo": "bar"},
    )
    session.add(new_thing)
    session.commit()
    count, pages, data = read_things_summary(session, 0, 0)
    assert 1 == count
    assert 0 == pages
    assert len(data) > 0


last_time_thing_created_values = [("test", "test"), (None, "test")]


@pytest.mark.parametrize(
    "authority_id_for_fetch,authority_id_for_create", last_time_thing_created_values
)
def test_last_time_thing_created(
    session: Session, authority_id_for_fetch: str, authority_id_for_create: str
):
    created = last_time_thing_created(session, authority_id_for_fetch)
    assert created is None
    new_thing = Thing(
        id="123456",
        authority_id=authority_id_for_create,
        resolved_url="http://foo.bar",
        resolved_status=200,
        resolved_content={"foo": "bar"},
        tcreated=datetime.datetime.now(),
    )
    session.add(new_thing)
    session.commit()
    new_created = last_time_thing_created(session, authority_id_for_fetch)
    assert new_created is not None


def test_paged_things_with_ids(session: Session):
    authority = "test_authority"
    old_tcreated = datetime.datetime(1978, month=11, day=22)
    _add_some_things(session, 10, authority, old_tcreated)
    things = paged_things_with_ids(session, authority, 200, 10, 0, None, 0)
    # This should be everything, check we get 10 back
    assert 10 == len(things)
    assert type(things[0]) is Thing
    # Now make sure we only get one since we're filtering by id (and we know internally there is a sequence)
    things_with_id = paged_things_with_ids(session, authority, 200, 10, 0, None, 9)
    assert 1 == len(things_with_id)
    now = datetime.datetime.now()
    # Now add some stuff with the current date, and verify we don't get the old ones anymore
    _add_some_things(session, 5, authority, now)
    things_with_tcreated = paged_things_with_ids(session, authority, 200, 10, 0, now, 0)
    assert 5 == len(things_with_tcreated)
    all_things = paged_things_with_ids(session, authority, 200, 100, 0, None, 0)
    assert 15 == len(all_things)


def test_things_for_sitemap(session: Session):
    authority = "test"
    _add_some_things(session, 20, authority)
    things = things_for_sitemap(session, None, 200, 100, 0, None)
    # should have a list of 20
    assert 20 == len(things)
    for thing in things:
        assert 2 == len(thing)
        assert type(thing[0]) is str
        assert type(thing[1]) is datetime.datetime
    # remember this for later
    last_tstamp = things[-1][1]
    last_id = things[-1][0]

    different_authority = "different"
    _add_some_things(session, 20, different_authority)

    # Now fetch with the tstamp and id
    new_things = things_for_sitemap(session, different_authority, 200, 100, 0, last_tstamp, int(last_id))
    # Should get 21, as the method returns >= timestamp
    assert 20 == len(new_things)
    for new_thing in new_things:
        assert new_thing[1] >= last_tstamp


def test_thing_iterator(session: Session):
    authority_id = "test"
    num_things = 10
    _add_some_things(session, num_things, authority_id, None)
    iterator = ThingRecordIterator(session, authority_id, 200, 5, 0, None)
    count_iterated_things = 0
    for thing in iterator.yieldRecordsByPage():
        assert type(thing) is Thing
        count_iterated_things += 1
    assert num_things == count_iterated_things


def test_thing_with_identifier(session: Session):
    thing_id = "123456"
    new_thing = Thing(
        id=thing_id,
        authority_id="test",
        resolved_url="http://foo.bar",
        resolved_status=200,
        resolved_content={"foo": "bar"},
    )
    session.add(new_thing)
    session.commit()
    test_id = "ark:/123456"
    # Right now, query by that id shouldn't find anything
    thing_with_identifier = get_thing_with_id(session, test_id)
    assert thing_with_identifier is None
    thing_with_identifier = get_thing_with_id(session, thing_id)
    thing_with_identifier.identifiers = [test_id]
    session.commit()
    # Just added ID, should find it now
    thing_with_identifier = get_thing_with_id(session, test_id)
    assert thing_with_identifier is not None
    assert thing_id == thing_with_identifier.id


def _fetch_thing_identifiers(session: Session) -> set[str]:
    return all_thing_identifiers(session)


def _test_insert_identifiers(
    session: Session, thing: Thing
) -> set:
    session.add(thing)
    session.commit()
    insert_identifiers(thing)
    session.commit()
    # should have two ids for specified thing
    ids = _fetch_thing_identifiers(session)
    return ids


def test_insert_identifiers_geome(session: Session):
    thing_id = "123456"
    child_id = "1234567890"
    geome_thing = Thing(
        id=thing_id,
        authority_id="GEOME",
        resolved_url="http://foo.bar",
        resolved_status=200,
        resolved_content={"children": [{"bcid": child_id}]},
    )
    identifiers = _test_insert_identifiers(session, geome_thing)
    assert 2 == len(identifiers)


def test_insert_identifiers_opencontext(session: Session):
    thing_id = "7890"
    citation_uri = "12345"
    opencontext_thing = Thing(
        id=thing_id,
        authority_id="OPENCONTEXT",
        resolved_url="http://foo.bar",
        resolved_status=200,
        resolved_content={"citation uri": citation_uri},
    )
    identifiers = _test_insert_identifiers(session, opencontext_thing)
    assert 2 == len(identifiers)


def test_insert_identifiers_sesar(session: Session):
    thing_id = "7890"
    sesar_thing = _test_sesar_thing(thing_id)
    identifiers = _test_insert_identifiers(session, sesar_thing)
    assert 1 == len(identifiers)


def _test_sesar_thing(thing_id):
    sesar_thing = Thing(
        id=thing_id,
        authority_id="SESAR",
        resolved_url="http://foo.bar",
        resolved_status=200,
        resolved_content={},
    )
    return sesar_thing


def test_get_thing_identifiers_for_thing(session: Session):
    thing_id = "5678"
    sesar_thing = _test_sesar_thing(thing_id)
    _test_insert_identifiers(session, sesar_thing)
    refetched_identifiers = get_thing_identifiers_for_thing(
        session, sesar_thing.primary_key
    )
    assert 1 == len(refetched_identifiers)
    assert thing_id == refetched_identifiers[0]


def test_save_thing(session: Session) -> Thing:
    sesar_thing = Thing(
        id="12345",
        authority_id="SESAR",
        resolved_url="http://foo.bar",
        resolved_status=200,
        resolved_content={},
    )
    save_thing(session, sesar_thing)
    # we should have a ThingIdentifier now
    assert 1 == len(sesar_thing.identifiers)
    assert len(sesar_thing.__repr__()) > 0
    return sesar_thing


def test_save_existing_thing(session: Session):
    existing_thing = test_save_thing(session)
    # touch the timestamp to update the Thing
    existing_thing.tstamp = datetime.datetime.now()
    save_thing(session, existing_thing)
    ids = _fetch_thing_identifiers(session)
    # should still have 1 id since we didn't recreate existing ones during the save
    assert 1 == len(ids)


def test_save_or_update_thing_existing_thing(session: Session):
    existing_thing = test_save_thing(session)
    not_from_session_thing = Thing()
    not_from_session_thing.take_values_from_other_thing(existing_thing)
    updated_tstamp = datetime.datetime.now()
    not_from_session_thing.tstamp = updated_tstamp
    save_or_update_thing(session, not_from_session_thing)
    session.commit()
    # fetch it again, verify it has the proper tstamp
    refetched_thing = get_thing_with_id(session, not_from_session_thing.id)
    assert updated_tstamp == refetched_thing.tstamp


def test_mark_existing_thing_not_found(session: Session):
    existing_thing = test_save_thing(session)
    # make sure status is 200
    assert 200 == existing_thing.resolved_status
    resolved_url = "http://foo.bar.baz"
    mark_thing_not_found(session, existing_thing.id, resolved_url)
    # fetch it again to read out the db, should be 404
    refetched_thing = get_thing_with_id(session, existing_thing.id)
    assert 404 == refetched_thing.resolved_status
    assert resolved_url == refetched_thing.resolved_url


def test_mark_nonexistent_thing_not_found(session: Session):
    test_id = "ark:/123456"
    existing_thing_with_id = get_thing_with_id(session, test_id)
    assert existing_thing_with_id is None
    resolved_url = "http://foo.bar.baz.beg"
    mark_thing_not_found(session, test_id, resolved_url)
    not_found_thing = get_thing_with_id(session, test_id)
    assert not_found_thing is not None
    assert test_id == not_found_thing.id
    assert resolved_url == not_found_thing.resolved_url
    assert 404 == not_found_thing.resolved_status


def test_get_thing_meta(session: Session):
    test_save_thing(session)
    result = get_thing_meta(session)
    assert len(result) == 2
    assert "status" in result
    assert "authority" in result


def test_things_by_authority_count_dict(session: Session):
    test_save_thing(session)
    result = things_by_authority_count_dict(session)
    assert result["SESAR"] == 1


def test_insert_thing_identifier_if_not_present():
    thing = Thing(primary_key=1)
    thing.insert_thing_identifier_if_not_present("12345")
    assert 1 == len(thing.identifiers)

    # insert the same one again, should be no-op
    thing.insert_thing_identifier_if_not_present("12345")
    assert 1 == len(thing.identifiers)

    # create a new instance that is semantically equal, should be no-op
    thing.insert_thing_identifier_if_not_present("12345")
    assert 1 == len(thing.identifiers)


def test_take_values_from_other_thing():
    thing1 = Thing(
        id="123456",
        resolved_content={},
        resolved_url="http://foo.bar",
        resolved_status=200,
        tresolved=datetime.datetime.now(),
        resolve_elapsed=1,
        tcreated=datetime.datetime.now(),
        tstamp=datetime.datetime.now(),
    )
    thing2 = Thing(
        id="78910",
        resolved_content={"howdy": "ho"},
        resolved_url="http://foo.bar.baz",
        resolved_status=404,
        tresolved=datetime.datetime.now(),
        resolve_elapsed=10,
        tcreated=datetime.datetime.now(),
        tstamp=datetime.datetime.now(),
    )
    thing1.take_values_from_other_thing(thing2)
    assert thing1.id == thing2.id
    assert thing1.resolved_content == thing2.resolved_content
    assert thing1.resolved_url == thing2.resolved_url
    assert thing1.resolved_status == thing2.resolved_status
    assert thing1.tresolved == thing2.tresolved
    assert thing1.tcreated == thing2.tcreated
    assert thing1.tstamp == thing2.tstamp


def test_get_things_with_ids(session: Session):
    _add_some_things(session, 10, "authority", datetime.datetime.now())
    # test adder assigns ids via numeric string, e.g. 0…n
    ids_to_assert = ["0", "1", "2"]
    things = get_things_with_ids(session, ids_to_assert)
    assert len(things) == 3


def test_get_thing_with_id_with_identifier(session: Session):
    _add_some_things(session, 10, "authority", datetime.datetime.now())
    guid = "12345"
    should_be_empty = get_thing_with_id(session, guid)
    assert should_be_empty is None
    existing_thing = get_thing_with_id(session, "0")
    existing_thing.insert_thing_identifier_if_not_present(guid)
    # this bit is curious…
    flag_modified(existing_thing, "identifiers")
    session.commit()
    thing_with_identifier = get_thing_with_id(session, guid)
    assert thing_with_identifier is not None


def test_all_thing_identifiers(session: Session):
    _add_some_things(session, 10, "authority", datetime.datetime.now())
    all_identifiers = all_thing_identifiers(session)
    assert 10 == len(all_identifiers)


def test_all_thing_primary_keys(session: Session):
    _add_some_things(session, 10, "authority", datetime.datetime.now())
    all_primary_keys = all_thing_primary_keys(session)
    assert 10 == len(all_primary_keys)


def test_h3_values_without_points(session: Session):
    no_height = "8f3f6dadb58ad40"
    with_height = "8f3e6dca50120b3"
    point = Point(h3=with_height, latitude=1.0, longitude=1.0, height=10.0)
    session.add(point)
    session.commit()
    h3_no_height = h3_values_without_points(session, set([no_height, with_height]))
    assert 1 == len(h3_no_height)
    assert no_height == h3_no_height.pop()


def test_h3_to_height(session: Session):
    h3_1 = "8f3e6dca50120b3"
    height_1 = 10.0
    point1 = Point(h3=h3_1, latitude=1.0, longitude=1.0, height=height_1)
    session.add(point1)
    h3_2 = "8f3f6dadb58ad40"
    height_2 = 20.0
    point2 = Point(h3=h3_2, latitude=1.0, longitude=1.0, height=height_2)
    session.add(point2)
    session.commit()
    height_dict = h3_to_height(session)
    assert len(height_dict) == 2
    assert height_1 == height_dict.get(h3_1)
    assert height_2 == height_dict.get(h3_2)


def test_save_draft_thing(session: Session):
    draft_id = f"doi:12345/{random.randrange(0, 1000000)}"
    draft_thing = save_draft_thing_with_id(session, draft_id)
    assert draft_thing is not None
    assert draft_id == draft_thing.id


def test_save_person(session: Session):
    orcid_id = "0000-0003-2109-7692"
    person = save_person_with_orcid_id(session, orcid_id)
    assert person is not None
    assert person.primary_key is not None
    assert orcid_id == person.orcid_id


def test_all_orcid_ids(session: Session):
    orcid_id_1 = "1111-2222-3333-4444"
    orcid_id_2 = "6666-7777-8888-9999"
    save_person_with_orcid_id(session, orcid_id_1)
    save_person_with_orcid_id(session, orcid_id_2)
    orcid_ids = all_orcid_ids(session)
    assert 2 == len(orcid_ids)
    assert orcid_id_1 in orcid_ids
    assert orcid_id_2 in orcid_ids


def test_save_namespace(session: Session):
    orcid_id = "0000-0003-2109-7692"
    shoulder = "1234/fk44"
    namespace = Namespace()
    namespace.shoulder = shoulder
    namespace.allowed_people = [orcid_id]
    namespace = save_or_update_namespace(session, namespace)
    assert namespace is not None
    assert namespace.primary_key is not None
    assert namespace.allowed_people == [orcid_id]
    assert namespace.tstamp is not None
    assert namespace.tcreated is not None


def test_mint_identifiers_in_namespace(session: Session):
    orcid_id = "0000-0003-2109-7692"
    shoulder = "1234/fk44"
    namespace = Namespace()
    namespace.shoulder = shoulder
    namespace.allowed_people = [orcid_id]
    save_or_update_namespace(session, namespace)
    identifiers = mint_identifiers_in_namespace(session, namespace, 10)
    assert 10 == len(identifiers)
    # Then mint again to be sure that the state is saved and the identifiers are unique
    identifiers2 = mint_identifiers_in_namespace(session, namespace, 10)
    set1 = set(identifiers)
    set2 = set(identifiers2)
    assert set1.isdisjoint(set2)


def test_add_orcid_id_to_namespace(session: Session):
    orcid_id = "0000-0003-2109-7692"
    shoulder = "1234/fk44"
    namespace = Namespace()
    namespace.shoulder = shoulder
    namespace.allowed_people = [orcid_id]
    namespace = save_or_update_namespace(session, namespace)
    orcid_id_2 = "0000-0003-2109-7693"
    namespace.add_allowed_person(orcid_id_2)
    namespace = save_or_update_namespace(session, namespace)
    assert namespace.allowed_people == [orcid_id, orcid_id_2]
    namespace.remove_allowed_person(orcid_id)
    namespace = save_or_update_namespace(session, namespace)
    assert namespace.allowed_people == [orcid_id_2]


def _insert_test_taxonomy_names(session):
    name1 = TaxonomyName()
    name1.name = "name1"
    name1.kingdom = "kingdom1"
    save_taxonomy_name(session, name1, True)
    name2 = TaxonomyName()
    name2.name = "name2"
    name2.kingdom = "kingdom2"
    save_taxonomy_name(session, name2, True)


def test_taxonomy_name_to_kingdom_map(session: Session):
    _insert_test_taxonomy_names(session)
    map = taxonomy_name_to_kingdom_map(session)
    assert map["name1"] == "kingdom1"
    assert map["name2"] == "kingdom2"
    assert len(map) == 2


def test_kingdom_for_taxonomy_name(session: Session):
    _insert_test_taxonomy_names(session)
    kingdom = kingdom_for_taxonomy_name(session, "name1")
    assert "kingdom1" == kingdom
    # should work for itself, too
    kingdom = kingdom_for_taxonomy_name(session, "kingdom1")
    assert "kingdom1" == kingdom
    kingdom = kingdom_for_taxonomy_name(session, "name2")
    assert "kingdom2" == kingdom


def test_save_export_job(session: Session):
    export_job = _create_test_export_job(session)
    assert export_job.primary_key is not None
    assert export_job.tcreated is not None
    assert export_job.uuid is not None
    assert export_job.solr_query_params == TEST_QUERY_PARAMS


TEST_QUERY_PARAMS = [["fq", "searchText:Tucson"], ["fl", "id"], ["fl", "searchText"], ["wt", "json"], ["q", "*:*"], ["rows", 10], ["start", 0]]


def _create_test_export_job(session) -> ExportJob:
    export_job = ExportJob()
    export_job.creator_id = "123456"
    export_job.solr_query_params = TEST_QUERY_PARAMS
    export_job = save_or_update_export_job(session, export_job)
    return export_job


def test_export_job_with_uuid(session: Session):
    export_job = _create_test_export_job(session)
    refetched_export_job = export_job_with_uuid(session, export_job.uuid)
    assert export_job.primary_key == refetched_export_job.primary_key


def test_export_job_with_uuid_doesnt_exist(session: Session):
    shouldnt_exist = export_job_with_uuid(session, "foobar")
    assert shouldnt_exist is None
