import json
from unittest.mock import MagicMock, patch

import pytest

from fastapi.testclient import TestClient
from httpx import Response
from sqlmodel import Session, SQLModel, create_engine
from sqlmodel.pool import StaticPool


from isb_lib.models import thing
from isb_web.main import get_session, app, manage_app


def _test_model():
    thing_1 = thing.Thing()
    thing_1.id = TEST_IGSN
    thing_1.authority_id = TEST_AUTHORITY_ID
    thing_1.resolved_url = TEST_RESOLVED_URL
    thing_1.resolved_status = 200
    thing_1.item_type = "sample"
    thing_1.resolved_content = {
        "@id": "https://data.geosamples.org/sample/igsn/BSU00062W",
        "igsn": "BSU00062W",
        "@context": "https://raw.githubusercontent.com/IGSN/igsn-json/master/schema.igsn.org/json/registration/v0.1/context.jsonld",
        "registrant": {
            "name": "IEDA",
            "identifiers": {"id": "https://www.geosamples.org", "kind": "uri"},
        },
        "description": {
            "log": [
                {"type": "registered", "timestamp": "2020-12-10 04:28:11"},
                {"type": "published", "timestamp": "2020-12-10 12:00:00"},
                {"type": "lastUpdated", "timestamp": "2020-12-10 04:28:11"},
            ],
            "material": "Igneous>Volcanic>Rock",
            "collector": "Marwan A. Wartes",
            "publisher": {
                "@id": "https://www.geosamples.org",
                "url": "https://www.geosamples.org",
                "name": "EarthChem",
                "@type": "Organization",
                "contactPoint": {
                    "url": "https://www.geosamples.org/contact/",
                    "name": "Information Desk",
                    "@type": "ContactPoint",
                    "email": "info@geosamples.org",
                    "contactType": "Customer Service",
                },
            },
            "igsnPrefix": "BSU",
            "sampleName": "18MAW006A-p>2.85",
            "sampleType": "Individual Sample>Mechanical Fraction",
            "description": "Basal Canning/tongue of Hue Shale. Cliff beneath mass transport deposit. Station is ~3 m above creek level. Sugary, friable tuff. Light gray fresh. Hard silicied tuff above, dark gray bentonitic mudstone below. Sampled zone ~8 cm thick.",
            "contributors": [
                {
                    "@type": "Role",
                    "roleName": "Sample Owner",
                    "contributor": [
                        {
                            "name": "Mark Schmitz",
                            "@type": "Person",
                            "givenName": "Mark",
                            "familyName": "Schmitz",
                        }
                    ],
                },
                {
                    "@type": "Role",
                    "roleName": "Sample Registrant",
                    "contributor": [
                        {
                            "name": "Mark Schmitz",
                            "@type": "Person",
                            "givenName": "Mark",
                            "familyName": "Schmitz",
                        }
                    ],
                },
                {
                    "@type": "Role",
                    "roleName": "Sample Archive Contact",
                    "contributor": [{"@type": "Person"}],
                },
            ],
            "collectorDetail": "Alaska Division of Geological & Geophysical Surveys, 3354 College Road, Fairbanks, Alaska 99709-3707",
            "spatialCoverage": {
                "geo": [
                    {
                        "@type": "GeoCoordinates",
                        "latitude": "69.183874",
                        "elevation": "335.000000 Feet",
                        "longitude": "-148.550682",
                    }
                ],
                "@type": "Place",
            },
            "parentIdentifier": "BSU0005LI",
            "supplementMetadata": {
                "county": "North Slope Borough",
                "country": "United States",
                "document": [],
                "province": "Alaska",
                "sampleId": 4562894,
                "childIGSN": [],
                "elevation": 335.0,
                "fieldName": "tuff",
                "otherName": [],
                "siblingIGSN": ["BSU00062U", "BSU00062T", "BSU00062S", "BSU00062V"],
                "elevationUnit": "Feet",
                "currentArchive": "Boise State University",
                "geologicalUnit": "Canning Formation",
                "publicationUrl": [],
                "externalSampleId": "18MAW006A-p>2.85",
                "primaryLocationName": "Sagashak, creek east of Sagavanirktok River, North Slope Alaska",
                "currentArchiveContact": "iglinfo@boisestate.edu",
            },
        },
    }
    return thing_1


@pytest.fixture(name="session")
def session_fixture():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        thing_1 = _test_model()
        session.add(thing_1)
        session.commit()
        yield session


@pytest.fixture(name="client")
def client_fixture(session: Session):
    def get_session_override():
        return session

    app.dependency_overrides[get_session] = get_session_override
    client = TestClient(app)
    yield client
    app.dependency_overrides.clear()


@pytest.fixture(name="manage_client")
def manage_client_fixture(session: Session):
    def get_session_override():
        return session

    manage_app.dependency_overrides[get_session] = get_session_override
    client = TestClient(manage_app)
    yield client
    manage_app.dependency_overrides.clear()


TEST_IGSN = "IGSN:123456"
TEST_RESOLVED_URL = "http://foo/bar"
TEST_AUTHORITY_ID = "SESAR"


def test_things(client: TestClient, session: Session):
    response = client.request("GET", "/thing/", json={"authority": "SESAR"})
    data = response.json()
    first_fetched_thing = data["data"][0]
    assert data["total_records"] > 0
    assert data.get("params") is not None
    assert data.get("last_page") is not None
    assert response.status_code == 200
    assert "SESAR" == first_fetched_thing["authority_id"]


def test_get_thing(client: TestClient, session: Session):
    response = client.get(f"/thing/{TEST_IGSN}")
    data = response.json()
    assert response.status_code == 200
    assert data.get("@id") is not None


def test_resolve_thing(client: TestClient, session: Session):
    response = client.get(f"/resolve/{TEST_IGSN}", allow_redirects=False)
    assert response.status_code == 302


def test_non_existent_thing(client: TestClient, session: Session):
    response = client.get("/thing/6666666")
    assert response.status_code == 404


def test_get_thing_page(client: TestClient, session: Session):
    response = client.get(f"/thingpage/{TEST_IGSN}")
    data = response.content
    assert response.status_code == 200
    assert len(data) > 0


def test_non_existent_thing_page(client: TestClient, session: Session):
    response = client.get("/thingpage/6666666")
    assert response.status_code == 404


def test_get_thing_core_format(client: TestClient, session: Session):
    response = client.get(f"/thing/{TEST_IGSN}?format=core")
    assert response.status_code == 200
    data = response.json()
    assert "iSamples" in data.get("$schema")
    assert data.get("keywords") is not None
    assert data.get("curation") is not None
    assert data.get("label") is not None


def test_get_thing_list_metadata(client: TestClient, session: Session):
    response = client.get("/thing")
    data = response.json()
    assert response.status_code == 200
    assert "authority" in data
    assert "status" in data


def test_get_thing_list_types(client: TestClient, session: Session):
    response = client.get("/thing/types")
    data = response.json()
    assert response.status_code == 200
    assert "item_type" in data[0]


def test_get_things_for_sitemap(client: TestClient, session: Session):
    response = client.request("POST", "/things", json={"identifiers": [TEST_IGSN]})
    assert response.status_code == 200
    response_data = response.json()
    assert response_data[0]["id"] == TEST_IGSN


def test_manage_logout(manage_client: TestClient, session: Session):
    headers = {
        "authorization": "Bearer 123456"
    }
    response = manage_client.get("/logout", headers=headers, allow_redirects=False)
    assert response.status_code == 307


def test_manage_login(manage_client: TestClient, session: Session):
    response = manage_client.get("/login", allow_redirects=False)
    assert response.status_code == 302


def test_manage_no_auth_header_protected(manage_client: TestClient, session: Session):
    response = manage_client.get("/userinfo", allow_redirects=False)
    assert response.status_code == 400


def test_manage_with_invalid_token_header(manage_client: TestClient, session: Session):
    headers = {
        "authorization": "Bearer 123456"
    }
    response = manage_client.get("/userinfo", headers=headers, allow_redirects=False)
    assert response.status_code == 401


def test_manage_with_invalid_authorization_header(manage_client: TestClient, session: Session):
    headers = {
        "authorization": "123456"
    }
    response = manage_client.get("/userinfo", headers=headers, allow_redirects=False)
    assert response.status_code == 400


def _assert_on_solr_response(mock_solr_query: MagicMock, response: Response):
    assert response.status_code == 200
    assert mock_solr_query.called is True


@patch("isb_web.isb_solr_query.solr_query")
def test_solr_select_get(mock_solr_query: MagicMock, client: TestClient, session: Session):
    response = client.get("/thing/select")
    _assert_on_solr_response(mock_solr_query, response)


@patch("isb_web.isb_solr_query.solr_query")
def test_solr_select_get_with_slash(mock_solr_query: MagicMock, client: TestClient, session: Session):
    response = client.get("/thing/select/")
    _assert_on_solr_response(mock_solr_query, response)


@patch("isb_web.isb_solr_query.solr_query")
def test_solr_select_post(mock_solr_query: MagicMock, client: TestClient, session: Session):
    response = client.post("/thing/select", headers={"Content-Type": "application/json; charset=utf-8"}, data=json.dumps({"foo": "bar"}))  # type: ignore
    _assert_on_solr_response(mock_solr_query, response)


# This is a test comment
@patch("isb_web.isb_solr_query.solr_query")
def test_solr_select_post_with_slash(mock_solr_query: MagicMock, client: TestClient, session: Session):
    response = client.post("/thing/select/", headers={"Content-Type": "application/json; charset=utf-8"}, data=json.dumps({"foo": "bar"}))  # type: ignore
    _assert_on_solr_response(mock_solr_query, response)


@patch("isb_web.isb_solr_query.solr_searchStream")
def test_solr_stream(mock_solr_query: MagicMock, client: TestClient, session: Session):
    response = client.get("/thing/stream")
    _assert_on_solr_response(mock_solr_query, response)
