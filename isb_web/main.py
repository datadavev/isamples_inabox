import os
import datetime
from json import JSONDecodeError
from typing import Optional

import term_store
import uvicorn
import typing
import requests
import fastapi
from fastapi.logger import logger as fastapi_logger
import fastapi.staticfiles
import fastapi.templating
import fastapi.middleware.cors
import fastapi.responses
import accept_types
import json
from fastapi.params import Query, Depends
from sqlmodel import Session
import geojson

import isb_web
import isamples_metadata.GEOMETransformer
from isamples_metadata.vocabularies import vocabulary_mapper
from isb_lib.core import MEDIA_GEO_JSON, MEDIA_JSON, MEDIA_NQUADS, SOLR_TIME_FORMAT
from isb_lib.models.thing import Thing
from isb_lib.utilities import h3_utilities
from isb_lib.utilities.url_utilities import full_url_from_suffix
from isb_lib.vocabulary import vocab_adapter
from isb_web import sqlmodel_database, analytics, manage, debug, metrics, vocabulary, export, auth
from isb_web.analytics import AnalyticsEvent
from isb_web import schemas
from isb_web import crud
from isb_web import config
from isb_web import isb_enums
from isb_web import isb_solr_query
from isb_web import profiles
from isamples_metadata.SESARTransformer import SESARTransformer
from isamples_metadata.OpenContextTransformer import OpenContextTransformer
from isamples_metadata.SmithsonianTransformer import SmithsonianTransformer

import logging

from isb_web.api_types import ThingsSitemapParams, ReliqueryResponse, ReliqueryParams
from isb_web.schemas import ThingPage
from isb_web.sqlmodel_database import SQLModelDAO, taxonomy_name_to_kingdom_map
import isb_lib.stac
from isb_web.vocabulary import SAMPLEDFEATURE_URI, MATERIAL_URI, PHYSICALSPECIMEN_URI

THIS_PATH = os.path.dirname(os.path.abspath(__file__))

# Setup logging from the config, but don't
# blowup if the logging config can't be found
try:
    logging.config.fileConfig(config.Settings().logging_config, disable_existing_loggers=False)
except (KeyError, FileNotFoundError):
    logging.warning("Could not load logging configuration")
    pass
L = logging.getLogger("ISB")

tags_metadata = [
    {
        "name": "things",
        "description": "Core Things functionality.  Things are the core data model in iSamples."
    },
    {
        "name": "vocabularies",
        "description": "API endpoints for vending iSamples controlled vocabularies.  See <a href=\"https://github.com/isamplesorg/vocabularies/\">iSamples Vocabularies</a> for more details."
    },
    {
        "name": "solr",
        "description": "Solr queries, see <a href=\"https://solr.apache.org/guide/8_11/common-query-parameters.html\">Solr documentation</a> for some common query parameters."
    },
    {
        "name": "heatmaps",
        "description": "Heatmap representations of Things, suitable for consumption by mapping APIs",
    },
    {
        "name": "metrics",
        "description": "Internal metrics, suitable for consumption by a <a href=\"https://prometheus.io\">prometheus.io</a> exporter"
    },
    {
        "name": "stac",
        "description": "STAC geospatial information spec support, more details at <a href=\"https://stacspec.org/\">STAC</a>"
    }
]

THING_URL_PATH = config.Settings().thing_url_path
STAC_ITEM_URL_PATH = config.Settings().stac_item_url_path
STAC_COLLECTION_URL_PATH = config.Settings().stac_collection_url_path

TAXONOMY_NAME_TO_KINGDOM_MAP = {}

app = fastapi.FastAPI(openapi_tags=tags_metadata)
dao = SQLModelDAO(None)
manage_app = manage.manage_api
debug_app = debug.debug_api
# Avoid a circular dependency but share the db connection by pushing into the various handlers
manage.main_app = app
manage.dao = dao
metrics.dao = dao
vocabulary.dao = dao
export.dao = dao

app.add_middleware(
    fastapi.middleware.cors.CORSMiddleware,
    allow_origins=config.Settings().cors_allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"]
)
app.add_middleware(
    analytics.AnalyticsMiddleware
)
app.mount(
    "/static",
    fastapi.staticfiles.StaticFiles(directory=os.path.join(THIS_PATH, "static")),
    name="static",
)
app.mount(
    "/sitemaps",
    fastapi.staticfiles.StaticFiles(
        directory=os.path.join(THIS_PATH, "sitemaps"), check_dir=False
    ),
    name="sitemaps",
)
templates = fastapi.templating.Jinja2Templates(
    directory=os.path.join(THIS_PATH, "templates")
)
app.mount(
    "/ui",
    fastapi.staticfiles.StaticFiles(
        directory=os.path.join(THIS_PATH, "ui"), check_dir=False, html=True
    ),
    name="ui",
)
app.mount(manage.MANAGE_PREFIX, manage_app)
app.mount(debug.DEBUG_PREFIX, debug_app)
app.include_router(metrics.router)
app.include_router(vocabulary.router)
app.mount(export.EXPORT_PREFIX, export.export_app)


@app.on_event("startup")
def on_startup():
    dao.connect_sqlmodel(isb_web.config.Settings().database_url)
    session = dao.get_session()
    orcid_ids = sqlmodel_database.all_orcid_ids(session)
    # preload each of these into memory to avoid performance issues on hyde
    repository = term_store.get_repository(session)
    vocab_adapter.uijson_vocabulary_dict(SAMPLEDFEATURE_URI, repository)
    vocab_adapter.uijson_vocabulary_dict(MATERIAL_URI, repository)
    vocab_adapter.uijson_vocabulary_dict(PHYSICALSPECIMEN_URI, repository)
    vocabulary_mapper.sampled_feature_type()
    vocabulary_mapper.material_type()
    vocabulary_mapper.specimen_type()
    # Force this into memory so it's cached when we need it later
    global TAXONOMY_NAME_TO_KINGDOM_MAP
    if config.Settings().taxon_cache_enabled:
        TAXONOMY_NAME_TO_KINGDOM_MAP = taxonomy_name_to_kingdom_map(session)
    session.close()
    # Superusers are allowed to mint identifiers as well, so make sure they're in the list.
    orcid_ids.extend(isb_web.config.Settings().orcid_superusers)
    # The main handler's startup is the guaranteed spot where we know we have a db connection.
    # User the connected db session to push in to the auth module's orcid_ids state.
    auth.allowed_orcid_ids = orcid_ids
    term_store.create_database(dao.engine)


def get_session():
    with dao.get_session() as session:
        yield session


def predicateToURI(p: str):
    """
    Convert a predicate to a URI for n-quads generation.

    TODO: This is a placeholder implementation.

    Args:
        p: predicate value

    Returns: URI-ified predicate
    """
    if (p.find(":")) >= 0:
        return p
    return f"https://isamples.org/def/predicates/{p}"


"""
@app.get("/oauth-callback", name="oauth-callback")
async def oauth_callback(access_token_state=fastapi.Depends(oauth2_authorize_callback)):
#async def oauth_callback(access_token_state):
    atoken, state = access_token_state
    print(f"TOKEN = {atoken}")
    print(f"STATE = {state}")
    token = await oauth2_client.get_access_token(atoken, "http://localhost:8000/")
    return token

@app.get("/login", include_in_schema=False)
async def login():
    url = await oauth2_client.get_authorization_url("http://localhost:8000/oauth-callback?state=%2ftest")
    print(url)
    return RedirectResponse(url)
"""


@app.get("/thingpage/{identifier:path}", include_in_schema=False)
async def get_thing_page(request: fastapi.Request, identifier: str, session: Session = Depends(get_session)):
    # Retrieve record from the database
    item = sqlmodel_database.get_thing_with_id(session, identifier)
    if item is None:
        raise fastapi.HTTPException(
            status_code=404, detail=f"Thing not found: {identifier}"
        )
    item_ispartof = "unknown"
    if item.id.startswith("ark:"):
        item_ispartof = "https://n2t.net"
    elif item.id.startswith("IGSN"):
        item_ispartof = "https://igsn.org"
    content = await thing_resolved_content(identifier, item, session)
    content_str = json.dumps(content)
    base_url = str(request.url)

    jwt_url = full_url_from_suffix(base_url, "/manage/hypothesis_jwt")
    login_url = full_url_from_suffix(base_url, f"/manage/login?thing={identifier}")
    logout_url = full_url_from_suffix(base_url, f"/manage/logout?thing={identifier}")

    return templates.TemplateResponse(
        "thing.html", {
            "request": request,
            "thing_json": content_str,
            "thing_identifier": item.id,
            "thing_ispartof": item_ispartof,
            "authority": config.Settings().hypothesis_authority,
            "isamples_jwt_url": jwt_url,
            "hypothesis_api_url": config.Settings().hypothesis_server_url,
            "login_url": login_url,
            "logout_url": logout_url,
        }
    )


@app.get(f"/{THING_URL_PATH}", response_model=schemas.ThingListMeta,
         summary="Summary query of thing counts by authority", tags=["things"])
async def thing_list_metadata(
    request: fastapi.Request,
    session: Session = Depends(get_session),
):
    """Information about list identifiers"""
    meta = sqlmodel_database.get_thing_meta(session)
    analytics.attach_analytics_state_to_request(AnalyticsEvent.THING_LIST_METADATA, request)
    return meta


@app.get(f"/{THING_URL_PATH}/", response_model=ThingPage, summary="Query lists of things", tags=["things"])
def thing_list(
    request: fastapi.Request,
    offset: int = fastapi.Query(0, ge=0),
    limit: int = fastapi.Query(1000, lt=10000, gt=0),
    status: int = 200,
    authority: str = fastapi.Query(None),
    session: Session = Depends(get_session),
):
    total_records, npages, things = sqlmodel_database.read_things_summary(
        session, offset, limit, status, authority
    )
    properties = {
        "authority": authority or "None"
    }
    analytics.attach_analytics_state_to_request(AnalyticsEvent.THING_LIST, request, properties)
    params = {
        "limit": limit,
        "offset": offset,
        "status": status,
        "authority": authority,
    }
    return {
        "params": params,
        "last_page": npages,
        "total_records": total_records,
        "data": things,
    }


@app.get(f"/{THING_URL_PATH}/types", response_model=typing.List[schemas.ThingType], summary="List of thing types", tags=["things"])
async def thing_list_types(
    request: fastapi.Request,
    session: Session = Depends(get_session),
):
    """List of types of things with counts"""
    analytics.attach_analytics_state_to_request(AnalyticsEvent.THING_LIST_TYPES, request)
    return sqlmodel_database.get_sample_types(session)


async def _get_solr_select(request: fastapi.Request):
    """Send select request to the Solr isb_core_records collection.

    See https://solr.apache.org/guide/8_11/common-query-parameters.html
    """
    if request.method == "POST":
        params = []
        properties = {"q": "*:*"}
        await _handle_post_solr_select(params, properties, request)
    else:
        params, properties = isb_solr_query.get_solr_params_from_request(request)
    logging.warning(params)
    analytics.attach_analytics_state_to_request(AnalyticsEvent.THING_SOLR_SELECT, request, properties)

    # response object is generated in the called method. This is necessary
    # for the streaming response as otherwise the iterator is consumed
    # before returning here, hence defeating the purpose of the streaming
    # response.
    return isb_solr_query.solr_query(params)


async def _handle_post_solr_select(params, properties, request):
    body = await request.body()
    content_type = request.headers.get("Content-Type")
    if content_type is None:
        raise fastapi.HTTPException(
            status_code=400,
            detail="Content-Type header not present.  application/json is only supported Content-Type."
        )
    if not content_type.startswith("application/json"):
        raise fastapi.HTTPException(
            status_code=400,
            detail=f"application/json is only supported Content-Type. {content_type} is not supported."
        )
    if content_type is not None and content_type.startswith("application/json"):
        try:
            json_body = json.loads(body)
        except JSONDecodeError as e:
            raise fastapi.HTTPException(
                status_code=400,
                detail=f"Request body is not valid JSON: {e}"
            )
        for k, v in json_body.items():
            params.append([k, v])
            if k in properties:
                properties[k] = v


# TODO: Don't blindly accept user input!
@app.get(f"/{THING_URL_PATH}/select", response_model=typing.Any, tags=["solr"], summary="Thing query GET, query is read off query parameters")
@app.get(f"/{THING_URL_PATH}/select/", response_model=typing.Any, tags=["solr"], summary="Thing query GET, query is read off query parameters")
@app.post(f"/{THING_URL_PATH}/select/", response_model=typing.Any, tags=["solr"], summary="Thing query POST, request body must be encoded JSON")
@app.post(f"/{THING_URL_PATH}/select", response_model=typing.Any, tags=["solr"], summary="Thing query POST, request body must be encoded JSON")
async def get_solr_select(request: fastapi.Request):
    return await _get_solr_select(request)


@app.post(f"/{THING_URL_PATH}/reliquery", response_model=ReliqueryResponse, tags=["solr"])
def get_reliquery(request: fastapi.Request, params: ReliqueryParams) -> ReliqueryResponse:
    timestamp_str = datetime.datetime.now().strftime(SOLR_TIME_FORMAT)
    if params.previous_response is not None:
        query = params.previous_response.query
        description = f"{params.previous_response.description} re-executed at {timestamp_str}"
    else:
        query = params.query
        description = params.description
    solr_response_dict = isb_solr_query.reliquery_solr_query(query)
    json_response = solr_response_dict.get("response")
    docs = json_response.get("docs")
    identifiers = []
    for response_dict in docs:
        identifiers.append(response_dict["id"])
    reliquery_response = ReliqueryResponse(
        query=query,
        identifiers=identifiers,
        timestamp=timestamp_str,
        count=json_response.get("numFound"),
        return_count=len(identifiers),
        url=str(request.url),
        description=description)
    if params.previous_response is not None:
        reliquery_response.previous_count = params.previous_response.count
        reliquery_response.previous_url = params.previous_response.url
        reliquery_response.previous_timestamp = params.previous_response.timestamp
    return reliquery_response


@app.get(f"/{THING_URL_PATH}/stream", response_model=typing.Any, tags=["solr"], summary="Make a streaming request to the solr index")
async def get_solr_stream(request: fastapi.Request):
    '''
    Make a streaming request to the solr index.

    The Solr streaming API offers much richer interaction with the index, though
    also adds risk that a request may perform harmful actions. Here only
    search expressions are constructed from parameters similar to the select API
    to limit potential harm to the index.

    See https://solr.apache.org/guide/8_11/streaming-expressions.html

    Args:
        request: The http request

    Returns:
        fastapi.responses.StreamingResponse
    '''
    # Note that there may be duplicate keys in params, e.g. multiple fq=
    defparams = {
        "wt": "json",
        "q": "*:*",
        "fl": [
            "id",
            f"x:{isb_solr_query.LONGITUDE_FIELD}",
            f"y:{isb_solr_query.LATITUDE_FIELD}",
        ],
        "rows": isb_solr_query.MAX_STREAMING_ROWS,
        "start": 0,
        "select": "search",

        # if true, return counts per location
        "xycount": False,

        # if true, return only records with latitude,longitude
        "onlyxy": True,
    }
    properties = {
        "q": defparams["q"]
    }
    params = []
    # Update params with the provided parameters
    for k, v in request.query_params.multi_items():
        params.append([k, v])
        if k in properties:
            properties[k] = v
    params = isb_solr_query.set_default_params(params, defparams)
    # L.debug("Params: %s", params)
    analytics.attach_analytics_state_to_request(AnalyticsEvent.THING_SOLR_STREAM, request, properties)
    response = isb_solr_query.solr_searchStream(params)
    return fastapi.responses.StreamingResponse(
        response.iter_content(chunk_size=4096), media_type=MEDIA_JSON
    )


@app.get(f"/{THING_URL_PATH}/select/info", response_model=typing.Any, tags=["solr"], summary="Retrieve information about the solr schema")
async def get_solr_luke_info(request: fastapi.Request):
    """Retrieve information about the record schema.

    Returns: JSON
    """
    analytics.attach_analytics_state_to_request(AnalyticsEvent.THING_SOLR_LUKE_INFO, request)
    return isb_solr_query.solr_luke()


resolution_q = fastapi.Query(
    default=None,
    ge=0,
    le=16,
    description="H3 resolution. Default is to infer from min_cells.",
)
exclude_poles_q = fastapi.Query(
    default=True, description="Exclude cells containing the north or south poles."
)
bb_q = fastapi.Query(
    default=None,
    regex=h3_utilities.BB_REGEX,
    description="Bounding box for cells (minx, miny, maxx, maxy)",
    example="-150,-37,150,37",
)
min_cells_q = fastapi.Query(
    default=10, gt=0, lt=10000, description="Minimum number of cells to return."
)


@app.get(
    "/h3_counts/",
    name="Get H3 GeoJSON grid of record counts",
    description=("Return a GeoJSON feature collection of H3 cells for the provided bounding box or globally. "
                 "bb is min_longitude, min_latitude, max_longitude, max_latitude"
                 ),
    tags=["heatmaps"]
)
def get_h3_grid(
        resolution: typing.Optional[int] = resolution_q,
        exclude_poles: bool = exclude_poles_q,
        bb: typing.Optional[str] = bb_q,
        q: str = None,
) -> geojson.FeatureCollection:
    solr_query_params = h3_utilities.get_h3_solr_query_from_bb(bb, resolution, q)
    record_counts = h3_utilities.get_record_counts(
        query=solr_query_params.q, resolution=solr_query_params.resolution, exclude_poles=exclude_poles
    )
    return h3_utilities.h3s_to_feature_collection(
        set(record_counts.keys()), cell_props=record_counts
    )


@app.post("/things", response_model=typing.Any, tags=["things"])
async def get_things_for_sitemap(
    request: fastapi.Request,
    params: ThingsSitemapParams,
    session: Session = Depends(get_session),
):
    """Returns batched things suitable for sitemap ingestion
    Args:
        request: The fastapi request
        params: Class that contains the identifier list, JSON-encoded in the request body
        session: The database session to use to fetch things
    """
    content = sqlmodel_database.get_things_with_ids(session, params.identifiers)
    # things
    # for identifier in params.identifiers:
    #     thing = sqlmodel_database.get_thing_with_id(session, identifier)
    #     if thing is not None:
    #         content.append(thing)
    #     else:
    #         logging.error(f"No thing with identifier {identifier}")
    return content


def all_profiles_json_response(request_url: str):
    query_string_index = request_url.find("?")
    if query_string_index != -1:
        request_url = request_url[0:query_string_index]
    headers = profiles.get_all_profiles_response_headers(request_url)
    return fastapi.responses.JSONResponse(
        content={}, media_type=MEDIA_JSON, headers=headers
    )


def solr_thing_response(identifier: str):
    # Return solr representation of the record
    # Get the solr response, and return the doc portion or
    # and appropriate error condition
    status, doc = isb_solr_query.solr_get_record(identifier)
    if status == 200:
        return fastapi.responses.JSONResponse(
            content=doc, media_type="application/json"
        )
    raise fastapi.HTTPException(
        status_code=status,
        detail=f"Unable to retrieve solr record for identifier: {identifier}"
    )


@app.head(f"/{THING_URL_PATH}/{{identifier:path}}", response_model=typing.Any, tags=["things"])
@app.get(f"/{THING_URL_PATH}/{{identifier:path}}", response_model=typing.Any, tags=["things"])
async def get_thing(
    request: fastapi.Request,
    identifier: str,
    format: typing.Optional[isb_enums.ISBFormat] = None,
    _profile: Optional[str] = None,
    session: Session = Depends(get_session),
):
    properties = {
        "identifier": identifier
    }
    analytics.attach_analytics_state_to_request(AnalyticsEvent.THING_BY_IDENTIFIER, request, properties)
    """Record for the specified identifier"""
    if format == isb_enums.ISBFormat.SOLR:
        return solr_thing_response(identifier)

    if _profile == profiles.ALL_PROFILES_QSA_VALUE or _profile == profiles.ALT_PROFILES_QSA_VALUE \
            or request.method == "HEAD":
        return all_profiles_json_response(str(request.url))
    request_profile = profiles.get_profile_from_qsa(_profile)
    if request_profile is None:
        # didn't find in qsa, check headers
        request_profile = profiles.get_profile_from_http(request)

    # Retrieve record from the database
    item = sqlmodel_database.get_thing_with_id(session, identifier)
    if item is None:
        raise fastapi.HTTPException(
            status_code=404, detail=f"Thing not found: {identifier}"
        )
    if format == isb_enums.ISBFormat.FULL:
        return item
    if (request_profile is not None and request_profile == profiles.ISAMPLES_PROFILE) or \
            format == isb_enums.ISBFormat.CORE:
        if request_profile is None:
            request_profile = profiles.ISAMPLES_PROFILE
        content = await thing_resolved_content(identifier, item, session)
    else:
        # If no profile explicitly requested, use the default profile here (currently original source)
        if request_profile is None:
            request_profile = profiles.DEFAULT_PROFILE
        content = item.resolved_content
    headers = {}
    if request_profile is not None:
        headers.update(profiles.content_profile_headers(request_profile))
    return fastapi.responses.JSONResponse(
        content=content, media_type=item.resolved_media_type, headers=headers
    )


@app.get("/resolve/{identifier:path}", response_model=typing.Any, tags=["things"])
async def resolve_thing(
    request: fastapi.Request,
    identifier: str,
    full: bool = False,
    format: typing.Optional[isb_enums.ISBFormat] = None,
    _profile: Optional[str] = None,
    session: Session = Depends(get_session),
):
    url_str = str(request.url).replace("/resolve/", f"/{config.Settings().thing_url_path}/")
    headers = {
        "Location": url_str
    }
    do_redirect_header = request.headers.get("Do-Redirect")
    if do_redirect_header is not None and (do_redirect_header == "0" or do_redirect_header == "false"):
        return fastapi.responses.Response(headers=headers)
    return fastapi.responses.RedirectResponse(url=url_str, status_code=302, headers=headers)


async def thing_resolved_content(identifier: str, item: Thing, session: Session) -> dict:
    authority_id = item.authority_id
    if authority_id == "SESAR":
        content = SESARTransformer(item.resolved_content).transform(False)
    elif authority_id == "GEOME":
        content = (
            isamples_metadata.GEOMETransformer.geome_transformer_for_identifier(
                identifier, item.resolved_content, session, TAXONOMY_NAME_TO_KINGDOM_MAP
            ).transform(False)
        )
    elif authority_id == "OPENCONTEXT":
        content = OpenContextTransformer(item.resolved_content).transform(False)
    elif authority_id == "SMITHSONIAN":
        content = SmithsonianTransformer(item.resolved_content).transform(False)
    else:
        raise fastapi.HTTPException(
            status_code=400,
            detail=f"Core format not available for authority_id: {authority_id}",
        )
    return content


@app.get(f"/{STAC_ITEM_URL_PATH}/{{identifier:path}}", response_model=typing.Any, tags=["stac"])
async def get_stac_item(
    request: fastapi.Request,
    identifier: str,
    session: Session = Depends(get_session),
):
    properties = {
        "identifier": identifier
    }
    analytics.attach_analytics_state_to_request(AnalyticsEvent.STAC_ITEM_BY_IDENTIFIER, request, properties)
    # stac wants things to have filenames, so let these requests work, too.
    if identifier.endswith(".json"):
        identifier = identifier.removesuffix(".json")
    status, doc = isb_solr_query.solr_get_record(identifier)
    if status == 200:
        stac_item = isb_lib.stac.stac_item_from_solr_dict(
            doc, "http://isamples.org/stac/", "http://isamples.org/thing/"
        )
        if stac_item is not None:
            return fastapi.responses.JSONResponse(
                content=stac_item, media_type=MEDIA_GEO_JSON
            )
        else:
            # We don't have location data to make a stac item, return a 404
            status = 404

    raise fastapi.HTTPException(
        status_code=status,
        detail=f"Unable to retrieve stac item for identifier: {identifier}"
    )


@app.get(f"/{STAC_COLLECTION_URL_PATH}/{{filename:path}}", response_model=typing.Any, tags=["stac"])
def get_stac_collection(
    request: fastapi.Request,
    offset: int = fastapi.Query(0, ge=0),
    limit: int = fastapi.Query(1000, lt=10000, gt=0),
    authority: str = fastapi.Query(None),
    filename: Optional[str] = None
):
    properties = {
        "authority": authority or "None"
    }
    analytics.attach_analytics_state_to_request(AnalyticsEvent.STAC_COLLECTION, request, properties)

    solr_docs, has_next = isb_solr_query.solr_records_for_stac_collection(authority, offset, limit)
    stac_collection = isb_lib.stac.stac_collection_from_solr_dicts(solr_docs, has_next, offset, limit, authority)
    return fastapi.responses.JSONResponse(
        content=stac_collection, media_type=MEDIA_JSON
    )


@app.get(
    "/things_geojson_heatmap",
    response_model=typing.Any,
    summary="Gets a GeoJSON heatmap of Things",
    tags=["heatmaps"],
)
async def get_things_geojson_heatmap(
    query: str = Query(
        default="*:*",
        description="Solr query to use for selecting Things. Details at https://solr.apache.org/guide/6_6/the-standard-query-parser.html#the-standard-query-parser",
    ),
    fq: str = Query(
        default="",
        description="Filter query to use for selecting Things. Details at https://solr.apache.org/guide/8_9/the-standard-query-parser.html#the-standard-query-parser",
    ),
    min_lat: float = Query(
        default=-90.0,
        description="The minimum latitude for the bounding box in the Solr query. Valid values are -90.0 <= min_lat <= 90.",
    ),
    max_lat: float = Query(
        default=90.0,
        description="The maximum latitude for the bounding box in the Solr query. Valid values are -90.0 <= max_lat <= 90.",
    ),
    min_lon: float = Query(
        default=-180.0,
        description="The minimum longitude for the bounding box in the Solr query. Valid values are -180.0 <= min_lon <= 180.",
    ),
    max_lon: float = Query(
        default=180.0,
        description="The maximum longitude for the bounding box in the Solr query. Valid values are -180.0 <= max_lon <= 180.",
    ),
):
    """
    Returns a GeoJSON heatmap of all Things matching the specified Solr query in the bounding box described by the
    latitude and longitude parameters.  The format of the response is a GeoJSON Feature Collection:
    https://datatracker.ietf.org/doc/html/rfc7946#section-3.3
    """
    bounds = {
        isb_solr_query.MIN_LAT: min_lat,
        isb_solr_query.MAX_LAT: max_lat,
        isb_solr_query.MIN_LON: min_lon,
        isb_solr_query.MAX_LON: max_lon,
    }
    results = isb_solr_query.solr_geojson_heatmap(
        query, bounds, fq=fq, grid_level=None, show_bounds=False, show_solr_bounds=False
    )
    return fastapi.responses.JSONResponse(content=results, media_type=MEDIA_GEO_JSON)


@app.get(
    "/things_leaflet_heatmap",
    response_model=typing.Any,
    summary="Gets a Leaflet heatmap of Things",
    tags=["heatmaps"],
)
async def get_things_leaflet_heatmap(
    query: str = Query(
        default="*:*",
        description="Solr query to use for selecting Things. Details at https://solr.apache.org/guide/8_9/the-standard-query-parser.html#the-standard-query-parser",
    ),
    fq: str = Query(
        default="",
        description="Filter query to use for selecting Things. Details at https://solr.apache.org/guide/8_9/the-standard-query-parser.html#the-standard-query-parser",
    ),
    min_lat: float = Query(
        default=-90.0,
        description="The minimum latitude for the bounding box in the Solr query. Valid values are -90.0 <= min_lat <= 90.",
    ),
    max_lat: float = Query(
        default=90.0,
        description="The maximum latitude for the bounding box in the Solr query. Valid values are -90.0 <= max_lat <= 90.",
    ),
    min_lon: float = Query(
        default=-180.0,
        description="The minimum longitude for the bounding box in the Solr query. Valid values are -180.0 <= min_lon <= 180.",
    ),
    max_lon: float = Query(
        default=180.0,
        description="The maximum longitude for the bounding box in the Solr query. Valid values are -180.0 <= max_lon <= 180.",
    ),
):
    """
    Returns a Leaflet heatmap of all Things matching the specified Solr query in the bounding box described by the
    latitude and longitude parameters.  The format of the response is suitable for consumption by the Leaflet JavaScript
    library https://leafletjs.com
    """
    bounds = {
        isb_solr_query.MIN_LAT: min_lat,
        isb_solr_query.MAX_LAT: max_lat,
        isb_solr_query.MIN_LON: min_lon,
        isb_solr_query.MAX_LON: max_lon,
    }
    results = isb_solr_query.solr_leaflet_heatmap(query, bounds, fq=fq, grid_level=None)
    return fastapi.responses.JSONResponse(content=results, media_type=MEDIA_JSON)


@app.get(
    "/related",
    response_model=typing.List[schemas.RelationListMeta],
    tags=["things"]
)
# async def relation_metadata(db: sqlalchemy.orm.Session = fastapi.Depends((getDb))):
async def relation_metadata(request: fastapi.Request):
    """List of predicates with counts"""
    # return crud.getPredicateCounts(db)
    analytics.attach_analytics_state_to_request(AnalyticsEvent.RELATION_METADATA, request)
    session = requests.session()
    return crud.getPredicateCountsSolr(session)


'''
@app.get(
    "/related/",
    response_model=typing.List[schemas.RelationListEntry],
    responses={
        200: {
            "content": {
                MEDIA_NQUADS: {
                    "example": "s p o [name]  .\n"
                    "s p o [name]  .\n"
                    "s p o [name]  .\n"
                }
            }
        }
    },
)
async def get_related(
    db: sqlalchemy.orm.Session = fastapi.Depends((getDb)),
    s: str = None,
    p: str = None,
    o: str = None,
    source: str = None,
    name: str = None,
    offset: int = 0,
    limit: int = 1000,
    accept: typing.Optional[str] = fastapi.Header(MEDIA_JSON),
):
    """Relations that match provided s, p, o, source, name.

    Each property is optional. Exact matches only.
    """
    return_type = accept_types.get_best_match(accept, [MEDIA_JSON, MEDIA_NQUADS])
    res = crud.getRelations(db, s, p, o, source, name, offset, limit)
    if return_type == MEDIA_NQUADS:
        rows = []
        for row in res:
            quad = [row.s, predicateToURI(row.p), row.o]
            if row.name is not None or row.name != "":
                quad.append(row.name)
            quad.append(".")
            rows.append(" ".join(quad))
        return fastapi.responses.PlainTextResponse(
            "\n".join(rows), media_type=MEDIA_NQUADS
        )
    return res
'''


@app.get(
    "/related/",
    response_model=typing.List[schemas.RelationListEntry],
    responses={
        200: {
            "content": {
                MEDIA_NQUADS: {
                    "example": "s p o [name]  .\n"
                    "s p o [name]  .\n"
                    "s p o [name]  .\n"
                }
            }
        }
    },
    tags=["things"]
)
async def get_related_solr(
    request: fastapi.Request,
    s: Optional[str] = None,
    p: Optional[str] = None,
    o: Optional[str] = None,
    source: Optional[str] = None,
    name: Optional[str] = None,
    offset: int = 0,
    limit: int = 1000,
    accept: typing.Optional[str] = fastapi.Header(MEDIA_JSON),
):
    """Relations that match provided s, p, o, source, name.

    Each property is optional. Exact matches only.
    """
    analytics.attach_analytics_state_to_request(AnalyticsEvent.RELATED_SOLR, request)
    return_type = accept_types.get_best_match(accept, [MEDIA_JSON, MEDIA_NQUADS])
    rsession = requests.Session()
    res = crud.getRelationsSolr(rsession, s, p, o, source, name, offset, limit)
    if return_type == MEDIA_NQUADS:
        rows = []
        for row in res:
            quad = [row.s, predicateToURI(row.p), row.o]
            if row.name is not None or row.name != "":
                quad.append(row.name)
            quad.append(".")
            rows.append(" ".join(quad))
        return fastapi.responses.PlainTextResponse(
            "\n".join(rows), media_type=MEDIA_NQUADS
        )
    return res


@app.get("/map", include_in_schema=False)
async def map(request: fastapi.Request):
    return templates.TemplateResponse("spatial.html", {"request": request})


@app.get("/records_orig", include_in_schema=False)
async def records_orig(request: fastapi.Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/", include_in_schema=False)
async def root(request: fastapi.Request):
    return fastapi.responses.RedirectResponse(url=f"{request.scope.get('root_path')}/docs")


def main():
    logging.basicConfig(level=logging.DEBUG)
    uvicorn.run("isb_web.main:app", host="0.0.0.0", port=8000, reload=True)


if __name__ == "__main__":
    formatter = logging.Formatter(
        "[%(asctime)s.%(msecs)03d] %(levelname)s [%(thread)d] - %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )
    handler = (
        logging.StreamHandler()
    )  # RotatingFileHandler('/log/abc.log', backupCount=0)
    logging.getLogger().setLevel(logging.NOTSET)
    # fastapi_logger.addHandler(handler)
    # handler.setFormatter(formatter)

    # gunicorn_error_logger = logging.getLogger("gunicorn.error")
    # gunicorn_logger = logging.getLogger("gunicorn")
    # uvicorn_access_logger = logging.getLogger("uvicorn.access")
    # uvicorn_access_logger.handlers = gunicorn_error_logger.handlers

    # fastapi_logger.handlers = gunicorn_error_logger.handlers

    fastapi_logger.info("****************** Starting Server *****************")
    main()
