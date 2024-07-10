import asyncio
import datetime
import json
import logging
import os.path
import traceback
import urllib
from typing import Optional
import concurrent
from urllib.error import HTTPError
from urllib.request import urlopen

import fastapi.responses
import igsn_lib.time
import petl
import ijson
import time
from fastapi import Depends, FastAPI, HTTPException
from sqlmodel import Session
from starlette.middleware.gzip import GZipMiddleware
from starlette.responses import JSONResponse, FileResponse
from starlette.status import HTTP_201_CREATED, HTTP_404_NOT_FOUND, HTTP_200_OK, HTTP_202_ACCEPTED

import isb_web
from isamples_metadata.solr_field_constants import SOLR_ID, SOLR_LABEL, SOLR_HAS_CONTEXT_CATEGORY, \
    SOLR_HAS_MATERIAL_CATEGORY, SOLR_HAS_SPECIMEN_CATEGORY, SOLR_KEYWORDS, SOLR_INFORMAL_CLASSIFICATION, \
    SOLR_REGISTRANT, SOLR_PRODUCED_BY_RESPONSIBILITY, SOLR_PRODUCED_BY_DESCRIPTION, SOLR_PRODUCED_BY_RESULT_TIME, \
    SOLR_AUTHORIZED_BY, SOLR_COMPLIES_WITH, SOLR_PRODUCED_BY_SAMPLING_SITE_LOCATION_LONGITUDE, \
    SOLR_PRODUCED_BY_SAMPLING_SITE_LOCATION_LATITUDE, SOLR_RELATED_RESOURCE_ISB_CORE_ID, SOLR_CURATION_RESPONSIBILITY, \
    SOLR_CURATION_LOCATION, SOLR_CURATION_ACCESS_CONSTRAINTS, SOLR_CURATION_DESCRIPTION, SOLR_CURATION_LABEL, \
    SOLR_SAMPLING_PURPOSE, SOLR_PRODUCED_BY_SAMPLING_SITE_PLACE_NAME, \
    SOLR_PRODUCED_BY_SAMPLING_SITE_ELEVATION_IN_METERS, SOLR_PRODUCED_BY_SAMPLING_SITE_LABEL, \
    SOLR_PRODUCED_BY_SAMPLING_SITE_DESCRIPTION, SOLR_PRODUCED_BY_FEATURE_OF_INTEREST, SOLR_PRODUCED_BY_LABEL, \
    SOLR_PRODUCED_BY_ISB_CORE_ID, SOLR_DESCRIPTION, SOLR_SOURCE
from isb_lib.models.export_job import ExportJob
from isb_lib.sitemaps import _build_sitemap
from isb_lib.sitemaps.thing_sitemap import MAX_URLS_IN_SITEMAP, ThingSitemapIndexIterator
from isb_lib.utilities.solr_result_transformer import SolrResultTransformer, TargetExportFormat
from isb_web import isb_solr_query, analytics, sqlmodel_database, auth
from isb_web.analytics import AnalyticsEvent
from isb_web.sqlmodel_database import SQLModelDAO

EXPORT_PREFIX = "/export"
export_app = FastAPI(prefix=EXPORT_PREFIX)
auth.add_auth_middleware_to_app(export_app)
export_app.add_middleware(GZipMiddleware)
dao: Optional[SQLModelDAO] = None
DEFAULT_SOLR_FIELDS_FOR_EXPORT = [SOLR_ID, SOLR_AUTHORIZED_BY, SOLR_COMPLIES_WITH, SOLR_PRODUCED_BY_SAMPLING_SITE_LOCATION_LONGITUDE, SOLR_PRODUCED_BY_SAMPLING_SITE_LOCATION_LATITUDE, SOLR_RELATED_RESOURCE_ISB_CORE_ID, SOLR_CURATION_RESPONSIBILITY, SOLR_CURATION_LOCATION, SOLR_CURATION_ACCESS_CONSTRAINTS, SOLR_CURATION_DESCRIPTION, SOLR_CURATION_LABEL, SOLR_SAMPLING_PURPOSE, SOLR_REGISTRANT, SOLR_PRODUCED_BY_SAMPLING_SITE_PLACE_NAME, SOLR_PRODUCED_BY_SAMPLING_SITE_ELEVATION_IN_METERS, SOLR_PRODUCED_BY_SAMPLING_SITE_LABEL, SOLR_PRODUCED_BY_SAMPLING_SITE_DESCRIPTION, SOLR_PRODUCED_BY_RESULT_TIME, SOLR_PRODUCED_BY_RESPONSIBILITY, SOLR_PRODUCED_BY_FEATURE_OF_INTEREST, SOLR_PRODUCED_BY_DESCRIPTION, SOLR_PRODUCED_BY_LABEL, SOLR_PRODUCED_BY_ISB_CORE_ID, SOLR_INFORMAL_CLASSIFICATION, SOLR_KEYWORDS, SOLR_HAS_SPECIMEN_CATEGORY, SOLR_HAS_MATERIAL_CATEGORY, SOLR_HAS_CONTEXT_CATEGORY, SOLR_DESCRIPTION, SOLR_LABEL, SOLR_SOURCE]
MINIMAL_SOLR_FIELDS_FOR_EXPORT = [SOLR_ID, SOLR_PRODUCED_BY_SAMPLING_SITE_LOCATION_LATITUDE, SOLR_PRODUCED_BY_SAMPLING_SITE_LOCATION_LONGITUDE, SOLR_SOURCE]
INITIAL_CURSOR_MARK = "*"


def get_session():
    with dao.get_session() as session:
        yield session


executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)


def search_solr_and_export_results(export_job_id: str):
    try:
        _search_solr_and_export_results(export_job_id)
    except Exception as e:
        logging.error(f"Exception exporting job {export_job_id}, error: {e}")
        traceback.print_tb(e.__traceback__)


def _handle_error(session: Session, export_job: ExportJob, error: str):
    export_job.tcompleted = datetime.datetime.now()
    export_job.error = error
    sqlmodel_database.save_or_update_export_job(session, export_job)


def _search_solr_and_export_results(export_job_id: str):
    """Task function that gets a queued export job from the db, executes the solr query, and writes results to disk"""

    # note that we don't seem to be able to work with the session generator on the background thread, so explicitly
    # open and close a new session for each task we execute
    with dao.get_session() as session:  # type: ignore
        export_job = sqlmodel_database.export_job_with_uuid(session, export_job_id)
        if export_job is not None:
            export_job.tstarted = igsn_lib.time.dtnow()
            sqlmodel_database.save_or_update_export_job(session, export_job)
            start_time = time.time()
            solr_query_params = export_job.solr_query_params
            encoded_params = urllib.parse.urlencode(solr_query_params)  # type: ignore
            export_handler = isb_solr_query.get_solr_url("export")
            full_url = f"{export_handler}?{encoded_params}"
            try:
                src = urlopen(full_url)
            except HTTPError as e:
                _handle_error(session, export_job, f"HTTP Error, code: {e.code} reason: {e.reason}")
                return
            except Exception as e:
                _handle_error(session, export_job, f"Export Error {str(e)}")
                return
            docs = ijson.items(src, "response.docs.item", use_float=True)
            generator_docs = (doc for doc in docs)
            if export_job.is_sitemap:
                current_date = datetime.datetime.now().date()
                formatted_date = current_date.strftime("%Y-%m-%d")
                transformed_response_path = os.path.join(isb_web.config.Settings().sitemap_dir_prefix, formatted_date)
                if not os.path.exists(transformed_response_path):
                    os.mkdir(transformed_response_path)
            else:
                transformed_response_path = f"/tmp/{export_job.uuid}"
            table = petl.fromdicts(generator_docs)
            lines_per_file = -1 if not export_job.is_sitemap else MAX_URLS_IN_SITEMAP
            solr_result_transformer = SolrResultTransformer(table, TargetExportFormat[export_job.export_format], transformed_response_path, False, export_job.is_sitemap, lines_per_file)  # type: ignore
            file_path = solr_result_transformer.transform()[0]
            export_job.file_path = file_path
            print("Finished writing query response!")
            table_length = petl.util.counting.nrows(table)
            finish_time = time.time()
            logging.info(f"Chunk of {table_length} rows starting at index 0 completed fetching and writing in {finish_time - start_time} seconds")
            if export_job.is_sitemap:
                sitemap_index_iterator = ThingSitemapIndexIterator(transformed_response_path)
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(_build_sitemap(transformed_response_path, isb_web.config.Settings().sitemap_url_prefix, sitemap_index_iterator))
            export_job.tcompleted = igsn_lib.time.dtnow()
            sqlmodel_database.save_or_update_export_job(session, export_job)


@export_app.get("/create")
async def create(request: fastapi.Request, export_format: TargetExportFormat = TargetExportFormat.JSONL,
                 session: Session = Depends(get_session)) -> JSONResponse:
    """Creates a new export job with the specified solr query"""

    if request.query_params.get("sort") is not None:
        raise fastapi.HTTPException(
            status_code=415, detail="Sort field not supported for export"
        )

    # supported parameters are: q, fq, start, rows, format (right now format should be either CSV or JSON)

    # These will be inserted into the solr request if not present on the API call
    solr_api_defparams = _default_export_params("indexUpdatedTime asc")
    params, properties = isb_solr_query.get_solr_params_from_request_as_dict(request, solr_api_defparams, ["q", "fq", "start", "rows", "fl"])
    analytics.attach_analytics_state_to_request(AnalyticsEvent.THINGS_DOWNLOAD, request, properties)
    return await _create_export_job(export_format, params, request, session)


def _default_export_params(sort: str) -> dict[str, str]:
    solr_api_defparams = {
        "wt": "json",
        "q": "*:*",
        "fl": ",".join(DEFAULT_SOLR_FIELDS_FOR_EXPORT),
        "sort": sort
    }
    return solr_api_defparams


async def _create_export_job(export_format: TargetExportFormat, params: dict[str, str], request: fastapi.Request,
                             session: Session, is_sitemap: bool = False):
    export_job = ExportJob()
    export_job.creator_id = auth.orcid_id_from_session_or_scope(request)
    export_job.solr_query_params = params  # type: ignore
    export_job.export_format = export_format.value
    export_job.is_sitemap = is_sitemap
    sqlmodel_database.save_or_update_export_job(session, export_job)
    executor.submit(search_solr_and_export_results, export_job.uuid)  # type: ignore
    status_dict = {"status": "created", "uuid": export_job.uuid}
    return fastapi.responses.JSONResponse(content=status_dict, status_code=HTTP_201_CREATED)


@export_app.get("/create_sitemap")
async def create_sitemap(request: fastapi.Request, session: Session = Depends(get_session)) -> JSONResponse:
    orcid_id = auth.orcid_id_from_session_or_scope(request)
    if orcid_id not in isb_web.config.Settings().orcid_superusers:
        raise HTTPException(401, "orcid id not authorized to create sitemap")
    solr_api_defparams = _default_export_params("indexUpdatedTime asc")
    # remove this
    solr_api_defparams["q"] = "searchText:Tucson"
    return await _create_export_job(TargetExportFormat.JSONL, solr_api_defparams, request, session, True)


def _not_found_response() -> JSONResponse:
    return fastapi.responses.JSONResponse(content={"status": "not_found"}, status_code=HTTP_404_NOT_FOUND)


@export_app.get("/status")
def status(uuid: str = fastapi.Query(None), session: Session = Depends(get_session)) -> JSONResponse:
    """Looks up the status of the export job with the specified uuid"""
    export_job = sqlmodel_database.export_job_with_uuid(session, uuid)
    if export_job is None:
        return _not_found_response()
    else:
        if export_job.tcompleted is not None:
            if export_job.error is None:
                content = {"status": "completed", "tcompleted": str(export_job.tcompleted), "query": json.dumps(export_job.solr_query_params)}
            else:
                content = {"status": "error", "tcompleted": str(export_job.tcompleted), "reason": export_job.error}
            return fastapi.responses.JSONResponse(content=content, status_code=HTTP_200_OK)
        elif export_job.tstarted is not None:
            content = {"status": "started", "tstarted": str(export_job.tstarted)}
            return fastapi.responses.JSONResponse(content=content, status_code=HTTP_202_ACCEPTED)
        else:
            return fastapi.responses.JSONResponse(content={"status": "created"}, status_code=HTTP_201_CREATED)


@export_app.get("/download")
def download(uuid: str = fastapi.Query(None), session: Session = Depends(get_session)):
    export_job = sqlmodel_database.export_job_with_uuid(session, uuid)
    if export_job is None:
        return _not_found_response()
    else:
        if export_job.file_path is not None and os.path.exists(export_job.file_path):
            status_code = 200 if os.path.getsize(export_job.file_path) > 0 else 204
            return FileResponse(export_job.file_path, status_code=status_code)
        else:
            return _not_found_response()
