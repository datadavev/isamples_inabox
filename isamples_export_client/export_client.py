import datetime
import json
import logging
import time
from enum import Enum
import os
from typing import Optional, Any

import requests
from requests import Session, Response

from isamples_metadata.solr_field_constants import SOLR_INDEX_UPDATED_TIME
from isb_lib.core import datetimeToSolrStr

START_TIME = "start_time"

EXPORT_SERVER_URL = "export_server_url"

FORMAT = "format"

QUERY = "query"


class ExportJobStatus(Enum):
    CREATED = "created"
    STARTED = "started"
    COMPLETED = "completed"
    ERROR = "error"

    @staticmethod
    def string_to_enum(raw_string: str) -> "ExportJobStatus":
        for enum_value in ExportJobStatus:
            if enum_value.value == raw_string:
                return enum_value
        raise ValueError(f"No ExportJobStatus found for {raw_string}")


def _is_expected_response_code(response: Response):
    return 200 <= response.status_code < 300


class ExportClient:
    def __init__(self, query: str,
                 destination_directory: str,
                 jwt: str,
                 export_server_url: str,
                 format: str,
                 refresh_date: Optional[str] = None,
                 session: Session = requests.session(),
                 sleep_time: float = 5):
        self._query = query
        self._destination_directory = destination_directory
        self._jwt = jwt
        if not export_server_url.endswith("/"):
            export_server_url = f"{export_server_url}/"
        self._export_server_url = export_server_url
        self._format = format
        self._refresh_date = refresh_date
        self._rsession = session
        self._sleep_time = sleep_time
        try:
            os.makedirs(name=self._destination_directory, exist_ok=True)
        except OSError as e:
            raise ValueError(f"Unable to create export directory at {self._destination_directory}, error: {e}")

    @classmethod
    def from_existing_download(cls, refresh_dir: str, jwt: str) -> "ExportClient":
        manifest_file_path = ExportClient._manifest_file_path(refresh_dir)
        if not os.path.exists(manifest_file_path):
            raise ValueError(f"Refresh option was specified, but manifest file at {manifest_file_path} does not exist")
        with open(manifest_file_path, "r") as existing_file:
            manifest_list = json.load(existing_file)
            last_manifest_dict = manifest_list[-1]
            query = last_manifest_dict[QUERY]
            export_server_url = last_manifest_dict[EXPORT_SERVER_URL]
            format = last_manifest_dict[FORMAT]
            refresh_date = last_manifest_dict[START_TIME]
            return ExportClient(query, refresh_dir, jwt, export_server_url, format, refresh_date)

    @classmethod
    def _manifest_file_path(cls, dir_path: str):
        return os.path.join(dir_path, "manifest.json")

    def _authentication_headers(self) -> dict:
        return {
            "authorization": f"Bearer {self._jwt}"
        }

    def _query_with_timestamp(self) -> str:
        if self._refresh_date is not None:
            return f"{self._query} AND {SOLR_INDEX_UPDATED_TIME}:[{self._refresh_date} TO *]"
        else:
            return self._query

    def create(self) -> str:
        """Create a new export job, and return the uuid associated with the job"""
        query = self._query
        if self._refresh_date is not None:
            query = self._query_with_timestamp()

        create_url = f"{self._export_server_url}create?q={query}&export_format={self._format}"
        response = self._rsession.get(create_url, headers=self._authentication_headers())
        if _is_expected_response_code(response):
            json = response.json()
            return json.get("uuid")
        raise ValueError(f"Invalid response to export creation: {response.json()}")

    def status(self, uuid: str) -> Any:
        """Check the status of the specified export job"""
        status_url = f"{self._export_server_url}status?uuid={uuid}"
        response = self._rsession.get(status_url, headers=self._authentication_headers())
        if _is_expected_response_code(response):
            return response.json()
        raise ValueError(f"Invalid response to export status: {response.json()}")

    def download(self, uuid: str) -> str:
        """Download the exported result set to the specified destination"""
        download_url = f"{self._export_server_url}download?uuid={uuid}"
        with requests.get(download_url, stream=True, headers=self._authentication_headers()) as r:
            r.raise_for_status()
            current_time = datetime.datetime.now()
            date_string = current_time.strftime("%Y_%m_%d_%H_%M_%S")
            filename = f"isamples_export_{date_string}.{self._format}"
            local_filename = os.path.join(self._destination_directory, filename)
            with open(local_filename, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            return local_filename

    def write_manifest(self, query: str, uuid: str, tstarted: datetime.datetime, num_results: int) -> str:
        new_manifest_dict = {
            QUERY: query,
            "uuid": uuid,
            FORMAT: self._format,
            START_TIME: datetimeToSolrStr(tstarted),
            "num_results": num_results,
            EXPORT_SERVER_URL: self._export_server_url
        }
        if self._refresh_date is not None:
            # if we are refreshing, include the additional timestamp filter for verbosity's sake
            new_manifest_dict["query_with_timestamp"] = self._query_with_timestamp()
        manifest_path = ExportClient._manifest_file_path(self._destination_directory)
        if os.path.exists(manifest_path):
            with open(manifest_path, "r") as file:
                manifests = json.load(file)
            manifests.append(new_manifest_dict)
        else:
            manifests = [new_manifest_dict]
        with open(manifest_path, "w") as f:
            f.write(json.dumps(manifests, indent=4))
        return manifest_path

    def perform_full_download(self):
        logging.warning("Contacting the export service to start the export process")
        tstarted = datetime.datetime.now()
        uuid = self.create()
        logging.warning(f"Contacted the export service, created export job with uuid {uuid}")
        while True:
            try:
                json = self.status(uuid)
                status = ExportJobStatus.string_to_enum(json.get("status"))
                if status == ExportJobStatus.ERROR:
                    logging.warning(f"Export job failed with error.  Check that your solr query is valid and try again.  Response: {json}")
                    break
                if status != ExportJobStatus.COMPLETED:
                    time.sleep(self._sleep_time)
                    logging.warning(f"Export job still running, sleeping for {self._sleep_time} seconds")
                    continue
                else:
                    logging.warning("Export job completed, going to download")
                    filename = self.download(uuid)
                    logging.warning(f"Successfully downloaded file to {filename}")
                    num_results = sum(1 for _ in open(filename))
                    manifest_path = self.write_manifest(self._query, uuid, tstarted, num_results)
                    logging.warning(f"Successfully wrote manifest file to {manifest_path}")
                    break
            except Exception as e:
                logging.error("An error occurred:", e)
                # Sleep for a short time before retrying after an error
                time.sleep(self._sleep_time)
