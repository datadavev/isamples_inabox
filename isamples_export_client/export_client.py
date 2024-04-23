import datetime
import json
import logging
import time
from enum import Enum
import os

import requests
from requests import Session, Response

from isb_lib.core import datetimeToSolrStr


class ExportJobStatus(Enum):
    CREATED = "created"
    STARTED = "started"
    COMPLETED = "completed"

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
                 session: Session = requests.session(),
                 sleep_time: float = 5):
        self._query = query
        self._destination_directory = destination_directory
        self._jwt = jwt
        if not export_server_url.endswith("/"):
            export_server_url = f"{export_server_url}/"
        self._export_server_url = export_server_url
        self._format = format
        self._rsession = session
        self._sleep_time = sleep_time
        try:
            os.makedirs(name=self._destination_directory, exist_ok=True)
        except OSError as e:
            raise ValueError(f"Unable to create export directory at {self._destination_directory}, error: {e}")

    def _authentication_headers(self) -> dict:
        return {
            "authorization": f"Bearer {self._jwt}"
        }

    def create(self) -> str:
        """Create a new export job, and return the uuid associated with the job"""
        create_url = f"{self._export_server_url}create?q={self._query}&export_format={self._format}"
        response = self._rsession.get(create_url, headers=self._authentication_headers())
        if _is_expected_response_code(response):
            json = response.json()
            return json.get("uuid")
        raise ValueError(f"Invalid response to export creation: {response.json()}")

    def status(self, uuid: str) -> ExportJobStatus:
        """Check the status of the specified export job"""
        status_url = f"{self._export_server_url}status?uuid={uuid}"
        response = self._rsession.get(status_url, headers=self._authentication_headers())
        if _is_expected_response_code(response):
            json = response.json()
            status = json.get("status")
            return ExportJobStatus.string_to_enum(status)
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

    def write_manifest(self, query: str, uuid: str, tstarted: datetime.datetime, num_results: int):
        manifest_dict = {
            "query": query,
            "uuid": uuid,
            "tstarted": datetimeToSolrStr(tstarted),
            "num_results": num_results
        }
        with open(os.path.join(self._destination_directory, "manifest.json"), "w") as f:
            f.write(json.dumps(manifest_dict, indent=4))

    def perform_full_download(self):
        logging.warning("Contacting the export service to start the export process")
        tstarted = datetime.datetime.now()
        uuid = self.create()
        logging.warning(f"Contacted the export service, created export job with uuid {uuid}")
        while True:
            try:
                status = self.status(uuid)
                if status != ExportJobStatus.COMPLETED:
                    time.sleep(self._sleep_time)
                    logging.warning(f"Export job still running, sleeping for {self._sleep_time} seconds")
                    continue
                else:
                    logging.warning("Export job completed, going to download")
                    filename = self.download(uuid)
                    logging.warning(f"Successfully downloaded file to {filename}")
                    num_results = sum(1 for _ in open(filename))
                    self.write_manifest(self._query, uuid, tstarted, num_results)
                    break
            except Exception as e:
                logging.error("An error occurred:", e)
                # Sleep for a short time before retrying after an error
                time.sleep(self._sleep_time)
