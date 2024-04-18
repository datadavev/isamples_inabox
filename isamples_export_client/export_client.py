from enum import Enum
import os

import requests
from requests import Session


class ExportJobStatus(Enum):
    CREATED = "created"
    STARTED = "started"
    COMPLETED = "completed"

    @staticmethod
    def string_to_enum(cls, raw_string: str) -> Enum:
        for enum_value in ExportJobStatus:
            if enum_value.value == raw_string:
                return enum_value
        raise ValueError(f"No ExportJobStatus found for {raw_string}")

class ExportClient:
    def __init__(self, query: str, destination_directory: str, jwt: str, export_server_url: str, format: str, session: Session = requests.session()):
        self._query = query
        self._destination_directory = destination_directory
        self._jwt = jwt
        if not export_server_url.endswith("/"):
            export_server_url = f"{export_server_url}/"
        self._export_server_url = export_server_url
        self._format = format
        self._rsession = session
        try:
            os.makedirs(name=self._destination_directory, exist_ok=True)
        except OSError as e:
            raise ValueError(f"Unable to create export directory at {self._destination_directory}")

    def create(self) -> str:
        """Create a new export job, and return the uuid associated with the job"""
        create_url = f"{self._export_server_url}create?q={self._query}&format={self._format}"
        response = self._rsession.get(create_url)
        if response.status_code == 200:
            json = response.json()
            return json.get("uuid")
        raise ValueError(f"Invalid response to export creation: {response}")

    def status(self, uuid: str) -> ExportJobStatus:
        """Check the status of the specified export job"""
        status_url = f"{self._export_server_url}status?uuid={uuid}"
        response = self._rsession.get(status_url)
        if response.status_code == 200:
            json = response.json()
            status = json.get("status")
            return ExportJobStatus.string_to_enum(status)
        raise ValueError(f"Invalid response to export status: {response}")

    def download(self, uuid: str) -> bool:
        """Download the exported result set to the specified destination"""