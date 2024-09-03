import logging
import re
from typing import Optional

import requests

from isamples_metadata.metadata_constants import METADATA_COMPLIES_WITH


class LocalContextsInfo:
    def __init__(self, project_json: dict):
        self.title = project_json.get("title")
        project_notice = project_json.get("notice")
        bc_labels = project_json.get("bc_labels")
        if project_notice is not None:
            self.notices = [LocalContextsNotice(current_notice.get("img_url"), current_notice.get("default_text"), current_notice.get("name")) for current_notice in project_notice]
        elif bc_labels is not None:
            self.notices = [LocalContextsNotice(current_notice.get("img_url"), current_notice.get("label_text"), current_notice.get("name")) for current_notice in bc_labels]
        else:
            self.notices = []
        self.project_page = project_json.get("project_page")


class LocalContextsNotice:
    def __init__(self, img_url: str, text: str, name: str):
        self.img_url = img_url
        self.text = text
        self.name = name


class LocalContextsClient:
    LOCAL_CONTEXTS_API_PREFIX = "https://localcontextshub.org/api/v1/projects/"
    PROJECT_ID_REGEX = re.compile(r"localcontexts:projects/(.*)")

    def localcontexts_project_id_for_complies_with_id(self, complies_with_id: str) -> Optional[str]:
        """Returns the localcontexts id if present in the specified complies_with identifier"""
        # looks like this: "localcontexts:projects/71b32571-0176-4627-8e01-4d78818432a7"
        match = LocalContextsClient.PROJECT_ID_REGEX.match(complies_with_id)
        if match is not None:
            return match.group(1)
        else:
            return None

    def project_info(self, project_id: str, rsession: requests.Session = requests.Session()) -> Optional[LocalContextsInfo]:
        # include the slash on the end to avoid the redirect
        project_detail_url = f"{LocalContextsClient.LOCAL_CONTEXTS_API_PREFIX}{project_id}/"
        response = rsession.get(project_detail_url)
        if response.status_code == 200:
            project_dict = response.json()
            return LocalContextsInfo(project_dict)
        else:
            logging.warning(f"Received unexpected response from localcontexts: {response}")
            return None


LOCAL_CONTEXTS_INFO_CACHE: dict[str, LocalContextsInfo] = {}


def local_contexts_info_for_resolved_content(resolved_content: dict, rsession: requests.Session = requests.Session()) -> Optional[LocalContextsInfo]:
    complies_with = resolved_content.get(METADATA_COMPLIES_WITH)
    client = LocalContextsClient()
    if complies_with is not None:
        for complies in complies_with:
            project_id = client.localcontexts_project_id_for_complies_with_id(complies)
            if project_id is not None:
                project_info = LOCAL_CONTEXTS_INFO_CACHE.get(project_id)
                if project_info is None:
                    project_info = client.project_info(project_id, rsession)
                    if project_info is not None:
                        LOCAL_CONTEXTS_INFO_CACHE[project_id] = project_info
                return project_info
        return None
    else:
        return None
