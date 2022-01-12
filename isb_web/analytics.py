import logging
import typing
import requests
import json
import isb_web.config
from isb_format import _NoValue
from isb_lib.core import MEDIA_JSON

ANALYTICS_URL = isb_web.config.Settings().analytics_url
ANALYTICS_DOMAIN = isb_web.config.Settings().analytics_domain


class AnalyticsEvent(_NoValue):
    """Enum class representing all possible analytics events we may record.  These need to be defined in plausible.io
    per https://plausible.io/docs/custom-event-goals#using-custom-props"""

    THING_LIST = "thing_list"
    THING_LIST_METADATA = "thing_list_metadata"
    THING_LIST_TYPES = "thing_list_types"
    THING_SOLR_SELECT = "thing_solr_select"
    THING_SOLR_LUKE_INFO = "thing_solr_luke_info"
    THING_SOLR_STREAM = "thing_solr_stream"
    THING_BY_IDENTIFIER = "thing_by_identifier"
    STAC_ITEM_BY_IDENTIFIER = "stac_item_by_identifier"
    STAC_COLLECTION = "stac_collection"
    RELATED_METADATA = "related_metadata"
    RELATED_SOLR = "related_solr"


def record_analytics_event(
    event: AnalyticsEvent,
    caller_user_agent: str,
    caller_ip: str,
    caller_url: str,
    properties: typing.Optional[typing.Dict] = None,
) -> bool:
    """
    Records an analytics event in plausible.io
    Args:
        event: The event to record
        caller_user_agent: User agent of the caller
        caller_ip: IP of the caller
        caller_url: URL the caller used to invoke the event
        properties: Custom event properties

    Returns: true if plausible responds with a 202, false otherwise

    """
    headers = {
        "Content-Type": MEDIA_JSON,
        "User-Agent": caller_user_agent,
        "X-Forwarded-For": caller_ip,
    }
    data_dict = {"name": event.value, "domain": ANALYTICS_DOMAIN, "url": caller_url}
    if properties is not None:
        # plausible.io has a bug where it needs the props to be stringified when posted in the data
        # https://github.com/plausible/analytics/discussions/1570
        data_dict["props"] = json.dumps(properties)
    post_data_str = json.dumps(data_dict).encode("utf-8")
    response = requests.post(ANALYTICS_URL, headers=headers, data=post_data_str)
    if response.status_code != 202:
        logging.error("Error recording analytics event %s, status code: %s", event.value, response.status_code)
    return response.status_code == 202
