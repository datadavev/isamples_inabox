import logging
import typing
from typing import Any

import fastapi
import requests
import starlette.middleware.base
import json
import isb_web.config
from isb_web.isb_enums import _NoValue
from isb_lib.core import MEDIA_JSON

PROPERTIES = "properties"

EVENT = "event"

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
    RELATION_METADATA = "relation_metadata"
    RELATED_SOLR = "related_solr"
    THINGS_DOWNLOAD = "things_download"


def attach_analytics_state_to_request(
    event: AnalyticsEvent,
    request: fastapi.Request,
    properties: typing.Optional[typing.Dict] = None,
):
    """
    Attaches analytics data to the request for later asynchronous processing by the AnalyticsMiddleware
    """
    metrics_dict: dict[str, Any] = {
        EVENT: event
    }
    if properties is not None:
        metrics_dict[PROPERTIES] = properties
    request.state.metrics = metrics_dict


class AnalyticsMiddleware(starlette.middleware.base.BaseHTTPMiddleware):
    """
    Middleware for sending analytics messages to plausible.

    This middleware wraps requests and checks for a request.state.metrics dict property.
    If present, then metrics["event"] is used as the event and metrics["properties"]
    if present is used as the optional properties to include with the event.
    """

    def __init__(
        self,
        app,
        analytics_url: str = ANALYTICS_URL,
        analytics_domain: str = ANALYTICS_DOMAIN,
    ):
        super().__init__(app)
        self.analytics_url = analytics_url
        self.analytics_domain = analytics_domain

    async def dispatch(self, request: fastapi.Request, call_next: typing.Callable):
        """
        Wraps a request to send an analytics message on completion.

        A methods reports metrics by appending a metric to request.state.metrics.

        Checks for response.state.analytics, if present then sends off the bundle.
        """
        request.state.metrics = None
        response = await call_next(request)
        if self.analytics_domain is not None and self.analytics_url is not None:
            if request.state.metrics is not None:
                _headers = self._analytics_request_headers(request)
                _data = self._analytics_request_data(
                    request.state.metrics.get(EVENT, None),
                    request,
                    request.state.metrics.get(PROPERTIES, None),
                )
                response.background = starlette.background.BackgroundTask(
                    self.send_metrics, _headers, _data
                )
        return response

    def send_metrics(self, headers: dict, data: dict):
        post_data_str = json.dumps(data).encode("utf-8")
        try:
            response = requests.post(
                self.analytics_url, headers=headers, data=post_data_str, timeout=1
            )
            response.close()
        except Exception as e:
            logging.error(
                "Exception recording analytics event %s, exception: %s",
                data.get("event"),
                e,
            )

    def _analytics_request_data(
        self,
        event: AnalyticsEvent,
        request: fastapi.Request,
        properties: typing.Optional[typing.Dict],
    ) -> typing.Dict:
        referer = request.headers.get("referer")
        data_dict = {
            "name": event.value,
            "domain": self.analytics_domain,
            "url": str(request.url),
        }
        if referer is not None:
            data_dict["referrer"] = referer
        if properties is not None:
            # plausible.io has a bug where it needs the props to be stringified when posted in the data
            # https://github.com/plausible/analytics/discussions/1570
            data_dict["props"] = json.dumps(properties)
        return data_dict

    def _analytics_request_headers(self, request: fastapi.Request) -> typing.Dict:
        headers = {
            "Content-Type": MEDIA_JSON,
            "User-Agent": request.headers.get("user-agent", "no-user-agent"),
            "X-Forwarded-For": request.headers.get("x-forwarded-for", "no-client-ip"),
        }
        return headers
