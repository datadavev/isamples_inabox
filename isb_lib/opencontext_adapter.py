import datetime
from typing import Optional
import pytz

import isb_lib.core
import logging
import requests
import isb_lib.models.thing
import typing
import dateparser
from isamples_metadata import OpenContextTransformer
from isamples_metadata.core_json_transformer import CoreJSONTransformer
from isb_lib.core import MEDIA_JSONL
from isb_lib.utilities.requests_utilities import RetryingRequests

HTTP_TIMEOUT = 60  # seconds
OPENCONTEXT_PAGE_SIZE = 1000       # number of result records per request "page"
OPENCONTEXT_API = f"https://opencontext.org/query/.json?attributes=iSamples&cat=oc-gen-cat-sample-col%7C%7Coc-gen-cat-bio-subj-ecofact%7C%7Coc-gen-cat-object&cursorMark=%2a&response=metadata,uri-meta&sort=updated--desc,context--asc&type=subjects&rows={OPENCONTEXT_PAGE_SIZE}"
MEDIA_JSON = "application/json"


def get_logger():
    return logging.getLogger("isb_lib.opencontext_adapter")


class OpenContextItem(object):
    AUTHORITY_ID = "OPENCONTEXT"

    def __init__(self, identifier: str, source):
        self.identifier = identifier
        self.item = source

    def as_thing(
        self,
        t_created: Optional[datetime.datetime],
        status: int,
        resolved_url: str,
        t_resolved: datetime.datetime,
        resolve_elapsed: Optional[float],
        media_type: Optional[str] = None,
    ) -> isb_lib.models.thing.Thing:
        L = get_logger()
        L.debug("OpenContextItem.asThing")
        if media_type is None:
            media_type = MEDIA_JSON
        _thing = isb_lib.models.thing.Thing(
            id=self.identifier,
            tcreated=t_created,
            item_type=None,
            authority_id=OpenContextItem.AUTHORITY_ID,
            resolved_url=resolved_url,
            resolved_status=status,
            tresolved=t_resolved,
            resolve_elapsed=resolve_elapsed,
        )
        if not isinstance(self.item, dict):
            L.error("Item is not an object")
            return _thing
        _thing.item_type = "sample"
        _thing.resolved_media_type = media_type
        # Note that we can't use this field in opencontext as the information is batched and we don't have access
        # to the raw http response here.
        # _thing.resolve_elapsed = resolve_elapsed
        _thing.resolved_content = self.item
        _thing.h3 = OpenContextTransformer.geo_to_h3(_thing.resolved_content)
        return _thing


class OpenContextRecordIterator(isb_lib.core.IdentifierIterator):
    def __init__(
        self,
        offset: int = 0,
        max_entries: int = -1,
        date_start: Optional[datetime.datetime] = None,
        date_end: Optional[datetime.datetime] = None,
        page_size: int = OPENCONTEXT_PAGE_SIZE,
    ):
        super().__init__(
            offset=offset,
            max_entries=max_entries,
            date_start=date_start,
            date_end=date_end,
            page_size=page_size,
        )
        self._past_date_start = False
        self.url = OPENCONTEXT_API
        self._retrying_requests = RetryingRequests(include_random_on_failure=True, timeout=HTTP_TIMEOUT, sleep_sec=60, success_func=is_valid_opencontext_response)
        # force the updated date to be considered UTC as that is what the OC dates are
        if self._date_start is not None:
            self._date_start = self._date_start.replace(tzinfo=pytz.utc)
        if self._date_end is not None:
            self._date_end = self._date_end.replace(tzinfo=pytz.utc)

    def records_in_page(self):
        L = get_logger()
        L.debug("records_in_page")
        headers = {"Accept": "application/json", "user-agent": "isamplesbot-3000/0.0.1"}
        _page_size = self._page_size
        params = {
            "rows": _page_size,
        }
        more_work = True
        num_records = 0
        rsession = requests.Session()
        while more_work and self.url is not None and not self._past_date_start:
            L.info("trying to hit %s", self.url)
            response = self._retrying_requests.get(self.url, rsession=rsession, params=params, headers=headers)
            data = response.json()
            next_url = data.get("next-json")
            results = data.get("oc-api:has-results", {})
            for record in results:
                L.info("records_in_page Record id: %s", record.get("uri", None))
                # force the updated date to be considered UTC as that is what the OC dates are
                record_updated = dateparser.parse(record["updated"]).replace(tzinfo=pytz.utc)
                if self._date_start is not None and record_updated is not None and record_updated < self._date_start:
                    L.info(
                        "Iterated record with updated date %s earlier than previous max %s. Update is complete.",
                        record_updated,
                        self._date_start
                    )
                    data = {}
                    self._past_date_start = True
                    break
                # print(json.dumps(record, indent=2))
                # raise NotImplementedError
                yield record
                num_records += 1

            # TODO: this is a hack and should ideally be replaced on the OC side.
            if next_url is not None:
                self.url = next_url.replace("ALL-STANDARD-LD", "iSamples")
            else:
                self.url = None
            if (
                len(data.get("oc-api:has-results", {})) < _page_size
                or self.url is None
                or 0 < self._max_entries <= num_records
                or num_records == self._page_size
                or self._past_date_start
            ):
                more_work = False

    def loadEntries(self):
        self._cpage = []
        self._page_offset = 0
        counter = 0
        for item in self.records_in_page():
            self._cpage.append(item)
            counter += 1
            if 0 < self._max_entries < counter:
                break
        self._total_records = len(self._cpage)

    def _getPage(self):
        """Override this method to retrieve a page of entries from the service.

        After completion, self._cpage contains the next page of entries or None if there
        are no more pages available, and self._page_offset is set to the first entry (usually 0)

        """
        is_none = self._cpage is None
        if is_none or self._page_offset >= len(self._cpage):
            self.loadEntries()
        if self._coffset >= self._total_records:
            return
        self._page_offset = 0

    def last_url_str(self) -> str:
        return self.url


def _validate_resolved_content(thing: isb_lib.models.thing.Thing) -> dict:
    return isb_lib.core.validate_resolved_content(OpenContextItem.AUTHORITY_ID, thing)


def reparse_as_core_record(thing: isb_lib.models.thing.Thing) -> typing.List[typing.Dict]:
    resolved_content = _validate_resolved_content(thing)
    try:
        if thing.resolved_media_type == MEDIA_JSONL:
            transformer = CoreJSONTransformer(thing.resolved_content)
        else:
            transformer = OpenContextTransformer.OpenContextTransformer(resolved_content)
        return [isb_lib.core.coreRecordAsSolrDoc(transformer)]
    except Exception as e:
        get_logger().error("Failed trying to run transformer on %s, exception: %s", thing.resolved_content, e)
        raise


def identifier_from_thing_dict(thing_dict: typing.Dict) -> str:
    opencontext_id = thing_dict["citation uri"]
    # check the type here because some records don't have it and have a boolean False instead
    if opencontext_id is not None and type(opencontext_id) is str:
        opencontext_id = isb_lib.normalized_id(opencontext_id)
    else:
        opencontext_id = thing_dict["uri"]
        get_logger().error(
            "Received OpenContext record without citation uri -- this is going to be problematic but will save.")
    return opencontext_id


def load_thing(
    thing_dict: typing.Dict, t_resolved: datetime.datetime, url: str
) -> isb_lib.models.thing.Thing:
    """
    Load a thing from its source.

    Minimal parsing of the thing is performed to populate the database record.

    Args:
        thing_dict: Dictionary representing the thing
        t_resolved: When the item was resolved from the source

    Returns:
        Instance of Thing
    """
    L = get_logger()
    id = identifier_from_thing_dict(thing_dict)
    t_created = t_created_from_thing_dict(thing_dict)
    L.info("loadThing: %s", id)
    item = OpenContextItem(id, thing_dict)
    # TODO, unlike the other collections, we are fetching these via a pre-paginated API, so we can't put anything in the
    # db if we fail looking up an identifier because we won't have an identifier if the lookup fails.  Maybe there is
    # nothing to do here, but it's a difference between collections.
    thing = item.as_thing(t_created, 200, url, t_resolved, None)
    return thing


def t_created_from_thing_dict(thing_dict):
    updated_time = thing_dict.get("updated")
    if updated_time is not None:
        t_created = dateparser.parse(updated_time)
    return t_created


def update_thing(thing: isb_lib.models.thing.Thing, updated_record: typing.Dict, t_resolved: datetime.datetime, url: str):
    """
    Updates an existing Thing row in the database

    Args:
        thing: The thing row from the database
        t_resolved: When the item was resolved from the source
        url: The url the row was retrieved from
    """
    thing.resolved_content = updated_record
    thing.tresolved = t_resolved
    updated_str = updated_record.get("updated")
    if updated_str is not None:
        thing.tcreated = dateparser.parse(updated_str)
    thing.resolved_url = url


def is_valid_opencontext_response(response: requests.Response) -> bool:
    data = response.json()
    if data is None:
        return False
    results = data.get("oc-api:has-results", None)
    return results is not None
