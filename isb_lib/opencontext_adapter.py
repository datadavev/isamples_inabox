import datetime
import time

import isb_lib.core
import logging
import requests
import igsn_lib.models.thing
import typing
import dateparser
import isamples_metadata.OpenContextTransformer

HTTP_TIMEOUT = 10.0  # seconds
OPENCONTEXT_API = "https://opencontext.org/subjects-search/.json?add-attribute-uris=1&attributes=obo-foodon-00001303%2Coc-zoo-has-anat-id%2Ccidoc-crm-p2-has-type%2Ccidoc-crm-p45-consists-of%2Ccidoc-crm-p49i-is-former-or-current-keeper-of%2Ccidoc-crm-p55-has-current-location%2Cdc-terms-temporal%2Cdc-terms-creator%2Cdc-terms-contributor&prop=oc-gen-cat-sample-col%7C%7Coc-gen-cat-bio-subj-ecofact%7C%7Coc-gen-cat-object&response=metadata%2Curi-meta&sort=updated--desc"
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
        t_created: datetime.datetime,
        status: int,
        resolved_url: str,
        t_resolved: datetime.datetime,
        resolve_elapsed: float,
        media_type: str = None,
    ) -> igsn_lib.models.thing.Thing:
        L = get_logger()
        L.debug("OpenContextItem.asThing")
        if media_type is None:
            media_type = MEDIA_JSON
        _thing = igsn_lib.models.thing.Thing(
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
        _thing.related = None
        _thing.resolved_media_type = media_type
        # Note that we can't use this field in opencontext as the information is batched and we don't have access
        # to the raw http response here.
        # _thing.resolve_elapsed = resolve_elapsed
        _thing.resolved_content = self.item
        return _thing


class OpenContextRecordIterator(isb_lib.core.IdentifierIterator):
    def __init__(
        self,
        offset: int = 0,
        max_entries: int = -1,
        date_start: datetime.datetime = None,
        date_end: datetime.datetime = None,
        page_size: int = 100,
    ):
        super().__init__(
            offset=offset,
            max_entries=max_entries,
            date_start=date_start,
            date_end=date_end,
            page_size=page_size,
        )
        self.url = OPENCONTEXT_API

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
        while more_work and self.url is not None:
            L.info("trying to hit %s", self.url)
            response = requests.get(
                self.url, params=params, headers=headers, timeout=HTTP_TIMEOUT
            )

            if response.status_code != 200:
                L.error(
                    "Unable to load records; status: %s; reason: %s",
                    response.status_code,
                    response.reason,
                )
                break
            # L.debug("recordsInProject data: %s", response.text[:256])
            data = response.json()
            for record in data.get("oc-api:has-results", {}):
                L.info("records_in_page Record id: %s", record.get("uri", None))
                # print(json.dumps(record, indent=2))
                # raise NotImplementedError
                yield record
                num_records += 1
            self.url = data.get("next-json")
            if len(data.get("oc-api:has-results", {})) < _page_size:
                more_work = False
            elif self.url is None:
                # If we hit the last page, there won't be a 'next-json' key
                more_work = False
            elif 0 < self._max_entries <= num_records:
                more_work = False
            elif num_records == self._page_size:
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

    def last_url_str(self) -> typing.AnyStr:
        return self.url

def _validate_resolved_content(thing: igsn_lib.models.thing.Thing):
    if not isinstance(thing.resolved_content, dict):
        raise ValueError("Thing.resolved_content is not an object")
    if not thing.authority_id == OpenContextItem.AUTHORITY_ID:
        raise ValueError("Thing is not an OpenContext item")

def reparse_as_core_record(thing: igsn_lib.models.thing.Thing) -> typing.Dict:
    _validate_resolved_content(thing)
    transformer = isamples_metadata.OpenContextTransformer.OpenContextTransformer(thing.resolved_content)
    return isb_lib.core.coreRecordAsSolrDoc(transformer.transform())

def load_thing(
    thing_dict: typing.Dict, t_resolved: datetime.datetime, url: typing.AnyStr
) -> igsn_lib.models.thing.Thing:
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
    id = thing_dict["uri"]
    t_created = dateparser.parse(thing_dict.get("published"))
    L.info("loadThing: %s", id)
    item = OpenContextItem(id, thing_dict)
    # TODO, unlike the other collections, we are fetching these via a pre-paginated API, so we can't put anything in the
    # db if we fail looking up an identifier because we won't have an identifier if the lookup fails.  Maybe there is
    # nothing to do here, but it's a difference between collections.
    thing = item.as_thing(t_created, 200, url, t_resolved, None)
    return thing
