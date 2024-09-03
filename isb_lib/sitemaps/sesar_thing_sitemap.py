
import datetime
import typing
from typing import Optional

from sqlmodel import Session

from isb_lib.core import datetimeToSolrStr
from isb_lib.sitemaps import SitemapIndexEntry, UrlSetEntry, ThingSitemapIndexEntry, SESARThingUrlSetEntry
from isb_web.sqlmodel_database import things_for_sitemap

MAX_URLS_IN_SITEMAP = 50000


class SESARThingUrlSetIterator:
    """Iterator class responsible for listing individual urls in an urlset"""

    def __init__(
        self,
        sitemap_index: int,
        max_length: int,
        things: typing.List[tuple[str, datetime.datetime]],
    ):
        self._things: list = things
        self._thing_index = 0
        self._max_length = max_length
        self.num_urls = 0
        self.sitemap_index = sitemap_index
        self.last_tstamp_str: Optional[str] = None
        self.last_identifier: Optional[str] = None

    def __iter__(self):
        return self

    def __next__(self) -> UrlSetEntry:
        # Dont read past the bounds
        if self._thing_index == len(self._things):
            raise StopIteration
        next_thing = self._things[self._thing_index]
        timestamp_str = datetimeToSolrStr(next_thing[1])
        next_url_set_entry = SESARThingUrlSetEntry(next_thing[0], timestamp_str, next_thing[2])
        # Update the necessary state
        self.num_urls += 1
        self._thing_index += 1
        self.last_tstamp_str = next_url_set_entry.last_mod_str
        self.last_identifier = next_url_set_entry.identifier
        return next_url_set_entry

    def sitemap_index_entry(self) -> SitemapIndexEntry:
        return ThingSitemapIndexEntry(
            f"sitemap-{self.sitemap_index}.jsonl", self.last_tstamp_str or ""
        )


class SESARThingSitemapIndexIterator:
    """Iterator class responsible for listing the individual sitemap files in a sitemap index"""

    def __init__(
        self,
        session: Session,
        authority: Optional[str] = None,
        num_things_per_file: int = MAX_URLS_IN_SITEMAP,
        status: int = 200,
        offset: int = 0,
    ):
        self._last_timestamp_str: Optional[str] = None
        self._last_primary_key: Optional[str] = "0"
        self._authority = authority
        self._num_things_per_file = num_things_per_file
        self._status = status
        self._offset = offset
        self._last_url_set_iterator: Optional[SESARThingUrlSetIterator] = None
        self._session = session
        self.num_url_sets = 0

    def __iter__(self):
        return self

    def __next__(self) -> SESARThingUrlSetIterator:
        if self._last_url_set_iterator is not None:
            # Update our last values with the last ones from the previous iterator
            self._last_timestamp_str = self._last_url_set_iterator.last_tstamp_str
            self._last_primary_key = self._last_url_set_iterator.last_identifier
        things = things_for_sitemap(
            self._session, self._authority, 200, self._num_things_per_file, self._offset
        )
        if len(things) == 0:
            raise StopIteration
        next_url_set_iterator = SESARThingUrlSetIterator(
            self.num_url_sets, self._num_things_per_file, things
        )
        self._last_url_set_iterator = next_url_set_iterator
        self.num_url_sets = self.num_url_sets + 1
        self._offset = self._offset + self._num_things_per_file
        return next_url_set_iterator
