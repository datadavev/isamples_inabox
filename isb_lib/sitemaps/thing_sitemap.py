import datetime
import os.path
import typing
from pathlib import Path
from typing import Optional

from sqlmodel import Session

from isb_lib.core import datetimeToSolrStr
from isb_lib.sitemaps import SitemapIndexEntry, UrlSetEntry, ThingUrlSetEntry, ThingSitemapIndexEntry
from isb_web.sqlmodel_database import things_for_sitemap

MAX_URLS_IN_SITEMAP = 50000


class ThingUrlSetIterator:
    """Iterator class responsible for listing individual urls in an urlset"""

    def __init__(
        self,
        sitemap_index: int,
        max_length: int,
        things: typing.List[tuple[str, datetime.datetime]],
    ):
        self._thing_files: list = things
        self._thing_file_index = 0
        self._max_length = max_length
        self.num_urls = 0
        self.sitemap_index = sitemap_index
        self.last_tstamp_str: Optional[str] = None
        self.last_identifier: Optional[str] = None

    def __iter__(self):
        return self

    def __next__(self) -> UrlSetEntry:
        # Dont read past the bounds
        if self._thing_file_index == len(self._thing_files) or self._thing_file_index == self._max_length:
            raise StopIteration
        next_thing = self._thing_files[self._thing_file_index]
        timestamp_str = datetimeToSolrStr(next_thing[1])
        next_url_set_entry = ThingUrlSetEntry(next_thing[0], timestamp_str)
        # Update the necessary state
        self.num_urls += 1
        self._thing_file_index += 1
        self.last_tstamp_str = next_url_set_entry.last_mod_str
        self.last_identifier = next_url_set_entry.identifier
        return next_url_set_entry

    def sitemap_index_entry(self) -> SitemapIndexEntry:
        return ThingSitemapIndexEntry(
            f"sitemap-{self.sitemap_index}.xml", self.last_tstamp_str or ""
        )


class ThingSitemapIndexIterator:
    """Iterator class responsible for listing the individual sitemap files in a sitemap index"""

    def _initialize_directory_state(self):
        self._file_info = []
        for file in self._directory_path.iterdir():
            if file.is_file():
                modification_time = datetime.datetime.fromtimestamp(file.stat().st_mtime)
                self._file_info.append((file.name, modification_time))

    def __init__(
        self,
        directory_path: str,
        authority: Optional[str] = None,
        num_things_per_file: int = MAX_URLS_IN_SITEMAP,
        status: int = 200,
        offset: int = 0,
    ):
        self._last_timestamp_str: Optional[str] = None
        self._last_primary_key: Optional[str] = "0"
        self._directory_path = Path(directory_path)
        assert os.path.exists(self._directory_path)
        self._initialize_directory_state()
        self._authority = authority
        self._num_things_per_file = num_things_per_file
        self._status = status
        self._offset = offset
        self._last_url_set_iterator: Optional[ThingUrlSetIterator] = None
        self.num_url_sets = 0

    def __iter__(self):
        return self

    def __next__(self) -> ThingUrlSetIterator:
        if self._last_url_set_iterator is not None:
            # Update our last values with the last ones from the previous iterator
            self._last_timestamp_str = self._last_url_set_iterator.last_tstamp_str
            self._last_primary_key = self._last_url_set_iterator.last_identifier

        if self.num_url_sets * self._num_things_per_file > len(self._file_info):
            raise StopIteration
        next_url_set_iterator = ThingUrlSetIterator(
            self.num_url_sets, self._num_things_per_file, self._file_info
        )
        self._last_url_set_iterator = next_url_set_iterator
        self.num_url_sets = self.num_url_sets + 1
        self._offset = self._offset + self._num_things_per_file
        return next_url_set_iterator
