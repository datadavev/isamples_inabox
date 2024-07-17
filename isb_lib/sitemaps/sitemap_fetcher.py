from __future__ import annotations
import re
from abc import ABC
import datetime
from typing import Iterator, Optional

import lxml.etree
import requests
import typing
import logging

import isb_lib.core
import json


IDENTIFIER_REGEX = re.compile(r".*/thing/(.*)")

NUM_RETRIES = 5


class ThingsJSONLinesFetcher:
    def __init__(
        self,
        json_lines_url: str,
        sitemap_url: str,
        session: requests.Session = requests.session(),
    ):
        self.json_lines_url = json_lines_url
        self.sitemap_url = sitemap_url
        self._session = session
        self.json_dicts: list[dict] = []
        self.primary_keys_fetched: Optional[list[str]] = None

    def fetch_things(self) -> ThingsJSONLinesFetcher:
        try:
            for i in range(NUM_RETRIES):
                # headers = {"Content-Type": "application/json"}
                logging.info(f"Going to fetch json lines things from {self.sitemap_url} at {self.json_lines_url}")
                response = self._session.get(self.json_lines_url, timeout=90)
                if response.status_code != 200:
                    logging.error(f"Got response code {response.status_code} from {self.json_lines_url}, will retry")
                    continue
                else:
                    json_things = []
                    response_text = response.text
                    for line in response_text.splitlines():
                        json_things.append(json.loads(line))
                    self.json_dicts = json_things
                    logging.info(f"Completed fetching json lines things from {self.sitemap_url} at {self.json_lines_url}")
                    self.primary_keys_fetched = [
                        json_thing["sample_identifier"] for json_thing in self.json_dicts
                    ]
                    break
            if len(self.json_dicts) == 0:
                raise RuntimeError(f"Didn't receive a valid response from {self.json_lines_url} after {NUM_RETRIES} attempts.")
        except Exception as e:
            logging.critical(
                f"Error fetching things from: url: {self.json_lines_url} exception is {e}"
            )
        return self


class SitemapFetcher(ABC):
    def __init__(
        self,
        url: str,
        authority: str,
        last_modified: typing.Optional[datetime.datetime],
        session: requests.Session = requests.session(),
    ):
        self._url = url
        self._authority = authority
        self._last_modified = last_modified
        self._session = session
        self.urls_to_fetch: list[str] = []

    def _fetch_file(self):
        logging.info(f"Going to fetch sitemap at {self._url}")
        res = self._session.get(self._url)
        root = lxml.etree.fromstring(res.content)
        sitemap_list = root.getchildren()
        """These sitemap children look like this:
              <sitemap>
                <loc>http://mars.cyverse.org/sitemaps/sitemap-5.xml</loc>
                <lastmod>2006-08-10T12:00:00Z</lastmod>
              </sitemap>
              or this:
                <urlset>
                  <url>
                    <loc>thing/ark:/28722/k2bg30w29?full=false&amp;format=core</loc>
                    <lastmod>2021-07-02T22:49:54Z</lastmod>
                  </url>
                </urlset>
            Either way, we can parse them the same way
        """
        for sitemap_child in sitemap_list:
            loc = (
                sitemap_child.iterchildren(
                    "{http://www.sitemaps.org/schemas/sitemap/0.9}loc"
                )
                .__next__()
                .text
            )
            lastmod = (
                sitemap_child.iterchildren(
                    "{http://www.sitemaps.org/schemas/sitemap/0.9}lastmod"
                )
                .__next__()
                .text
            )
            lastmod_date = isb_lib.core.parsed_datetime_from_isamples_format(lastmod)
            if (
                self._last_modified is None
                or lastmod_date.timestamp() >= self._last_modified.timestamp()
            ):
                self.urls_to_fetch.append(loc)

    def url_iterator(self) -> Iterator:
        return iter(self.urls_to_fetch)

    @property
    def url(self):
        return self._url


class SitemapFileFetcher(SitemapFetcher):
    def fetch_sitemap_file(self) -> SitemapFetcher:
        """Fetches the contents of the particular sitemap file and stores the URLs to fetch"""
        self._fetch_file()
        return self


class SitemapIndexFetcher(SitemapFetcher):
    def __init__(
        self,
        url: str,
        authority: str,
        last_modified: typing.Optional[datetime.datetime],
        session: requests.Session = requests.session(),
    ):
        super().__init__(url, authority, last_modified, session)

    def fetch_index_file(self):
        xmlp = lxml.etree.XMLParser(
            recover=True,
            remove_comments=True,
            resolve_entities=False,
        )
        lxml.etree.set_default_parser(xmlp)
        self._fetch_file()

    def fetch_child_files(self) -> typing.List[SitemapFileFetcher]:
        """Fetches the individual sitemap URLs from the sitemap index, and returns them in a list"""
        file_fetchers = []
        for url in self.urls_to_fetch:
            child_file_fetcher = self.sitemap_file_fetcher(url)
            child_file_fetcher.fetch_sitemap_file()
            file_fetchers.append(child_file_fetcher)
        return file_fetchers

    def prepare_sitemap_file_url(self, file_url: str) -> str:
        """Mainly used as a placeholder for overriding in unit testing"""
        return file_url

    def sitemap_file_fetcher(self, url: str) -> SitemapFileFetcher:
        return SitemapFileFetcher(
            self.prepare_sitemap_file_url(url),
            self._authority,
            self._last_modified,
            self._session,
        )
