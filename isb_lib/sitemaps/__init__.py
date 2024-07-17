"""
Module for parsing sitemap.xml documents.

Chunks of this code based on https://github.com/scrapy/scrapy/blob/master/scrapy/utils/sitemap.py
"""
import asyncio
import datetime
import types
import logging
import re
import struct
import io
import gzip
import typing
import urllib.parse
import lxml.etree
import requests
import dateparser
import functools
import os.path
from aiofile import AIOFile, Writer

INDEX_XML = "sitemap-index.xml"

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

L = logging.getLogger("sitemaps")


class SitemapIndexEntry:
    """Individual sitemap file entry in the sitemap index"""

    def __init__(self, sitemap_filename: str, last_mod: str):
        self.sitemap_filename = sitemap_filename
        self.last_mod_str = last_mod

    def loc_suffix(self):
        return self.sitemap_filename


class ThingSitemapIndexEntry(SitemapIndexEntry):
    def loc_suffix(self):
        return f"sitemaps/{self.sitemap_filename}"


class UrlSetEntry:
    """Individual url entry in an urlset"""

    def __init__(self, identifier: str, last_mod: str):
        self.identifier = identifier
        self.last_mod_str = last_mod

    def loc_suffix(self):
        return self.identifier


class ThingUrlSetEntry(UrlSetEntry):
    def loc_suffix(self):
        return f"sitemaps/{self.identifier}"


table = str.maketrans(
    {
        "<": "&lt;",
        ">": "&gt;",
        "&": "&amp;",
        "'": "&apos;",
        '"': "&quot;",
    }
)


def xmlesc(txt):
    return txt.translate(table)


# adapted from https://github.com/Haikson/sitemap-generator/blob/master/pysitemap/format_processors/xml.py
async def write_urlset_file(dest_path: str, host: str, urls: typing.List[UrlSetEntry]):
    async with AIOFile(dest_path, "w") as aiodf:
        writer = Writer(aiodf)
        header = """<?xml version="1.0" encoding="utf-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" \
xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" \
xsi:schemaLocation="http://www.sitemaps.org/schemas/sitemap/0.9 \
http://www.sitemaps.org/schemas/sitemap/0.9/sitemap.xsd">\n"""
        await writer(header)
        await aiodf.fsync()
        for entry in urls:
            loc_str = xmlesc(
                os.path.join(host, entry.loc_suffix())
            )
            lastmod_str = ""
            if entry.last_mod_str is not None:
                lastmod_str = f"\n    <lastmod>{xmlesc(entry.last_mod_str)}</lastmod>"
            url_str = f"  <url>\n    <loc>{loc_str}</loc>{lastmod_str}\n  </url>\n"
            await writer(url_str)
        await aiodf.fsync()

        await writer("</urlset>")
        await aiodf.fsync()


async def write_sitemap_index_file(
    base_path: str, host: str, sitemap_index_entries: typing.List[SitemapIndexEntry]
):
    index_file_path = os.path.join(base_path, INDEX_XML)
    async with AIOFile(index_file_path, "w") as aiodf:
        writer = Writer(aiodf)
        header = """<?xml version="1.0" encoding="utf-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" \
xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" \
xsi:schemaLocation="http://www.sitemaps.org/schemas/sitemap/0.9 \
https://www.sitemaps.org/schemas/sitemap/0.9/siteindex.xsd">\n"""
        await writer(header)
        await aiodf.fsync()
        for sitemap_index_entry in sitemap_index_entries:
            loc_str = xmlesc(
                os.path.join(host, sitemap_index_entry.loc_suffix())
            )
            lastmod_str = xmlesc(sitemap_index_entry.last_mod_str)
            await writer(
                f"  <sitemap>\n    <loc>{loc_str}</loc>\n    <lastmod>{lastmod_str}</lastmod>\n  </sitemap>\n"
            )
        await aiodf.fsync()

        await writer("</sitemapindex>")
        await aiodf.fsync()


def build_sitemap(base_path: str, host: str, iterator: typing.Iterator):
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(_build_sitemap(base_path, host, iterator))
    loop.run_until_complete(future)


async def _build_sitemap(base_path: str, host: str, iterator: typing.Iterator):
    sitemap_index_entries = []
    for urlset_iterator in iterator:
        entries_for_urlset = []
        for urlset_entry in urlset_iterator:
            entries_for_urlset.append(urlset_entry)
        sitemap_index_entry = urlset_iterator.sitemap_index_entry()
        sitemap_index_entries.append(sitemap_index_entry)
        urlset_dest_path = os.path.join(base_path, sitemap_index_entry.sitemap_filename)
        await write_urlset_file(urlset_dest_path, host, entries_for_urlset)
        logging.info(
            "Done with urlset_iterator, wrote "
            + str(urlset_iterator.num_urls)
            + " records to "
            + sitemap_index_entry.sitemap_filename
        )
    await write_sitemap_index_file(base_path, host, sitemap_index_entries)


@functools.cache
def _toDatetimeTZ(V):
    return dateparser.parse(
        V, settings={"TIMEZONE": "+0000", "RETURN_AS_TIMEZONE_AWARE": True}
    )


# @deprecated('GzipFile.read1')
def read1(gzf, size=-1):
    return gzf.read1(size)


def gunzip(data):
    """Gunzip the given data and return as much data as possible.
    This is resilient to CRC checksum errors.
    """
    f = gzip.GzipFile(fileobj=io.BytesIO(data))
    output_list = []
    chunk = b"."
    while chunk:
        try:
            chunk = f.read1(8196)
            output_list.append(chunk)
        except (IOError, EOFError, struct.error):
            # complete only if there is some data, otherwise re-raise
            # see issue 87 about catching struct.error
            # some pages are quite small so output_list is empty and f.extrabuf
            # contains the whole page content
            if output_list or getattr(f, "extrabuf", None):
                try:
                    output_list.append(f.extrabuf[-f.extrasize:])
                finally:
                    break
            else:
                raise
    return b"".join(output_list)


def gzipMagicNumber(response):
    return response.content[:3] == b"\x1f\x8b\x08"


def isXmlResponse(response):
    # TODO: implement
    if response.url.endswith(".xml"):
        return True
    return False


def regex(x):
    if isinstance(x, str):
        return re.compile(x)
    return x


def sitemapUrlsFromRobots(robots_text, base_url=None):
    """Return iterator over sitemap urls in robots_text"""
    for line in robots_text.splitlines():
        if line.lstrip().lower().startswith("sitemap:"):
            url = line.split(":", 1)[1].strip()
            yield urllib.parse.urljoin(base_url, url)


def iterloc(it, alt=False):
    for d in it:
        ts = d.get("lastmod", None)
        yield (d["loc"], ts)

        # Also consider alternate URLs (xhtml:link rel="alternate")
        if alt and "alternate" in d:
            yield from (d["alternate"], ts)


class SiteMapIterator(object):
    '''Iterates over a single XML sitemap document.
    '''
    def __init__(self, xml_text):
        xmlp = lxml.etree.XMLParser(
            recover=True, remove_comments=True, resolve_entities=False
        )
        self._root = lxml.etree.fromstring(xml_text, parser=xmlp)
        rt = self._root.tag
        self.type = self._root.tag.split("}", 1)[1] if "}" in rt else rt

    def __iter__(self):
        for elem in self._root.getchildren():
            d = {}
            for el in elem.getchildren():
                tag = el.tag
                name = tag.split("}", 1)[1] if "}" in tag else tag

                if name == "link":
                    if "href" in el.attrib:
                        d.setdefault("alternate", []).append(el.get("href"))
                else:
                    d[name] = el.text.strip() if el.text else ""

            if "loc" in d:
                yield d


class SiteMap(object):
    def __init__(self, url, start_from: datetime.datetime, alt_rules=None, session=requests.Session(), url_prefix=None):
        self.sitemap_url = url
        self.sitemap_alternate_links = False
        self.sitemap_rules = [("", "parse")]
        self.sitemap_follow = [""]
        # The Sitemap timestamp is only a date with no timezone, so create a new datetime with only year, month, and day
        if start_from is not None:
            self.start_from = datetime.datetime(
                year=start_from.year,
                month=start_from.month,
                day=start_from.day,
                tzinfo=None
            )
        else:
            self.start_from = None
        self._session = session
        self._url_prefix = url_prefix
        self._cbs = []
        self._all_sitemaps: list[str] = []  # list of all sitemaps visited
        if alt_rules is not None:
            for r, c in alt_rules:
                if isinstance(c, str):
                    c = getattr(self, c)
                self._cbs.append((regex(r), c))
        else:
            for r, c in self.sitemap_rules:
                if isinstance(c, str):
                    c = getattr(self, c)
                self._cbs.append((regex(r), c))
        self._follow = [regex(x) for x in self.sitemap_follow]

    def parse(self, task):
        """Return an object given an entry"""
        url = task.get("url", "")
        igsn = url.split("/")[-1]
        dm = _toDatetimeTZ(task.get("loc_timestamp", ""))
        return (igsn, dm)

    def sitemapFilter(self, entries):
        """Override this to filter entries"""
        for entry in entries:
            yield entry

    def parseSitemap(self, response):  # noqa: C901 -- need to examine computational complexity
        # L.debug("parseSitemap with url: %s", response.url)
        self._all_sitemaps.append(response.url)
        if response.url.endswith("/robots.txt"):
            for url in sitemapUrlsFromRobots(response.text, base_url=response.url):
                yield {"task": "request", "body": {"url": url, "cb": self.parseSitemap}}
        else:
            body = self.getSitemapBody(response)
            if body is None:
                L.warning("Ignoring invalid sitemap: %s", response.url)
                return
            s = SiteMapIterator(body)
            L.info("Sitemap type = %s", s.type)
            s_it = self.sitemapFilter(s)
            if s.type == "sitemapindex":
                for (loc, ts) in iterloc(s_it, self.sitemap_alternate_links):
                    if any(x.search(loc) for x in self._follow):
                        yield {
                            "task": "sitemap",
                            "body": {"url": loc, "cb": self.parseSitemap},
                        }
            elif s.type == "urlset":
                for (loc, ts) in iterloc(s_it, self.sitemap_alternate_links):
                    # ts looks like this: 2018-03-27, shockingly dateparser.parse was very slow on these
                    pieces = ts.split("-")
                    ts_datetime = datetime.datetime(year=int(pieces[0]), month=int(pieces[1]), day=int(pieces[2]), tzinfo=None)
                    for r, c in self._cbs:
                        if r.search(loc) and self.start_from is None or ts_datetime >= self.start_from:
                            req = {
                                "task": "load",
                                "body": {"url": loc, "cb": c, "loc_timestamp": ts},
                            }
                            # L.debug("REQ: %s", req)
                            yield req

    def getSitemapBody(self, response):
        if isXmlResponse(response):
            return response.content
        elif gzipMagicNumber(response):
            return gunzip(response.content)
        elif response.url.endswith(".xml") or response.url.endswith(".xml.gz"):
            return response.content
        L.warning("getSitemapBody no xml: %s", response.url)

    def scanItems(self, iter=None):
        if iter is None:
            response = self._session.get(self.sitemap_url)
            iter = self.parseSitemap(response)
        # if handed an iterator, iterate...
        if isinstance(iter, types.GeneratorType):
            for action in iter:
                task = action.get("task", None)
                if task == "sitemap":
                    url = action["body"]["url"]
                    url = self._prepare_sitemap_url(url)
                    r = self._session.get(url)
                    for item in self.scanItems(action["body"]["cb"](r)):
                        yield item
                elif task == "load":
                    cb = action["body"].pop("cb")
                    params = action["body"]
                    for item in self.scanItems(cb(params)):
                        yield (item)
        # otherwise yield it, if it's not None
        elif iter is not None:
            yield iter

    def _prepare_sitemap_url(self, url):
        if self._url_prefix is not None:
            url = os.path.join(self._url_prefix, url)
        # If the sitemap url doesn't start with an http scheme, prefix it with https
        if not (url.startswith("https://") or url.startswith("http://") or url.startswith("file://")):
            url = f"https://{url}"
        return url


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    url = "https://app.geosamples.org/sitemaps/sitemap-index.xml"
    sm = SiteMap(url, datetime.datetime.now())
    counter = 0
    for item in sm.scanItems():
        counter += 1
        print(f"{counter:07d}: {item}")
