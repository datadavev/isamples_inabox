import asyncio
import datetime
import json
import os.path
import shutil
import tempfile
from xml.etree import ElementTree

import lxml
import pytest
import requests

from isb_lib.sitemaps import SitemapIndexEntry, ThingSitemapIndexEntry, UrlSetEntry, ThingUrlSetEntry, \
    write_urlset_file, write_sitemap_index_file, INDEX_XML, build_sitemap, SiteMap
from isb_lib.sitemaps.gh_pages_sitemap import GHPagesSitemapIndexIterator
from isb_lib.sitemaps.thing_sitemap import ThingSitemapIndexIterator
from test_utils import LocalFileAdapter


def test_sitemap_index_entry():
    entry = SitemapIndexEntry("sitemap_0.xml", "1234556")
    assert entry is not None
    assert entry.loc_suffix() is not None


def test_thing_sitemap_index_entry():
    entry = ThingSitemapIndexEntry("sitemap_0.xml", "1234556")
    assert entry is not None
    assert entry.loc_suffix() is not None


def test_url_set_entry():
    entry = UrlSetEntry("ark:/21547/DSz2761.json", "1234556")
    assert entry is not None
    assert entry.loc_suffix() is not None


def test_thing_url_set_entry():
    entry = ThingUrlSetEntry("ark:/21547/DSz2761", "1234556")
    assert entry is not None
    assert entry.loc_suffix() is not None


def _assert_file_exists_and_is_xml(path):
    assert os.path.exists(path)
    with open(path) as f:
        text = f.read()
        xmlp = lxml.etree.XMLParser(
            recover=True, remove_comments=True, resolve_entities=False
        )
        root = lxml.etree.fromstring(text.encode("UTF-8"), parser=xmlp)
        assert root is not None
    os.remove(path)


def test_write_urlset_file():
    path = os.path.join(tempfile.gettempdir(), "sitemap-0.xml")
    host = "https://hyde.cyverse.org"
    urls = [UrlSetEntry("ark:/21547/DSz2761.json", "1234556")]
    asyncio.get_event_loop().run_until_complete(write_urlset_file(path, host, urls))
    _assert_file_exists_and_is_xml(path)


def test_write_sitemap_index_file():
    host = "https://hyde.cyverse.org"
    filename = "sitemap_0.xml"
    path = os.path.join(tempfile.gettempdir(), INDEX_XML)
    asyncio.get_event_loop().run_until_complete(write_sitemap_index_file(tempfile.gettempdir(), host, [SitemapIndexEntry(filename, "1234556")]))
    _assert_file_exists_and_is_xml(path)


def test_build_sitemap():
    path = tempfile.mkdtemp()
    dummy_thing = {
        "@id": "123456"
    }
    json_object = json.dumps(dummy_thing, indent=4)
    json_path = os.path.join(path, "thing_1.json")
    with open(json_path, "w") as outfile:
        outfile.write(json_object)
        build_sitemap(path, "https://hyde.cyverse.org", GHPagesSitemapIndexIterator(path))
        _assert_file_exists_and_is_xml(os.path.join(path, INDEX_XML))


@pytest.fixture
def local_file_requests_session():
    requests_session = requests.session()
    requests_session.mount("file://", LocalFileAdapter())
    return requests_session


def test_sitemap_scan_items(local_file_requests_session):
    local_url_path = f"file://{os.path.join(os.getcwd(), 'test_data/sitemaps')}"
    file_url = os.path.join(local_url_path, "test_sitemap_index.xml")
    sitemap = SiteMap(file_url, datetime.datetime(year=1900, month=1, day=1, hour=0, minute=0), None,
                      local_file_requests_session, local_url_path)
    for item in sitemap.scanItems():
        assert item is not None


def test_thing_sitemap_index_iterator():
    directory_path = os.path.join(os.getcwd(), "./test_data/example_sitemap_files")
    dest_directory_path = os.path.join(directory_path, "generated")
    if os.path.exists(dest_directory_path):
        shutil.rmtree(dest_directory_path)
    os.mkdir(dest_directory_path)
    sitemap_index_iterator = ThingSitemapIndexIterator(directory_path)
    build_sitemap(dest_directory_path, "https://central.isample.xyz/sitemaps/123456/", sitemap_index_iterator)
    sitemap_index_path = os.path.join(dest_directory_path, "sitemap-index.xml")
    sitemap_zero = os.path.join(dest_directory_path, "sitemap-0.xml")
    assert os.path.exists(sitemap_index_path)
    assert os.path.exists(sitemap_zero)
    sitemap_tree = ElementTree.parse(sitemap_zero)
    # The urls should be ordered by filename -- assert that they are
    for child in sitemap_tree.getroot():
        if "url" in child.tag:
            for url_child in child:
                if "loc" in url_child.tag:
                    assert f"sitemap-" in url_child.text