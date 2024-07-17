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
from scripts.consume_sitemaps import _json_line_to_thing_dict
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
                    assert "sitemap-" in url_child.text


def test_json_line_into_thing_dict():
    json_line = """{"label": "Bathymodiolus sp. AJ9VQ03", "keywords": [{"keyword": "Animalia"}, {"keyword": "Bathymodiolus sp."}, {"keyword": "Bivalvia"}, {"keyword": "IZ"}, {"keyword": "Mollusca"}, {"keyword": "Mytilidae"}, {"keyword": "North Atlantic Ocean"}], "description": "basisOfRecord: MaterialSample | occurrenceRemarks: Order: 2885; Box Number: MBARI_0036; Box Position: F/5; MBARI Note: SIO Box 6; Sinatra | catalogNumber: USNM 1464106 | recordNumber: A3120-(B3-5) | fieldNumber: AL-3120 | type: PhysicalObject | individualCount: 1 | disposition: in collection | startDayOfYear: 191 | endDayOfYear: 191", "produced_by": {"label": "", "identifier": "", "result_time": "1997-07-10", "sampling_site": {"label": "MID-ATLANTIC RIDGE - Lucky Strike", "place_name": ["MID-ATLANTIC RIDGE - Lucky Strike", "North Atlantic Ocean"], "description": "verbatimLatitude: 37-17.629N | verbatimLongitude: 32-16.532W", "sample_location": {"latitude": 37.2938, "elevation": "", "longitude": -32.2755}}, "responsibility": [{"name": " R. Vrijenhoek et al.", "role": "recordedBy"}], "has_feature_of_interest": ""}, "sample_identifier": "ark:/65665/300008335-8d74-4c3f-873c-a9d8b4b3d6a8", "source_collection": "SMITHSONIAN", "has_context_category": [{"label": "Marine water body bottom"}], "has_material_category": [{"label": "Organic material"}], "has_specimen_category": [{"label": "Organism part"}], "informal_classification": ["Bathymodiolus sp."]}"""
    json_dict = json.loads(json_line)
    now = datetime.datetime.now()
    thing_dict, identifier = _json_line_to_thing_dict(json_dict, "https://henry.cyverse.org/smithsonian/sitemaps/sitemap-0.jsonl",
                                          now, now)
    assert thing_dict is not None
    assert identifier is not None
    assert thing_dict["h3"] is not None
    