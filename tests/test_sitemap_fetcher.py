import pytest
import requests
import os
import datetime
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, create_engine, Session

from isb_lib.sitemaps.sitemap_fetcher import (
    SitemapIndexFetcher,
    SitemapFileFetcher,
)
from test_utils import LocalFileAdapter


class LocalFileSitemapIndexFetcher(SitemapIndexFetcher):
    def prepare_sitemap_file_url(self, file_url: str) -> str:
        """Overridden to reconstruct file urls by subbing in the cwd plus the relative file path"""
        filename = os.path.join(os.getcwd(), file_url)
        return f"file://{filename}"

    def sitemap_file_fetcher(self, url: str) -> SitemapFileFetcher:
        return LocalFileSitemapFileFetcher(
            self.prepare_sitemap_file_url(url),
            self._authority,
            self._last_modified,
            self._session,
        )


class LocalFileSitemapFileFetcher(SitemapFileFetcher):
    def prepare_thing_file_url(self, file_url: str) -> str:
        """Overridden to reconstruct file urls by subbing in the cwd plus the relative file path"""
        filename = os.path.join(os.getcwd(), file_url)
        return f"file://{filename}"


sitemap_fetcher_test_values = [
    ("test_data/sitemaps/test_sitemap_index.xml", None, 2),
    (
        "test_data/sitemaps/test_sitemap_index.xml",
        datetime.datetime(year=2015, month=11, day=22, hour=12, minute=34),
        2,
    ),
    (
        "test_data/sitemaps/test_sitemap_index.xml",
        datetime.datetime(year=2020, month=11, day=22, hour=12, minute=34),
        1,
    ),
]


@pytest.fixture
def local_file_requests_session():
    requests_session = requests.session()
    requests_session.mount("file://", LocalFileAdapter())
    return requests_session


@pytest.fixture(name="sqlmodel_session")
def session_fixture():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


@pytest.mark.parametrize(
    "sitemap_filename,last_mod_date,expected_num_urls", sitemap_fetcher_test_values
)
def test_sitemap_fetcher(
    sitemap_filename: str,
    last_mod_date: datetime.datetime,
    expected_num_urls: int,
    local_file_requests_session,
    sqlmodel_session,
):
    filename = os.path.join(os.getcwd(), sitemap_filename)
    sitemap_index_file = f"file://{filename}"
    sitemap_fetcher = LocalFileSitemapIndexFetcher(
        sitemap_index_file, "OPENCONTEXT", last_mod_date, local_file_requests_session
    )
    sitemap_fetcher.fetch_index_file()
    assert expected_num_urls == len(sitemap_fetcher.urls_to_fetch)
    # This kicks off the request for each child file
    child_fetchers = sitemap_fetcher.fetch_child_files()
    for child_fetcher in child_fetchers:
        # Note that this data has been doctored to have the same number of qualified urls for the sitemap index
        # and the individual child files, so this assertion is valid -- otherwise this wouldn't necessarily work
        assert expected_num_urls == len(child_fetcher.urls_to_fetch)
