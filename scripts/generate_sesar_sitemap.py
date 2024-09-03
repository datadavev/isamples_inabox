import click

import isb_web.config
import isb_lib.core
from isb_lib.sitemaps import build_sesar_sitemap
from isb_lib.sitemaps.sesar_thing_sitemap import SESARThingSitemapIndexIterator, MAX_URLS_IN_SITEMAP
from isb_web.sqlmodel_database import SQLModelDAO


@click.command()
@click.option(
    "-p",
    "--path",
    type=str,
    default=None,
    help="The disk path where the sitemap files are written",
)
@click.option(
    "-h",
    "--host",
    type=str,
    default=None,
    help="The hostname to include in the sitemap file",
)
@click.option(
    "-o",
    "--offset",
    type=str,
    default="0",
    help="The database offset to start iterating at",
)
@click.pass_context
def main(ctx, path: str, host: str, offset: str):
    print("main")
    isb_lib.core.things_main(ctx, isb_web.config.Settings().database_url, isb_web.config.Settings().solr_url, "INFO")
    session = SQLModelDAO(isb_web.config.Settings().database_url).get_session()
    build_sesar_sitemap(path, host, SESARThingSitemapIndexIterator(session, "SESAR", MAX_URLS_IN_SITEMAP, 200, int(offset)))


if __name__ == "__main__":
    main()
