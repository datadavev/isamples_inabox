import click

import isb_web.config
import isb_lib.core
from isb_lib.sitemaps import build_sesar_sitemap
from isb_lib.sitemaps.sesar_thing_sitemap import SESARThingSitemapIndexIterator
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
@click.pass_context
def main(ctx, path: str, host: str):
    print("main")
    isb_lib.core.things_main(ctx, isb_web.config.Settings().database_url, isb_web.config.Settings().solr_url, "INFO")
    session = SQLModelDAO(isb_web.config.Settings().database_url).get_session()
    build_sesar_sitemap(path, host, SESARThingSitemapIndexIterator(session, "SESAR", 100))


if __name__ == "__main__":
    main()