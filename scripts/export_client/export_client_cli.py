import click
import requests
import time

from isamples_export_client.export_client import ExportClient


@click.command()
@click.option(
    "-q",
    "--query",
    help="The solr query to execute.",
)
@click.option(
    "-d",
    "--destination",
    help="The destination directory where the downloaded content should be written.",
)
@click.option(
    "-t",
    "--jwt",
    help="The JWT for the authenticated user.",
)
@click.option(
    "-u",
    "--url",
    help="The URL to the iSamples server to export from.",
    default="https://central.isample.xyz/isamples_central/export"
)
@click.option(
    "-f",
    "--format",
    help="The format of the exported content.",
    type=click.Choice(["jsonl", "csv"], case_sensitive=False),
    default="jsonl"
)
def main(query: str, destination: str, jwt: str, export_url: str, format: str):
    client = ExportClient(query, destination, jwt, export_url, format)
    client.perform_full_download()


if __name__ == "__main__":
    main()
