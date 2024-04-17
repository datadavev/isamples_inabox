import click


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
def main():
    print()


if __name__ == "__main__":
    main()
