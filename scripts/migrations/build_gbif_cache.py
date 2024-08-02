import click
import pandas
from sqlmodel import Session

import isb_lib.core
from isb_lib.models.taxonomy_name import TaxonomyName
from isb_web.sqlmodel_database import SQLModelDAO, save_taxonomy_name


@click.command()
@click.option(
    "-d", "--db_url", default=None, help="SQLAlchemy database URL for storage"
)
@click.option(
    "-f", "--file", default=None, help="The path to the GBIF tsv file with all the taxon info"
)
@click.option(
    "-b", "--batch_size", default=10000, help="The batch size to use when writing to the database"
)
@click.option(
    "-v",
    "--verbosity",
    default="DEBUG",
    help="Specify logging level",
    show_default=True,
)
@click.pass_context
def main(ctx, db_url, file, batch_size, verbosity):
    isb_lib.core.things_main(ctx, db_url, None, verbosity)
    session = SQLModelDAO((ctx.obj["db_url"]), echo=True).get_session()
    read_taxon_data(session, batch_size, file)


def read_taxon_data(session: Session, batch_size: int, taxon_file: str):
    dataframe = pandas.read_csv(taxon_file, sep="\t", on_bad_lines="warn")
    current_batch_size = 0
    total_rows = 0
    for row in dataframe.iterrows():
        data = row[1]
        print(f"scientific name is : {data.scientificName}, canonical name is: {data.canonicalName} kingdom is: {data.kingdom}")
        if data.kingdom is not None:
            current_batch_size += 1
            taxonomy_name = TaxonomyName()
            taxonomy_name.name = data.scientificName
            taxonomy_name.kingdom = data.kingdom
            save_taxonomy_name(session, taxonomy_name)
            if data.canonicalName is not None:
                taxonomy_name_canonical = TaxonomyName()
                taxonomy_name_canonical.name = data.canonicalName
                taxonomy_name_canonical.kingdom = data.kingdom
                save_taxonomy_name(session, taxonomy_name_canonical)
            if current_batch_size == batch_size:
                session.commit()
                total_rows += current_batch_size
                current_batch_size = 0
                print(f"Just wrote to database, written {total_rows} rows")
    session.commit()


"""
Takes a .tsv file from the GBIF Backbone (https://www.gbif.org/dataset/d7dddbf4-2cf0-4f39-9b2a-bb099caae36c) and
transforms to a database index for use when building the GEOME solr index
"""
if __name__ == "__main__":
    main()
