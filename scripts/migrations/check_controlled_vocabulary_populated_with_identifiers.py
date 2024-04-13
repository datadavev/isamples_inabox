import click
import term_store

import isb_lib.core
from isamples_metadata.GEOMETransformer import GEOMETransformer
from isamples_metadata.OpenContextTransformer import OpenContextTransformer
from isamples_metadata.SESARTransformer import SESARTransformer
from isamples_metadata.SmithsonianTransformer import SmithsonianTransformer
from isamples_metadata.metadata_constants import METADATA_HAS_MATERIAL_CATEGORY, METADATA_HAS_CONTEXT_CATEGORY, \
    METADATA_HAS_SPECIMEN_CATEGORY
from isb_lib.vocabulary import vocab_adapter
from isb_web.sqlmodel_database import SQLModelDAO, random_things_with_authority, taxonomy_name_to_kingdom_map
from isb_web.vocabulary import SAMPLEDFEATURE_URI, MATERIAL_URI, PHYSICALSPECIMEN_URI


@click.command()
@click.option(
    "-d", "--db_url", default=None, help="SQLAlchemy database URL for storage"
)
@click.option(
    "-a", "--authority", default=None, help="Authority to use for lookup"
)
@click.option(
    "-v",
    "--verbosity",
    default="DEBUG",
    help="Specify logging level",
    show_default=True,
)
@click.pass_context
def main(ctx, db_url, authority, verbosity):
    isb_lib.core.things_main(ctx, db_url, None, verbosity)
    session = SQLModelDAO(db_url, echo=True).get_session()
    repository = term_store.get_repository(session)
    vocab_adapter.uijson_vocabulary_dict(SAMPLEDFEATURE_URI, repository)
    vocab_adapter.uijson_vocabulary_dict(MATERIAL_URI, repository)
    vocab_adapter.uijson_vocabulary_dict(PHYSICALSPECIMEN_URI, repository)

    authorities = ["SMITHSONIAN", "GEOME", "OPENCONTEXT", "SESAR"]
    taxon_name_map = taxonomy_name_to_kingdom_map(session)
    for authority in authorities:
        print(f"About to check {authority}")
        random_things = random_things_with_authority(session, authority, 1000)
        for thing in random_things:
            if authority == "SMITHSONIAN":
                transformer = SmithsonianTransformer(thing.resolved_content)
            elif authority == "GEOME":
                transformer = GEOMETransformer(thing.resolved_content, None, session, taxon_name_map)
            elif authority == "OPENCONTEXT":
                transformer = OpenContextTransformer(thing.resolved_content)
            elif authority == "SESAR":
                transformer = SESARTransformer(thing.resolved_content)
            transformed = transformer.transform()
            assert len(transformed.get(METADATA_HAS_MATERIAL_CATEGORY)) >= 1
            assert len(transformed.get(METADATA_HAS_CONTEXT_CATEGORY)) >= 1
            assert len(transformed.get(METADATA_HAS_SPECIMEN_CATEGORY)) >= 1
        print("")


"""
Do a sanity check that the things are returning proper controlled vocabulary identifiers when running the script
"""
if __name__ == "__main__":
    main()
