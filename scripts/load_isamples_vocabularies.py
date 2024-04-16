import click
import click_config_file
import term_store
import term_store.vocab_terms
from term_store import TermRepository

from isb_web.sqlmodel_database import SQLModelDAO

"""Loads the vocabularies used for iSamples things into the iSamples PostGreSQL database"""


def load_terms(repository: TermRepository):
    vocab = term_store.vocab_terms.SKOSVocabulary()
    vocab.load("https://raw.githubusercontent.com/isamplesorg/vocabularies/main/vocabulary/material_sample_type.ttl")
    vocab.load("https://raw.githubusercontent.com/isamplesorg/vocabularies/main/vocabulary/material_type.ttl")
    vocab.load("https://raw.githubusercontent.com/isamplesorg/vocabularies/main/vocabulary/sampled_feature_type.ttl")
    vocab.load("https://raw.githubusercontent.com/isamplesorg/metadata_profile_biology/main/vocabulary/biology_sampledfeature_extension.ttl")
    vocab.load("https://raw.githubusercontent.com/isamplesorg/metadata_profile_earth_science/main/vocabulary/earthenv_material_extension_mineral_group.ttl")
    vocab.load("https://raw.githubusercontent.com/isamplesorg/metadata_profile_earth_science/main/vocabulary/earthenv_material_extension_rock_sediment.ttl")
    vocab.load("https://raw.githubusercontent.com/isamplesorg/metadata_profile_earth_science/main/vocabulary/earthenv_specimen_type.ttl")
    vocab.load_terms_to_model_store(repository)


@click.command()
@click.option(
    "-d", "--db_url", default=None, help="SQLAlchemy database URL for storage"
)
@click_config_file.configuration_option(config_file_name="isb.cfg")
def main(db_url):
    dao = SQLModelDAO(db_url)
    term_store.clear_database(dao.engine)
    term_store.create_database(dao.engine)
    session = term_store.get_session(dao.engine)
    repository = term_store.get_repository(session)
    load_terms(repository)


if __name__ == "__main__":
    main()
