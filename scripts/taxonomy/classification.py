import click
import click_config_file
from isamples_metadata.Transformer import Transformer
from isamples_metadata import SESARTransformer, OpenContextTransformer

import isb_lib
from isb_lib.core import ThingRecordIterator
from isb_web.sqlmodel_database import SQLModelDAO
import json


@click.command()
@click.option(
    "-d",
    "--db_url",
    default=None,
    help="Postgres database URL",
)
@click.option("-s", "--solr_url", default=None, help="Solr index URL")
@click.option(
    "-m",
    "--max_records",
    type=int,
    default=1000,
    help="Maximum records to load, -1 for all",
)
@click.option(
    "-a",
    "--authority_id",
    default="SESAR",
    help="Collection type",
)
@click.option(
    "-v",
    "--verbosity",
    default="DEBUG",
    help="Specify logging level",
    show_default=True,
)
@click_config_file.configuration_option()
@click.pass_context
def generate_test_records(
    ctx, db_url: str, solr_url: str, max_records: int,
    authority_id: str, verbosity: str
):
    isb_lib.core.things_main(ctx, db_url, solr_url, verbosity)
    db_session = SQLModelDAO(db_url).get_session()
    thing_iterator = ThingRecordIterator(
        db_session,
        authority_id=authority_id,
        page_size=max_records
    )
    material_result = {}
    specimen_result = {}
    MATERIAL_TEST_RECORD_SIZE = 200
    SAMPLE_TEST_RECORD_SIZE = 200

    for thing in thing_iterator.yieldRecordsByPage():
        if authority_id == "SESAR":
            igsn = "IGSN:" + thing.resolved_content["igsn"]
            sesar_transformer = SESARTransformer.SESARTransformer(thing.resolved_content)
            material_type = sesar_transformer._material_type()
            if not material_type:
                material_pred = sesar_transformer.has_material_categories()
                material_confidence = sesar_transformer.has_material_category_confidences([])
                material_dict = {"material": {"values": material_pred, "confidence": material_confidence}}
                material_result[igsn] = material_dict
        else:
            citation_uri = thing.resolved_content["citation uri"]
            oc_transformer = OpenContextTransformer.OpenContextTransformer(thing.resolved_content)
            if not oc_transformer._material_type():
                material_pred = oc_transformer.has_material_categories()
                material_confidence = oc_transformer.has_material_category_confidences([])
                if not material_confidence:
                    # rule defined
                    material_confidence = [Transformer.RULE_BASED_CONFIDENCE]
                    material_dict = {"material": {"values": material_pred, "confidence": material_confidence}}
                    material_result[citation_uri] = material_dict
            if not oc_transformer._specimen_type():
                specimen_pred = oc_transformer.has_sample_object_types()
                specimen_confidence = oc_transformer.has_sample_object_type_confidences([])
                if not specimen_confidence:
                    specimen_confidence = [Transformer.RULE_BASED_CONFIDENCE]
                    specimen_dict = {"materialSample": {"values": specimen_pred, "confidence": specimen_confidence}}
                    specimen_result[citation_uri] = specimen_dict
        if len(material_result) == MATERIAL_TEST_RECORD_SIZE and len(specimen_result) == SAMPLE_TEST_RECORD_SIZE:
            with open('sesar_material_test_model_values.json', 'w') as f:
                f.write(json.dumps(material_result, indent=4))
            with open('specimen_test_records.json', 'w') as f:
                f.write(json.dumps(specimen_result, indent=4))
            break

    db_session.close()


if __name__ == "__main__":
    # generate the test records for indexing
    generate_test_records()
