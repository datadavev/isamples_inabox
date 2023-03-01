import asyncio
import click
import click_config_file
import time
from aiofile import AIOFile, Writer
from sqlmodel import Session

import isb_lib
from isamples_metadata.SESARTransformer import SESARTransformer
from isb_lib.core import ThingRecordIterator
from isb_web.sqlmodel_database import SQLModelDAO


@click.command()
@click.option(
    "-d", "--db_url", default=None, help="SQLAlchemy database URL for storage"
)
@click.option("-f", "--file", default=None, help="The destination output file")
@click.option(
    "-v",
    "--verbosity",
    default="DEBUG",
    help="Specify logging level",
    show_default=True,
)
@click_config_file.configuration_option(config_file_name="isb.cfg")
@click.pass_context
def main(ctx, db_url, file: str, verbosity):
    isb_lib.core.things_main(ctx, db_url, None, verbosity)
    session = SQLModelDAO(db_url).get_session()
    if not file.endswith(".tsv"):
        file = file + ".tsv"
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_models(session, file))


async def run_models(session: Session, dest_path: str):
    start = time.time()
    thing_iterator = ThingRecordIterator(session, "SESAR", page_size=50000)
    counter = 0
    async with AIOFile(dest_path, "w") as aiodf:
        writer = Writer(aiodf)
        header = "id\tmaterial_categories\tmaterial_category_confidences\n"
        await writer(header)
        await aiodf.fsync()
        for thing in thing_iterator.yieldRecordsByPage():
            counter = counter + 1
            if counter % 1000 == 0:
                current_time = time.time()
                print(
                    f"{current_time - start} seconds elapsed, {counter} records processed"
                )
            try:
                transformer = SESARTransformer(thing.resolved_content)
                id = transformer.sample_identifier_string()
                material_categories = transformer.has_material_categories()
                material_category_confidences = (
                    transformer.has_material_category_confidences(material_categories)
                )
                material_categories_str = ",".join(material_categories)
                if (
                    material_category_confidences is not None
                    and len(material_category_confidences) > 0
                ):
                    material_category_confidences_str = ",".join(
                        [
                            str(confidence)
                            for confidence in material_category_confidences
                        ]
                    )
                else:
                    material_category_confidences_str = ""
                record_line = f"{id}\t{material_categories_str}\t{material_category_confidences_str}\n"
                await writer(record_line)
                await aiodf.fsync()
            finally:
                continue


"""
Generates SESAR material categories and confidences
"""
if __name__ == "__main__":
    main()
