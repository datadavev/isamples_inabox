import click
import click_config_file
from sqlmodel import Session, select

import isb_lib.core
from isb_lib.models.thing import Thing, Point
from isb_web.sqlmodel_database import SQLModelDAO, h3_values_without_points
import h3


@click.command()
@click.option(
    "-d", "--db_url", default=None, help="SQLAlchemy database URL for storage"
)
@click.option(
    "-v",
    "--verbosity",
    default="DEBUG",
    help="Specify logging level",
    show_default=True,
)
@click_config_file.configuration_option(config_file_name="isb.cfg")
@click.pass_context
def main(ctx, db_url, verbosity):
    isb_lib.core.things_main(ctx, db_url, None, verbosity)
    session = SQLModelDAO(db_url, echo=True).get_session()
    populate_points(session)


def populate_points(session: Session):
    distinct_h3_select = select(Thing.h3).filter(Thing.h3.isnot(None)).distinct()  # type: ignore
    print("Will select distinct h3 values")
    h3_vals = session.exec(distinct_h3_select).all()
    print("Have distinct h3 values")
    h3_no_points = h3_values_without_points(session, set(h3_vals))
    point_inserts = []
    for h3_no_point in h3_no_points:
        geo = h3.cell_to_latlng(h3_no_point)
        point_inserts.append(
            {"h3": h3_no_point, "latitude": geo[0], "longitude": geo[1]}
        )
    print("Will insert h3 mappings")
    session.bulk_insert_mappings(mapper=Point, mappings=point_inserts)  # type: ignore
    print("Will commit")
    session.commit()


"""
Insert Point rows for h3 values
"""
if __name__ == "__main__":
    main()
