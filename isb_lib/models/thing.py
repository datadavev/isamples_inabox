import igsn_lib.time
import json
from typing import Optional
import typing
from sqlmodel import Field, SQLModel, create_engine, Session, select
from datetime import datetime
import sqlalchemy
import isb_web.config
import click
import click_config_file

class Thing(SQLModel, table=True):
    primary_key: Optional[int] = Field(
        # Need to use SQLAlchemy here because we can't have the Python attribute named _id or SQLModel won't see it
        sa_column=sqlalchemy.Column(
            "_id",
            sqlalchemy.Integer,
            primary_key=True,
            doc="sequential integer primary key, good for paging",
        ),
    )
    id: Optional[str] = Field(
        default=None,
        index=True,
        nullable=False,
        description="identifier scheme:value, globally unique",
    )
    tstamp: datetime = Field(
        default=igsn_lib.time.dtnow(),
        description="When the entry was added to this database, JD",
    )
    tcreated: Optional[datetime] = Field(
        default=None,
        nullable=True,
        description="When the record was created, if available",
    )
    item_type: Optional[str] = Field(
        default=None,
        index=True,
        nullable=True,
        description="Type of thing described by this identifier",
    )
    authority_id: Optional[str] = Field(
        default=None,
        nullable=True,
        description="Authority of this thing",
    )
    resolved_url: Optional[str] = Field(
        default=None,
        nullable=False,
        description="URL that was resolved for the identifier",
    )
    resolved_status: Optional[int] = Field(
        default=None,
        nullable=True,
        index=True,
        description="Status code of the resolve response",
    )
    tresolved: Optional[datetime] = Field(
        default=None, nullable=True, description="When the record was resolved"
    )
    resolve_elapsed: Optional[float] = Field(
        default=None, nullable=True, description="Time in seconds to resolve record"
    )
    resolved_content: Optional[str] = Field(
        default=None,
        nullable=True,
        description="Resolved content, {content_type:, content: }",
    )
    resolved_media_type: Optional[str] = Field(
        default=None, nullable=True, description="Media type of resolved content"
    )

    def __repr__(self):
        return json.dumps(self.as_json_dict(), indent=2)

    def as_json_dict(self) -> typing.Dict:
        res = {
            "id": self.id,
            "tstamp": igsn_lib.time.datetimeToJsonStr(self.tstamp),
            "tcreated": igsn_lib.time.datetimeToJsonStr(self.tcreated),
            "item_type": self.item_type,
            "authority_id": self.authority_id,
            "resolved_url": self.resolved_url,
            "resolved_status": self.resolved_status,
            "tresolved": igsn_lib.time.datetimeToJsonStr(self.tresolved),
            "resolved_content": self.resolved_content,
            "resolved_elapsed": self.resolve_elapsed,
            "resolved_media_type": self.resolved_media_type,
        }
        return res


@click.command()
@click_config_file.configuration_option(config_file_name="opencontext.cfg")
@click.pass_context
def main(context):
    engine = create_engine(context.default_map['db_url'], echo=True)
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        statement = select(Thing).limit(10)
        results = session.exec(statement)
        things = results.all()
        print(f"Things are {things}")


if __name__ == "__main__":
    main()
