import igsn_lib.time
import json
from typing import Optional
import typing
from sqlmodel import Field, SQLModel
from datetime import datetime
import sqlalchemy

from isb_lib.models.conditional_jsonb_type import ConditionalJSONB
from isb_lib.models.string_list_type import StringListType

MEDIA_JSONL = "application/jsonl"

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
    resolved_content: Optional[dict] = Field(
        # Use the raw SQLAlchemy column in order to get the proper JSON behavior
        sa_column=sqlalchemy.Column(
            ConditionalJSONB,
            nullable=True,
            default=None,
            doc="Resolved content, {content_type:, content: }",
        ),
    )
    resolved_media_type: Optional[str] = Field(
        default=None, nullable=True, description="Media type of resolved content"
    )
    identifiers: Optional[list[str]] = Field(
        sa_column=sqlalchemy.Column(
            StringListType,
            nullable=True,
            default=None,
            doc="Additional identifiers used to look up the Thing"
        )
    )
    h3: Optional[str] = Field(
        default=None, nullable=True, description="The h3 value (https://uber.github.io/h3-py/intro.html)"
    )

    def insert_thing_identifier_if_not_present(self, identifier: str):
        if self.identifiers is None:
            self.identifiers = []
        if identifier not in self.identifiers:
            self.identifiers.append(identifier)

    def take_values_from_other_thing(self, other_thing: "Thing"):
        self.primary_key = other_thing.primary_key
        self.id = other_thing.id
        self.resolved_content = other_thing.resolved_content
        self.resolved_url = other_thing.resolved_url
        self.resolved_status = other_thing.resolved_status
        self.tresolved = other_thing.tresolved
        self.resolve_elapsed = other_thing.resolve_elapsed
        self.tcreated = other_thing.tcreated
        self.tstamp = other_thing.tstamp

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

    def is_transformed(self):
        return self.resolved_media_type is not None and self.resolved_media_type == MEDIA_JSONL


class ThingIdentifier(SQLModel, table=True):
    guid: Optional[str] = Field(
        primary_key=True,
        default=None,
        nullable=False,
        index=False,
        description="The String GUID",
    )
    tstamp: datetime = Field(
        default=igsn_lib.time.dtnow(),
        description="When the identifier was added to this database",
        index=False,
    )
    thing_id: Optional[int] = Field(
        default=None, nullable=False, foreign_key="thing._id", index=False
    )


class Point(SQLModel, table=True):
    h3: Optional[str] = Field(
        primary_key=True,
        default=None,
        nullable=False,
        index=False,
        description="The h3 value representing the geo"
    )
    longitude: Optional[float] = Field(
        default=None, nullable=False, description="Longitude of the geo", index=False
    )
    latitude: Optional[float] = Field(
        default=None, nullable=False, description="Latitude of the geo", index=False
    )
    height: Optional[float] = Field(
        default=None, nullable=True, description="Height of the geo, as returned by Cesium.  Null if uncalculated."
    )
