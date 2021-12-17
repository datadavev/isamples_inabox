import igsn_lib.time
import json
from typing import Optional, List
import typing
from sqlmodel import Field, SQLModel, Relationship
from datetime import datetime
import sqlalchemy


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
            sqlalchemy.JSON,
            nullable=True,
            default=None,
            doc="Resolved content, {content_type:, content: }",
        ),
    )
    resolved_media_type: Optional[str] = Field(
        default=None, nullable=True, description="Media type of resolved content"
    )
    identifiers: List["ThingIdentifier"] = Relationship(
        back_populates="thing", sa_relationship_kwargs={"cascade": "all,delete-orphan"}
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

    def take_values_from_json_dict(self, json_dict: typing.Dict):
        self.authority_id = json_dict["authority_id"]
        self.id = json_dict["id"]
        self.resolved_content = json_dict["resolved_content"]
        self.tcreated = json_dict["tcreated"]
        self.tresolved = json_dict["tresolved"]
        self.resolved_url = json_dict["resolved_url"]
        self.resolved_media_type = json_dict["resolved_media_type"]
        self.item_type = json_dict["item_type"]
        self.resolved_status = json_dict["resolved_status"]
        self.resolve_elapsed = json_dict["resolve_elapsed"]
        # Update the tstamp to now, to indicate this is the last modified date from an iSamples perspective
        self.tstamp = datetime.now()


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
    thing: Optional[Thing] = Relationship(
        back_populates="identifiers", sa_relationship_kwargs={"cascade": "all"}
    )
