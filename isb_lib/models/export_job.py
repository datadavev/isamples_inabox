from datetime import datetime
from typing import Optional
import sqlalchemy
from sqlmodel import SQLModel, Field

from isb_lib.models.string_list_type import StringListType
from isb_lib.utilities.solr_result_transformer import TargetExportFormat


class ExportJob(SQLModel, table=True):
    primary_key: Optional[int] = Field(
        # Need to use SQLAlchemy here because we can't have the Python attribute named _id or SQLModel won't see it
        sa_column=sqlalchemy.Column(
            "_id",
            sqlalchemy.Integer,
            primary_key=True,
            doc="sequential integer primary key",
        ),
    )
    tcreated: Optional[datetime] = Field(
        default=None,
        nullable=False,
        description="When the job was created.",
        index=False
    )
    tstarted: Optional[datetime] = Field(
        default=None,
        nullable=True,
        description="When the job was started.",
        index=True
    )
    tcompleted: Optional[datetime] = Field(
        default=None,
        nullable=True,
        description="When the job was completed.",
        index=True
    )
    creator_id: Optional[str] = Field(
        default=None,
        nullable=True,
        description="The orcid of the person that created the job.",
        index=True
    )
    uuid: Optional[str] = Field(
        default=None,
        nullable=False,
        description="Unique identifier for this export job.",
        unique=True,
        index=True
    )
    solr_query_params: Optional[list] = Field(
        sa_column=sqlalchemy.Column(
            StringListType,
            nullable=False,
            default=None,
            doc="Serialized solr query parameter for the export job.",
        )
    )
    export_format: Optional[str] = Field(
        default=TargetExportFormat.CSV.value,
        nullable=False,
        description="Format of the exported data.",
        index=False
    )
    file_path: Optional[str] = Field(
        default=None,
        nullable=True,
        description="The path to the exported file.",
        index=False
    )
