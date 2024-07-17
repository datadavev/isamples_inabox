import typing
import datetime
from typing import Optional

import isamples_metadata.SmithsonianTransformer

import isb_lib
import isb_lib.core
import logging

from isamples_metadata import SmithsonianTransformer
from isamples_metadata.Transformer import Transformer
from isamples_metadata.core_json_transformer import CoreJSONTransformer
from isb_lib.core import MEDIA_JSONL
from isb_lib.models.thing import Thing


class SmithsonianItem(object):
    AUTHORITY_ID = "SMITHSONIAN"
    TEXT_CSV = "text/csv"

    def __init__(self, identifier: str, source_dict: typing.Dict):
        self.identifier = identifier
        self.source_item = source_dict

    def as_thing(
        self,
        t_created: Optional[datetime.datetime],
        status: int,
        source_file_path: str,
        t_resolved: datetime.datetime,
    ) -> Thing:
        logging.debug("SmithsonianItem.asThing")
        _thing = Thing(
            id=self.identifier,
            tcreated=t_created,
            item_type=None,
            authority_id=SmithsonianItem.AUTHORITY_ID,
            resolved_url=f"file://{source_file_path}",
            resolved_status=status,
            tresolved=t_resolved,
            resolve_elapsed=0,
        )
        if not isinstance(self.source_item, dict):
            logging.error("Item is not an object")
            return _thing
        _thing.item_type = "sample"
        _thing.resolved_media_type = SmithsonianItem.TEXT_CSV
        # Note that this field doesn't make sense for Smithsonian as the information is coming from a local file
        # _thing.resolve_elapsed = resolve_elapsed
        _thing.resolved_content = self.source_item
        _thing.h3 = SmithsonianTransformer.geo_to_h3(_thing.resolved_content)
        return _thing


def load_thing(
    thing_dict: typing.Dict, t_resolved: datetime.datetime, file_path: str
) -> Thing:
    """
    Load a thing from its source.

    Minimal parsing of the thing is performed to populate the database record.

    Args:
        thing_dict: Dictionary representing the thing
        t_resolved: When the item was resolved from the source

    Returns:
        Instance of Thing
    """
    L = isb_lib.core.getLogger()

    # For the purposes of the Things db, we want to use a normalized form of the identifier.  Note that there is one
    # other column in the Smithsonian dump that we'd need to transform to the normalized form if we wanted to use it,
    # that is occurrenceID.  Currently it is unused in our Transformer codebase.
    normalized_id = isb_lib.normalized_id(thing_dict["id"])
    try:
        t_created = datetime.datetime(
            year=int(thing_dict["year"]),
            month=int(thing_dict["month"]),
            day=int(thing_dict["day"]),
        )
    except ValueError:
        # In many cases, these don't seem to be populated.  There's nothing we can do if they aren't there, so just
        # leave it as None.
        t_created = None
    L.info("loadThing: %s", normalized_id)
    item = SmithsonianItem(normalized_id, thing_dict)
    thing = item.as_thing(t_created, 200, file_path, t_resolved)
    return thing


def _validate_resolved_content(thing: Thing):
    isb_lib.core.validate_resolved_content(SmithsonianItem.AUTHORITY_ID, thing)


def reparse_as_core_record(thing: Thing) -> typing.List[typing.Dict]:
    _validate_resolved_content(thing)
    try:
        if thing.resolved_content is not None:
            transformer: Transformer
            if thing.resolved_media_type == MEDIA_JSONL:
                transformer = CoreJSONTransformer(thing.resolved_content)
            else:
                transformer = isamples_metadata.SmithsonianTransformer.SmithsonianTransformer(
                    thing.resolved_content
                )
            solr_doc = isb_lib.core.coreRecordAsSolrDoc(transformer)
            # This isn't present in Smithsonian data.  Fall back to the value on Thing
            solr_doc["sourceUpdatedTime"] = isb_lib.core.datetimeToSolrStr(thing.tstamp)
            return [solr_doc]
        else:
            return []
    except Exception:
        logging.fatal(
            "Failed trying to run transformer on %s", str(thing.resolved_content)
        )
        raise
