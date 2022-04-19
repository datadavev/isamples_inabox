"""
Implements an adapter for getting records from GEOME
"""
import logging
import functools
import datetime
import typing
import dateparser

import isamples_metadata.GEOMETransformer
import requests
import igsn_lib.oai
import igsn_lib.time
import isb_lib.core
from isamples_metadata import GEOMETransformer
from isb_lib.models.thing import Thing

HTTP_TIMEOUT = 10.0  # seconds
GEOME_API = "https://api.geome-db.org/v1/"


def getLogger():
    return logging.getLogger("isb_lib.geome_adapter")


@functools.cache
def _datetimeFromSomething(tstr):
    return igsn_lib.time.datetimeFromSomething(tstr)


def geomeEventRecordTimestamp(record):
    t_collected = None
    try:
        _year = record.get("yearCollected", "")
        tstr = f"{_year}-01-01"
        if _year != "":
            _month = record.get("monthCollected", "")
            if _month != "":
                tstr = f"{_year}-{_month}"
                _day = record.get("dayCollected", "")
                if _day != "":
                    tstr = f"{_year}-{_month}-{_day}"
                    _tod = record.get("timeOfDay", "")
                    if _tod != "":
                        tstr = f"{tstr} {_tod}"
            t_collected = _datetimeFromSomething(tstr)
            # L.debug("T: %s, %s", tstr, t_collected)
    except Exception:
        pass
    return t_collected


class GEOMEIdentifierIterator(isb_lib.core.IdentifierIterator):
    def __init__(
        self,
        offset: int = 0,
        max_entries: int = -1,
        date_start: datetime.datetime = None,
        date_end: datetime.datetime = None,
        record_type: str = "Sample",
    ):
        super().__init__(
            offset=offset,
            max_entries=max_entries,
            date_start=date_start,
            date_end=date_end,
        )
        self._record_type = record_type
        self._project_ids = None

    def listProjects(self):
        L = getLogger()
        L.debug("Loading project ids...")
        url = f"{GEOME_API}projects"
        headers = {
            "Accept": "application/json",
        }
        params = {"includePublic": "true", "admin": "false"}
        response = requests.get(
            url, params=params, headers=headers, timeout=HTTP_TIMEOUT
        )
        if response.status_code != 200:
            raise ValueError(
                "Unable to load projects, status: %s; reason: %s",
                response.status_code,
                response.reason,
            )
        projects = response.json()
        for project in projects:
            L.debug("project id: %s", project.get("projectId", None))
            yield project

    def recordsInProject(self, project_id, record_type):
        L = getLogger()
        L.debug("recordsInProject project %s", project_id)
        params = {"includePublic": "true", "admin": "false"}
        url = f"{GEOME_API}projects/{project_id}/expeditions"
        headers = {
            "Accept": "application/json",
        }
        _page_size = 1000
        response = requests.get(
            url, params=params, headers=headers, timeout=HTTP_TIMEOUT
        )
        if response.status_code != 200:
            L.error(
                "Unable to load project expeditions project:%s; status: %s; reason: %s",
                project_id,
                response.status_code,
                response.reason,
            )
            return
        expeditions_json = response.json()
        if len(expeditions_json) == 0:
            L.error("Project doesn't have any expeditions project:%s", project_id)
        for expedition_dict in expeditions_json:
            # Skip developer messages
            if type(expedition_dict) is dict:
                expedition_modified_date = expedition_dict["modified"]
                expedition_modified_datetime = dateparser.parse(expedition_modified_date)
                # For each expedition, check to see if the mod date is greater than the last time we fetched.
                # If it is, fetch the bcids of the samples in the expedition.
                if self._date_start is None or expedition_modified_datetime.timestamp() > self._date_start.timestamp():
                    more_work = True
                    params = {
                        "limit": _page_size,
                        "page": 0,
                        "q": f"_expeditions_:{expedition_dict['expeditionCode']}",
                    }
                    while more_work:
                        expedition_records_url = f"{GEOME_API}records/Sample/json"
                        response = requests.get(
                            expedition_records_url, params=params, headers=headers, timeout=HTTP_TIMEOUT
                        )
                        if response.status_code != 200:
                            L.error(
                                "Unable to load project expeditions project:%s; expedition:%s, status: %s; reason: %s",
                                project_id,
                                expedition_dict["expeditionCode"],
                                response.status_code,
                                response.reason,
                            )
                            break
                        # L.debug("recordsInProject data: %s", response.text[:256])
                        expeditions_json = response.json()

                        for record in expeditions_json.get("content", {}).get(record_type, []):
                            L.debug("recordsInProject Record id: %s", record.get("bcid", None))
                            # When we yield here, yield both the record and the expedition modify date, since
                            # we'll compare the modify date of the expedition when we go to fetch again.
                            yield record, expedition_modified_datetime
                        if len(expeditions_json.get("content", {}).get(record_type, [])) < _page_size:
                            more_work = False
                        params["page"] = params["page"] + 1

    def _loadEntries(self):
        L = getLogger()
        if self._project_ids is None:
            # Load the project IDs
            # each entry is a dict with key
            self._project_ids = []
            for p in self.listProjects():
                pid = p.get("projectId", None)
                if pid is not None:
                    self._project_ids.append(
                        {
                            "project_id": pid,
                            "identifiers": [],
                            "loaded": False,
                        }
                    )
        self._cpage = []
        for p in self._project_ids:
            # return the next set of identifiers within a project
            if not p["loaded"]:
                for (record, modified_date) in self.recordsInProject(p["project_id"], self._record_type):
                    # record identifier
                    rid = record.get("bcid", None)
                    if rid is not None:
                        p["identifiers"].append((rid, modified_date))
                p["loaded"] = True
                L.info(
                    "Added %s identifiers from project %s",
                    len(p["identifiers"]),
                    p["project_id"],
                )
                # Make this the next page
                self._cpage = p["identifiers"]
                self._total_records += len(self._cpage)
                # if more than zero records on this page, then break to yield them
                if len(self._cpage) > 0:
                    L.debug(
                        "Breaking on project_id: %s, total_records: %s",
                        p["project_id"],
                        self._total_records,
                    )
                    break

    def __len__(self):
        return self._total_records

    def _getPage(self):
        if self._cpage is None or self._coffset >= self._total_records:
            self._page_offset = 0
            self._loadEntries()
            return


def getGEOMEItem_json(identifier, verify=False):
    headers = {"Accept": "application/json"}
    #
    # url = f"{GEOME_API}records/{urllib.parse.quote(identifier, safe='')}"
    url = f"{GEOME_API}records/{identifier}"
    params = {"includeChildren": "true", "includeParent": "true"}
    res = requests.get(
        url, headers=headers, params=params, verify=verify, timeout=HTTP_TIMEOUT
    )
    return res


class GEOMEItem(object):
    AUTHORITY_ID = "GEOME"
    RELATION_TYPE = {
        "Event": "has_event",
        "Tissue": "has_tissue",
        "Sample_Photo": "has_photo",
        "Diagnostics": "has_diagnostic",
        "UNDEFINED": "has_",
    }

    def __init__(self, identifier, source):
        self.identifier = identifier
        self.item = source

    def solrRelations(self):
        related = []
        _id = self.item.get("parent", {}).get("bcid", "")
        _typ = self.item.get("parent", {}).get("entity", "UNDEFINED")
        if _id != "":
            related.append(
                isb_lib.core.relationAsSolrDoc(
                    igsn_lib.time.dtnow(),
                    self.identifier,
                    self.identifier,
                    GEOMEItem.RELATION_TYPE[_typ],
                    _id,
                    "",
                )
            )
        for child in self.item.get("children", []):
            _id = child.get("bcid")
            if _id != "":
                _typ = child.get("entity", "UNDEFINED")
                related.append(
                    isb_lib.core.relationAsSolrDoc(
                        igsn_lib.time.dtnow(),
                        self.identifier,
                        self.identifier,
                        GEOMEItem.RELATION_TYPE[_typ],
                        _id,
                        "",
                    )
                )
        return related

    def getItemType(self):
        return self.item.get("record", {}).get("entity", None)

    def asThing(
        self,
        identifier: str,
        t_created: datetime.datetime,
        status: int,
        resolved_url: str,
        t_resolved: datetime.datetime,
        resolve_elapsed: float,
        media_type: str,
    ):
        L = getLogger()
        L.debug("GEOMEItem.asThing")
        if t_created is None:
            parent_record = self.item.get("parent", {})
            t_created = geomeEventRecordTimestamp(parent_record)
        _thing = Thing(
            id=identifier,
            tcreated=t_created,
            item_type=None,
            authority_id=GEOMEItem.AUTHORITY_ID,
            resolved_url=resolved_url,
            resolved_status=status,
            tresolved=t_resolved,
            resolve_elapsed=resolve_elapsed,
        )
        if not isinstance(self.item, dict):
            L.error("Item is not an object")
            return _thing
        _thing.item_type = self.getItemType()
        _thing.resolved_media_type = media_type
        _thing.resolve_elapsed = resolve_elapsed
        _thing.resolved_content = self.item
        _thing.geohash = GEOMETransformer.geohash_for_content(_thing.resolved_content)
        return _thing


def _validateResolvedContent(thing: Thing):
    isb_lib.core.validate_resolved_content(GEOMEItem.AUTHORITY_ID, thing)


def reparseRelations(thing):
    _validateResolvedContent(thing)
    item = GEOMEItem(thing.id, thing.resolved_content)
    return item.solrRelations()


def reparseThing(thing):
    """Reparse the resolved_content"""
    _validateResolvedContent(thing)
    item = GEOMEItem(thing.id, thing.resolved_content)
    thing.item_type = item.getItemType()
    thing.tstamp = igsn_lib.time.dtnow()
    return thing


def loadThing(identifier: str, t_created: datetime.datetime, existing_thing: Thing) -> Thing:
    L = getLogger()
    L.info("loadThing: %s", identifier)
    response = getGEOMEItem_json(identifier, verify=True)
    t_resolved = igsn_lib.time.dtnow()
    elapsed = igsn_lib.time.datetimeDeltaToSeconds(response.elapsed)
    for h in response.history:
        elapsed = igsn_lib.time.datetimeDeltaToSeconds(h.elapsed)
    r_url = response.url
    r_status = response.status_code
    media_type = response.headers["content-type"]
    obj = None
    try:
        obj = response.json()
    except Exception as e:
        L.warning(e)
    if existing_thing is None:
        item = GEOMEItem(identifier, obj)
        thing = item.asThing(identifier, t_created, r_status, r_url, t_resolved, elapsed, media_type)
    else:
        # Already have the row fetched from the db, set the fields on it since it's a SQLModel proxy
        thing = existing_thing
        thing.resolved_content = obj
        thing.resolved_url = r_url
        thing.resolved_status = r_status
        thing.tresolved = t_resolved
        thing.resolve_elapsed = elapsed
        thing.tcreated = t_created
    return thing


def reloadThing(thing):
    """Given an instance of thing, reload from the source and reparse."""
    L = getLogger()
    L.debug("reloadThing id=%s", thing.id)
    identifier = igsn_lib.normalize(thing.id)
    return loadThing(identifier, thing.tcreated)


def reparseAsCoreRecord(thing: Thing) -> typing.List[typing.Dict]:
    _validateResolvedContent(thing)
    core_records = []
    transformer = isamples_metadata.GEOMETransformer.GEOMETransformer(thing.resolved_content, thing.tcreated)
    parent_core_record = isb_lib.core.coreRecordAsSolrDoc(transformer)
    core_records.append(parent_core_record)
    for child_transfomer in transformer.child_transformers:
        child_core_record = isb_lib.core.coreRecordAsSolrDoc(child_transfomer)
        core_records.append(child_core_record)
    return core_records
