"""

"""
import logging
import datetime
import hashlib
import json
import traceback
import typing
import faulthandler
from signal import SIGINT

import igsn_lib.time
import term_store
from sqlmodel import Session

from isamples_metadata.metadata_constants import METADATA_SAMPLE_IDENTIFIER, METADATA_AT_ID, METADATA_LABEL, \
    METADATA_HAS_CONTEXT_CATEGORY, \
    METADATA_HAS_CONTEXT_CATEGORY_CONFIDENCE, METADATA_HAS_MATERIAL_CATEGORY, METADATA_HAS_MATERIAL_CATEGORY_CONFIDENCE, \
    METADATA_HAS_SAMPLE_OBJECT_TYPE, \
    METADATA_KEYWORDS, METADATA_PRODUCED_BY, METADATA_HAS_FEATURE_OF_INTEREST, METADATA_RESULT_TIME, \
    METADATA_SAMPLING_SITE, METADATA_ELEVATION, METADATA_LATITUDE, \
    METADATA_LONGITUDE, METADATA_PLACE_NAME, METADATA_SUBSAMPLE, METADATA_REGISTRANT, METADATA_SAMPLING_PURPOSE, \
    METADATA_CURATION, METADATA_ACCESS_CONSTRAINTS, METADATA_RESPONSIBILITY, \
    METADATA_RELATED_RESOURCE, METADATA_DESCRIPTION, METADATA_HAS_SAMPLE_OBJECT_TYPE_CONFIDENCE, \
    METADATA_CURATION_LOCATION, METADATA_SAMPLE_LOCATION, METADATA_KEYWORD, METADATA_NAME, \
    METADATA_ROLE, METADATA_IDENTIFIER, METADATA_LAST_MODIFIED_TIME
from isamples_metadata.metadata_exceptions import MetadataException
from isamples_metadata.solr_field_constants import SOLR_PRODUCED_BY_SAMPLING_SITE_ELEVATION_IN_METERS, \
    SOLR_CURATION_LABEL, SOLR_CURATION_DESCRIPTION, SOLR_CURATION_ACCESS_CONSTRAINTS, SOLR_CURATION_LOCATION, \
    SOLR_CURATION_RESPONSIBILITY, SOLR_SOURCE_UPDATED_TIME
from isamples_metadata.vocabularies import vocabulary_mapper
from isb_lib.models.thing import Thing
from isamples_metadata.Transformer import Transformer, geo_to_h3
import dateparser
from dateparser.date import DateDataParser
import re
import requests
import shapely.wkt
import shapely.geometry

from isb_lib.vocabulary import vocab_adapter
from isb_web import sqlmodel_database
from isb_web.sqlmodel_database import SQLModelDAO
from typing import Optional

from isb_web.vocabulary import SAMPLEDFEATURE_URI, MATERIAL_URI, MATERIALSAMPLEOBJECTTYPE_URI

RECOGNIZED_DATE_FORMATS = [
    "%Y",  # e.g. 1985
    "%Y-%m-%d",  # e.g. 1947-08-06
    "%Y-%m",  # e.g. 2020-07
    "%Y-%m-%d %H:M:%S",  # e.g 2019-12-08 15:54:00
]
DATEPARSER_SETTINGS = {
    "DATE_ORDER": "YMD",
    "PREFER_DAY_OF_MONTH": "first",
    "TIMEZONE": "UTC",
    "RETURN_AS_TIMEZONE_AWARE": True,
}
ddp = DateDataParser(languages=["en"], settings=DATEPARSER_SETTINGS)

SOLR_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
ELEVATION_PATTERN = re.compile(r"\s*(-?\d+\.?\d*)\s*m?", re.IGNORECASE)


MEDIA_JSON = "application/json"
MEDIA_NQUADS = "application/n-quads"
MEDIA_GEO_JSON = "application/geo+json"
MEDIA_JSONL = "application/jsonl"


def getLogger():
    return logging.getLogger("isb_lib.core")


LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "WARN": logging.WARNING,
    "ERROR": logging.ERROR,
    "FATAL": logging.CRITICAL,
    "CRITICAL": logging.CRITICAL,
}
LOG_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
LOG_FORMAT = "%(asctime)s %(name)s:%(levelname)s: %(message)s"


def initialize_logging(verbosity: str):
    logging.basicConfig(
        level=LOG_LEVELS.get(verbosity, logging.INFO),
        format=LOG_FORMAT,
        datefmt=LOG_DATE_FORMAT,
    )
    L = getLogger()
    verbosity = verbosity.upper()
    if verbosity not in LOG_LEVELS.keys():
        L.warning("%s is not a log level, set to INFO", verbosity)


def things_main(ctx, db_url, solr_url, verbosity="INFO"):
    ctx.ensure_object(dict)
    initialize_logging(verbosity)

    getLogger().info("Using database at: %s", db_url)
    ctx.obj["db_url"] = db_url
    getLogger().info("Using solr at: %s", solr_url)
    ctx.obj["solr_url"] = solr_url


def initialize_vocabularies(session: Session):
    repository = term_store.get_repository(session)
    vocab_adapter.uijson_vocabulary_dict(SAMPLEDFEATURE_URI, repository)
    vocab_adapter.uijson_vocabulary_dict(MATERIAL_URI, repository)
    vocab_adapter.uijson_vocabulary_dict(MATERIALSAMPLEOBJECTTYPE_URI, repository)
    vocabulary_mapper.sampled_feature_type()
    vocabulary_mapper.material_type()
    vocabulary_mapper.specimen_type()


def datetimeToSolrStr(dt):
    if dt is None:
        return None
    return dt.strftime(SOLR_TIME_FORMAT)


def relationAsSolrDoc(
    ts,
    source,
    s,
    p,
    o,
    name,
):
    doc = {
        "source": source,
        "s": s,
        "p": p,
        "o": o,
        "name": name,
    }
    doc["id"] = hashlib.md5(json.dumps(doc).encode("utf-8")).hexdigest()
    doc["tstamp"] = datetimeToSolrStr(ts)
    return doc


def validate_resolved_content(authority_id: str, thing: Thing) -> dict:
    if not isinstance(thing.resolved_content, dict):
        raise ValueError("Thing.resolved_content is not an object")
    if not thing.authority_id == authority_id:
        raise ValueError(f"Mismatched authority_id on Thing, expecting {authority_id}, received {thing.authority_id}")
    return thing.resolved_content


def _shouldAddMetadataValueToSolrDoc(metadata: typing.Dict, key: str) -> bool:
    shouldAdd = False
    value = metadata.get(key)
    if value is not None:
        if key == "latitude":
            # Explicitly disallow bools as they'll pass the logical test otherwise and solr will choke downstream
            shouldAdd = type(value) is not bool and -90.0 <= value <= 90.0
            if not shouldAdd:
                getLogger().error("Invalid latitude %f", value)
        elif key == "longitude":
            shouldAdd = type(value) is not bool and -180.0 <= value <= 180
            if not shouldAdd:
                getLogger().error("Invalid longitude %f", value)
        elif type(value) is list:
            shouldAdd = len(value) > 0
        elif type(value) is str:
            shouldAdd = len(value) > 0 and value != Transformer.NOT_PROVIDED
        else:
            shouldAdd = True
    return shouldAdd


def _gather_keyword_labels(keyword_dicts: list[dict]) -> list[str]:
    labels = []
    for keyword_dict in keyword_dicts:
        label = keyword_dict.get(METADATA_KEYWORD)
        if type(label) is dict:
            """ complex keywords look like:
                {
                  "keyword": {
                    "id": "https://vocab.getty.edu/aat/300247919",
                    "label": "fossils"
                  },
                  "keyword_uri": "",
                  "scheme_name": "Getty Art & Architecture Thesaurus"
                }
            """
            label = label.get(METADATA_LABEL)
        labels.append(label)
    return labels


def _gather_vocabulary_identifiers(vocabulary_dicts: list[dict]) -> list[str]:
    return [vocabulary_dict[METADATA_IDENTIFIER] for vocabulary_dict in vocabulary_dicts]


def _gather_registrant_name(registrant_dict: dict) -> str:
    return registrant_dict[METADATA_NAME]


def _coreRecordAsSolrDoc(coreMetadata: typing.Dict) -> typing.Dict:  # noqa: C901 -- need to examine computational complexity
    # Before preparing the document in solr format, strip out any whitespace in string values
    for k, v in coreMetadata.items():
        if type(v) is str:
            coreMetadata[k] = v.strip()

    doc = {
        "id": coreMetadata[METADATA_SAMPLE_IDENTIFIER],
        "isb_core_id": coreMetadata[METADATA_AT_ID],
        "indexUpdatedTime": datetimeToSolrStr(igsn_lib.time.dtnow())
    }
    if _shouldAddMetadataValueToSolrDoc(coreMetadata, METADATA_LAST_MODIFIED_TIME):
        doc[SOLR_SOURCE_UPDATED_TIME] = coreMetadata[METADATA_LAST_MODIFIED_TIME]
    if _shouldAddMetadataValueToSolrDoc(coreMetadata, METADATA_LABEL):
        doc["label"] = coreMetadata[METADATA_LABEL]
    if _shouldAddMetadataValueToSolrDoc(coreMetadata, METADATA_DESCRIPTION):
        doc["description"] = coreMetadata[METADATA_DESCRIPTION]
    if _shouldAddMetadataValueToSolrDoc(coreMetadata, METADATA_HAS_CONTEXT_CATEGORY):
        doc["hasContextCategory"] = _gather_vocabulary_identifiers(coreMetadata[METADATA_HAS_CONTEXT_CATEGORY])
    if _shouldAddMetadataValueToSolrDoc(coreMetadata, METADATA_HAS_CONTEXT_CATEGORY_CONFIDENCE):
        doc["hasContextCategoryConfidence"] = coreMetadata[METADATA_HAS_CONTEXT_CATEGORY_CONFIDENCE]
    if _shouldAddMetadataValueToSolrDoc(coreMetadata, METADATA_HAS_MATERIAL_CATEGORY):
        doc["hasMaterialCategory"] = _gather_vocabulary_identifiers(coreMetadata[METADATA_HAS_MATERIAL_CATEGORY])
    if _shouldAddMetadataValueToSolrDoc(coreMetadata, METADATA_HAS_MATERIAL_CATEGORY_CONFIDENCE):
        doc["hasMaterialCategoryConfidence"] = coreMetadata[METADATA_HAS_MATERIAL_CATEGORY_CONFIDENCE]
    if _shouldAddMetadataValueToSolrDoc(coreMetadata, METADATA_HAS_SAMPLE_OBJECT_TYPE):
        doc["hasSpecimenCategory"] = _gather_vocabulary_identifiers(coreMetadata[METADATA_HAS_SAMPLE_OBJECT_TYPE])
    if _shouldAddMetadataValueToSolrDoc(coreMetadata, METADATA_HAS_SAMPLE_OBJECT_TYPE_CONFIDENCE):
        doc["hasSpecimenCategoryConfidence"] = coreMetadata[METADATA_HAS_SAMPLE_OBJECT_TYPE_CONFIDENCE]
    if _shouldAddMetadataValueToSolrDoc(coreMetadata, METADATA_KEYWORDS):
        doc["keywords"] = _gather_keyword_labels(coreMetadata[METADATA_KEYWORDS])
    if _shouldAddMetadataValueToSolrDoc(coreMetadata, "informalClassification"):
        doc["informalClassification"] = coreMetadata["informalClassification"]
    if _shouldAddMetadataValueToSolrDoc(coreMetadata, METADATA_REGISTRANT):
        doc["registrant"] = _gather_registrant_name(coreMetadata[METADATA_REGISTRANT])
    if _shouldAddMetadataValueToSolrDoc(coreMetadata, METADATA_SAMPLING_PURPOSE):
        doc["samplingPurpose"] = coreMetadata[METADATA_SAMPLING_PURPOSE]
    if METADATA_PRODUCED_BY in coreMetadata:
        handle_produced_by_fields(coreMetadata, doc)
    if METADATA_CURATION in coreMetadata:
        handle_curation_fields(coreMetadata, doc)
    if METADATA_RELATED_RESOURCE in coreMetadata:
        handle_related_resources(coreMetadata, doc)

    return doc


def coreRecordAsSolrDoc(transformer: Transformer) -> typing.Dict:
    """
    Args:
        transformer: A Transformer instance containing the document to transform

    Returns: The coreMetadata in solr document format, suitable for posting to the solr JSON api
    (https://solr.apache.org/guide/8_1/json-request-api.html)
    """
    coreMetadata = transformer.transform()

    last_updated = transformer.last_updated_time()
    if last_updated is not None:
        date_time = parsed_date(last_updated)
        if date_time is not None:
            coreMetadata["sourceUpdatedTime"] = datetimeToSolrStr(date_time)
    return _coreRecordAsSolrDoc(coreMetadata)


def _gather_curation_responsibility(responsibility_dicts: list[str]) -> str:
    return [f"{responsibility_dict[METADATA_ROLE]}:{responsibility_dict[METADATA_NAME]}" for responsibility_dict in responsibility_dicts]


def handle_curation_fields(coreMetadata: typing.Dict, doc: typing.Dict):
    curation = coreMetadata[METADATA_CURATION]
    if _shouldAddMetadataValueToSolrDoc(curation, METADATA_LABEL):
        doc[SOLR_CURATION_LABEL] = curation[METADATA_LABEL]
    if _shouldAddMetadataValueToSolrDoc(curation, METADATA_DESCRIPTION):
        doc[SOLR_CURATION_DESCRIPTION] = curation[METADATA_DESCRIPTION]
    if _shouldAddMetadataValueToSolrDoc(curation, METADATA_ACCESS_CONSTRAINTS):
        doc[SOLR_CURATION_ACCESS_CONSTRAINTS] = curation[METADATA_ACCESS_CONSTRAINTS]
    if _shouldAddMetadataValueToSolrDoc(curation, METADATA_CURATION_LOCATION):
        doc[SOLR_CURATION_LOCATION] = curation[METADATA_CURATION_LOCATION]
    if _shouldAddMetadataValueToSolrDoc(curation, METADATA_RESPONSIBILITY):
        doc[SOLR_CURATION_RESPONSIBILITY] = _gather_curation_responsibility(curation[METADATA_RESPONSIBILITY])


def shapely_to_solr(shape: shapely.geometry.shape):
    centroid = shape.centroid
    bb = shape.bounds
    res = {
        "producedBy_samplingSite_location_ll": f"{centroid.y},{centroid.x}",
        "producedBy_samplingSite_location_bb": f"ENVELOPE({bb[0]}, {bb[2]}, {bb[3]}, {bb[1]})",
        "producedBy_samplingSite_location_rpt": shape.wkt
    }
    return res


def lat_lon_to_solr(coreMetadata: typing.Dict, latitude: typing.SupportsFloat, longitude: typing.SupportsFloat):
    coreMetadata.update(shapely_to_solr(shapely.geometry.Point(longitude, latitude)))
    coreMetadata["producedBy_samplingSite_location_latitude"] = latitude
    coreMetadata["producedBy_samplingSite_location_longitude"] = longitude
    for index in range(0, 16):
        h3_at_resolution = geo_to_h3(
            latitude,
            longitude,
            index,
        )
        field_name = f"producedBy_samplingSite_location_h3_{index}"
        coreMetadata[field_name] = h3_at_resolution


def _gather_produced_by_responsibilities(responsibility_dicts: list[dict]) -> list[str]:
    return [f"{responsibility_dict[METADATA_ROLE]}:{responsibility_dict[METADATA_NAME]}" for responsibility_dict in responsibility_dicts]


def handle_produced_by_fields(coreMetadata: typing.Dict, doc: typing.Dict):  # noqa: C901 -- need to examine computational complexity
    # The solr index flattens subdictionaries, so check the keys explicitly in the subdictionary to see if they should be added to the index
    producedBy = coreMetadata[METADATA_PRODUCED_BY]
    if _shouldAddMetadataValueToSolrDoc(producedBy, METADATA_LABEL):
        doc["producedBy_label"] = producedBy[METADATA_LABEL]
    if _shouldAddMetadataValueToSolrDoc(producedBy, METADATA_DESCRIPTION):
        doc["producedBy_description"] = producedBy[METADATA_DESCRIPTION]
    if _shouldAddMetadataValueToSolrDoc(producedBy, METADATA_RESPONSIBILITY):
        doc["producedBy_responsibility"] = _gather_produced_by_responsibilities(producedBy[METADATA_RESPONSIBILITY])
    if _shouldAddMetadataValueToSolrDoc(producedBy, METADATA_HAS_FEATURE_OF_INTEREST):
        doc["producedBy_hasFeatureOfInterest"] = producedBy[METADATA_HAS_FEATURE_OF_INTEREST]
    if _shouldAddMetadataValueToSolrDoc(producedBy, METADATA_RESULT_TIME):
        raw_date_str = producedBy[METADATA_RESULT_TIME]
        date_time = parsed_date(raw_date_str)
        if date_time is not None:
            solr_date_str = datetimeToSolrStr(date_time)
            doc["producedBy_resultTime"] = solr_date_str
            doc["producedBy_resultTimeRange"] = solr_date_str
    if _shouldAddMetadataValueToSolrDoc(producedBy, METADATA_AT_ID):
        produced_by_id = producedBy[METADATA_AT_ID]
        doc["producedBy_isb_core_id"] = produced_by_id
        doc["relations"] = [
            {
                "relation_target": produced_by_id,
                "relation_type": METADATA_SUBSAMPLE,
                "id": f"{doc['id']}_{METADATA_SUBSAMPLE}_{produced_by_id}"
            }
        ]
    if METADATA_SAMPLING_SITE in producedBy:
        samplingSite = producedBy[METADATA_SAMPLING_SITE]
        if _shouldAddMetadataValueToSolrDoc(samplingSite, METADATA_DESCRIPTION):
            doc["producedBy_samplingSite_description"] = samplingSite[METADATA_DESCRIPTION]
        if _shouldAddMetadataValueToSolrDoc(samplingSite, METADATA_LABEL):
            doc["producedBy_samplingSite_label"] = samplingSite[METADATA_LABEL]
        if _shouldAddMetadataValueToSolrDoc(samplingSite, METADATA_PLACE_NAME):
            doc["producedBy_samplingSite_placeName"] = samplingSite[METADATA_PLACE_NAME]

        if METADATA_SAMPLE_LOCATION in samplingSite:
            location = samplingSite[METADATA_SAMPLE_LOCATION]
            if _shouldAddMetadataValueToSolrDoc(location, METADATA_ELEVATION):
                elevation_value = location[METADATA_ELEVATION]
                if type(elevation_value) is str:
                    match = ELEVATION_PATTERN.match(elevation_value)
                    if match is not None:
                        doc[SOLR_PRODUCED_BY_SAMPLING_SITE_ELEVATION_IN_METERS] = float(match.group(1))
                elif type(elevation_value) is float:
                    doc[SOLR_PRODUCED_BY_SAMPLING_SITE_ELEVATION_IN_METERS] = elevation_value
            if _shouldAddMetadataValueToSolrDoc(
                location, METADATA_LATITUDE
            ) and _shouldAddMetadataValueToSolrDoc(location, METADATA_LONGITUDE):
                lat_lon_to_solr(doc, location[METADATA_LATITUDE], location[METADATA_LONGITUDE])


def handle_related_resources(coreMetadata: typing.Dict, doc: typing.Dict):
    related_resources = coreMetadata[METADATA_RELATED_RESOURCE]
    related_resource_ids = []
    for related_resource in related_resources:
        if type(related_resource) is dict:
            related_resource_ids.append(related_resource["target"])
        elif type(related_resource) is str:
            # if it's a string, just treat it as an id
            related_resource_ids.append(related_resource)
    doc["relatedResource_isb_core_id"] = related_resource_ids


def parsed_date(raw_date_str):
    # TODO: https://github.com/isamplesorg/isamples_inabox/issues/24
    date_data = ddp.get_date_data(raw_date_str, date_formats=RECOGNIZED_DATE_FORMATS)
    if date_data is not None:
        return date_data.date_obj
    else:
        return None


def parsed_datetime_from_isamples_format(raw_date_str) -> datetime.datetime:
    """dateparser was very slow on dates like this: 2006-03-22T12:00:00Z, so roll our own"""
    components = raw_date_str.split("T")
    date = components[0]
    time = components[1]
    # ts looks like this: 2018-03-27, shockingly dateparser.parse was very slow on these
    date_pieces = date.split("-")
    # chop off the TZ string
    time = time.replace("Z", "")
    time_pieces = time.split(":")
    lastmod_date = datetime.datetime(year=int(date_pieces[0]), month=int(date_pieces[1]), day=int(date_pieces[2]),
                                     hour=int(time_pieces[0]), minute=int(time_pieces[1]),
                                     second=int(float(time_pieces[2])), tzinfo=None)
    return lastmod_date


def solr_delete_records(rsession, ids_to_delete: typing.List[str], url):
    L = getLogger()
    headers = {"Content-Type": "application/json"}
    dicts_to_delete = []
    for id in ids_to_delete:
        dicts_to_delete.append({"id": id})
    params = {
        "delete": dicts_to_delete,
    }
    data = json.dumps(params).encode("utf-8")
    _url = f"{url}update?commit=true"
    res = rsession.post(_url, headers=headers, data=data)
    L.debug("post status: %s", res.status_code)
    L.debug("Solr update: %s", res.text)
    if res.status_code != 200:
        L.error(res.text)
        # TODO: something more elegant for error handling
        raise ValueError()
    else:
        L.debug("Successfully posted data %s to url %s", str(data), str(_url))


def solrAddRecords(rsession, records, url):
    """
    Push records to Solr.

    Existing records with the same id are overwritten with no consideration of version.

    Note that it Solr recommends no manual commits, instead rely on
    proper configuration of the core.

    Args:
        rsession: requests.Session
        relations: list of relations

    Returns: nothing

    """

    # Need to strip previously generated fields to avoid solr inconsistency errors
    for record in records:
        record.pop("_version_", None)
        record.pop("producedBy_samplingSite_location_bb__minY", None)
        record.pop("producedBy_samplingSite_location_bb__minX", None)
        record.pop("producedBy_samplingSite_location_bb__maxY", None)
        record.pop("producedBy_samplingSite_location_bb__maxX", None)

        # If we don't nuke all the copy fields, they'll end up copying over multiple times
        record.pop("searchText", None)
        record.pop("description_text", None)
        record.pop("producedBy_description_text", None)
        record.pop("producedBy_samplingSite_description_text", None)
        record.pop("curation_description_text", None)

    L = getLogger()
    headers = {"Content-Type": "application/json"}
    data = json.dumps(records).encode("utf-8")
    params = {"overwrite": "true"}
    _url = f"{url}update"
    L.debug("Going to post data %s to url %s", str(data), str(_url))
    res = rsession.post(_url, headers=headers, data=data, params=params)
    L.debug("post status: %s", res.status_code)
    L.debug("Solr update: %s", res.text)
    if res.status_code != 200:
        L.error(res.text)
        # TODO: something more elegant for error handling
        raise ValueError()
    else:
        L.debug("Successfully posted data %s to url %s", str(data), str(_url))


def solrCommit(rsession, url):
    L = getLogger()
    headers = {"Content-Type": "application/json"}
    params = {"commit": "true"}
    _url = f"{url}update"
    res = rsession.get(_url, headers=headers, params=params)
    L.debug("Solr commit: %s", res.text)


def solr_max_source_updated_time(
    url: str, authority_id: str, rsession=requests.session()
) -> typing.Optional[datetime.datetime]:
    headers = {"Content-Type": "application/json"}
    params = {
        "q": f"source:{authority_id}",
        "sort": "sourceUpdatedTime desc",
        "rows": 1,
    }
    _url = f"{url}select"
    res = rsession.get(_url, headers=headers, params=params)
    try:
        dict = res.json()
        docs = dict["response"]["docs"]
        if docs is not None and len(docs) > 0:
            return dateparser.parse(docs[0]["sourceUpdatedTime"])
    except Exception:
        getLogger().error("Didn't get expected JSON back from %s when fetching max source updated time for %s", _url, authority_id)

    return None


def sesar_fetch_lowercase_igsn_records(
    url: str, rows: int, rsession=requests.session()
) -> typing.List[typing.Dict]:
    headers = {"Content-Type": "application/json"}
    params = {
        "q": "source:SESAR AND id:*igsn*",
        "rows": rows,
    }
    _url = f"{url}select"
    res = rsession.get(_url, headers=headers, params=params)
    dict = res.json()
    docs = dict["response"]["docs"]
    return docs


def opencontext_fetch_broken_id_records(
    url: str, rows: int, rsession=requests.session()
) -> typing.List[typing.Dict]:
    headers = {"Content-Type": "application/json"}
    params = {
        "q": "source:OPENCONTEXT AND id:http*",
        "rows": rows,
    }
    _url = f"{url}select"
    res = rsession.get(_url, headers=headers, params=params)
    dict = res.json()
    docs = dict["response"]["docs"]
    return docs


class IdentifierIterator:
    def __init__(
        self,
        offset: int = 0,
        max_entries: int = -1,
        date_start: Optional[datetime.datetime] = None,
        date_end: Optional[datetime.datetime] = None,
        page_size: int = 100,
    ):
        self._start_offset = offset
        self._max_entries = max_entries
        self._date_start = date_start
        self._date_end = date_end
        self._page_size = page_size
        self._cpage = None
        self._coffset = self._start_offset
        self._page_offset = 0
        self._total_records = 0
        self._started = False

    def __len__(self):
        """
        Override if necessary to provide the length of the identifier list.

        Returns:
            integer
        """
        return self._total_records

    def _getPage(self):
        """Override this method to retrieve a page of entries from the service.

        After completion, self._cpage contains the next page of entries or None if there
        are no more pages available, and self._page_offset is set to the first entry (usually 0)

        This implementation generates a page of entries for testing purposes.
        """
        # Create at most 1000 records
        self._total_records = 1000
        if self._coffset >= self._total_records:
            self._cpage = None
            self._page_offset = 0
            return
        self._cpage = []
        for i in range(0, self._page_size):
            # create an entry tuple, (id, timestamp)
            entry = (
                self._coffset + i,
                igsn_lib.time.dtnow(),
            )
            self._cpage.append(entry)
        self._page_offset = 0

    def __iter__(self):
        return self

    def __next__(self):
        L = getLogger()
        # No more pages?
        if not self._started:
            L.debug("Not started, get first page")
            self._getPage()
            self._started = True
        if self._cpage is None:
            L.debug("_cpage is None, stopping.")
            raise StopIteration
        # L.debug("max_entries: %s; len(cpage): %s; page_offset: %s; coffset: %s", self._max_entries, len(self._cpage), self._page_offset, self._coffset)
        # Reached maximum requested entries?
        if self._max_entries > 0 and self._coffset >= self._max_entries:
            L.debug(
                "Over (%s) max entries (%s), stopping.",
                self._coffset,
                self._max_entries,
            )
            raise StopIteration
        # fetch a new page
        if self._page_offset >= len(self._cpage):
            # L.debug("Get page")
            self._getPage()
        try:
            entry = self._cpage[self._page_offset]
            self._page_offset += 1
            self._coffset += 1
            return entry
        except IndexError:
            raise StopIteration
        except KeyError:
            raise StopIteration
        except TypeError:
            raise StopIteration
        except ValueError:
            raise StopIteration


class CollectionAdaptor:
    def __init__(self):
        self._identifier_iterator = IdentifierIterator
        pass

    def listIdentifiers(self, **kwargs):
        return self._identifier_iterator(**kwargs)

    def getRecord(self, identifier, format=None, profile=None):
        return {"identifier": identifier}

    def listFormats(self, profile=None):
        return []

    def listProfiles(self, format=None):
        return []


class ThingRecordIterator:
    def __init__(
        self,
        session,
        authority_id: str,
        status: int = 200,
        page_size: int = 5000,
        offset: int = 0,
        limit: int = -1,
        min_time_created: Optional[datetime.datetime] = None,
    ):
        self._session = session
        self._authority_id = authority_id
        self._status = status
        self._page_size = page_size
        self._offset = offset
        self._min_time_created = min_time_created
        self._id = offset
        self._limit = limit
        self._total_selected = 0

    def yieldRecordsByPage(self):
        while True:
            n = 0
            things = sqlmodel_database.paged_things_with_ids(
                self._session,
                self._authority_id,
                self._status,
                self._page_size,
                self._offset,
                self._min_time_created,
                self._id,
            )
            max_id_in_page = 0
            for rec in things:
                n += 1
                self._total_selected += 1
                if self._limit is not None and self._total_selected == self._limit:
                    n = 0
                    break
                yield rec
                max_id_in_page = rec.primary_key
            if n == 0:
                break
            # Grab the next page, by only selecting records with _id > than the last one we fetched
            self._id = max_id_in_page


class CoreSolrImporter:
    def __init__(
        self,
        db_url: str,
        authority_id: str,
        db_batch_size: int,
        solr_batch_size: int,
        solr_url: str,
        offset: int = 0,
        min_time_created: Optional[datetime.datetime] = None,
    ):
        self._db_session = SQLModelDAO(db_url).get_session()
        self._authority_id = authority_id
        self._min_time_created = min_time_created
        self._thing_iterator = ThingRecordIterator(
            self._db_session,
            authority_id=self._authority_id,
            page_size=db_batch_size,
            offset=offset,
            min_time_created=min_time_created,
        )
        self._db_batch_size = db_batch_size
        self._solr_batch_size = solr_batch_size
        self._solr_url = solr_url

    def run_solr_import(  # noqa: C901
        self, core_record_function: typing.Callable
    ) -> typing.Set[str]:
        getLogger().info(
            "importing solr records with db batch size: %s, solr batch size: %s",
            self._db_batch_size,
            self._solr_batch_size,
        )
        faulthandler.enable()
        faulthandler.register(SIGINT)
        allkeys = set()
        rsession = requests.session()
        # h3_to_height = sqlmodel_database.h3_to_height(self._db_session)
        try:
            core_records = []
            for thing in self._thing_iterator.yieldRecordsByPage():
                try:
                    core_records_from_thing = core_record_function(thing)
                except MetadataException as e:
                    getLogger().info(f"Excluding record {thing.id} from index due to known exclusion: \"{e}\".")
                    continue
                except Exception as e:
                    traceback.print_exc()
                    getLogger().error("Failed trying to run transformer, skipping record %s exception %s",
                                      thing.resolved_content, e)
                    continue

                for core_record in core_records_from_thing:
                    core_record["source"] = self._authority_id
                    # Note that the h3 is precomputed and stored on the Thing itself because we do a
                    # "select distinct h3 from thing" query in order to determine which h3 values we need to compute
                    # Cesium elevation for.  The full order of operations is
                    # (1) compute h3 on things
                    # (2) select distinct h3 to determine points that need to be computed
                    # (3) compute points and insert into Point db cache table using Cesium JS API
                    # (4) at index time, consult Point cache to get elevation for thing, and since we've previously
                    #  computed the h3 just grab it off the Thing
                    # Step 3 in this sequence of events is both slow and API rate-limited by Cesium, so we take great
                    # pain to ensure that we're only querying the absolute minimum
                    core_record["producedBy_samplingSite_location_h3_15"] = thing.h3
                    # core_record["producedBy_samplingSite_location_cesium_height"] = h3_to_height.get(thing.h3)
                    if ("producedBy_samplingSite_location_cesium_height" in core_record):
                        core_record.pop("producedBy_samplingSite_location_cesium_height")
                    core_records.append(core_record)
                    allkeys.add(core_record["id"])
                batch_size = len(core_records)
                if batch_size > self._solr_batch_size:
                    solrAddRecords(
                        rsession,
                        core_records,
                        url=self._solr_url,
                    )
                    getLogger().info(
                        "Just added solr records, length of all keys is %d",
                        len(allkeys),
                    )
                    core_records = []
                elif batch_size % 1000 == 0:
                    logging.info(f"have done {batch_size}, current time is {datetime.datetime.now()}")
            if len(core_records) > 0:
                solrAddRecords(
                    rsession,
                    core_records,
                    url=self._solr_url,
                )
            solrCommit(rsession, url=self._solr_url)
            # verify records
            # for verifying that all records were added to solr
            # found = 0
            # for _id in allkeys:
            #    res = rsession.get(f"http://localhost:8983/solr/isb_rel/get?id={_id}").json()
            #    if res.get("doc",{}).get("id") == _id:
            #        found = found +1
            #    else:
            #        print(f"Missed: {_id}")
            # print(f"Found = {found}")
        finally:
            self._db_session.close()
        return allkeys
