"""

"""

import logging
import datetime
import hashlib
import json
import typing
import isamples_metadata.Transformer

import igsn_lib.time
from isamples_metadata.Transformer import Transformer
import dateparser
import re

RECOGNIZED_DATE_FORMATS = [
    "%Y",  # e.g. 1985
    "%Y-%m-%d",  # e.g. 1947-08-06
    "%Y-%m",  # e.g. 2020-07
    "%Y-%m-%d %H:M:%S",  # e.g 2019-12-08 15:54:00
]
DATEPARSER_SETTINGS = {
    'DATE_ORDER': 'YMD',
    'PREFER_DAY_OF_MONTH': 'first',
    'TIMEZONE': 'UTC',
    'RETURN_AS_TIMEZONE_AWARE': True
}

SOLR_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
ELEVATION_PATTERN = re.compile(r"\s*(-?\d+\.?\d*)\s*m?", re.IGNORECASE)

def getLogger():
    return logging.getLogger("isb_lib.core")


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

def _shouldAddMetadataValueToSolrDoc(metadata: typing.Dict, key: typing.AnyStr) -> bool:
    shouldAdd = False
    if key in metadata:
        value = metadata[key]
        if key =='latitude':
            shouldAdd = -90.0 <= value <= 90.0
            if not shouldAdd:
                getLogger().error("Invalid latitude %f", value)
        elif key == 'longitude':
            shouldAdd = -180.0 <= value <= 180
            if not shouldAdd:
                getLogger().error("Invalid longitude %f", value)
        elif type(value) is list:
            shouldAdd = len(value) > 0
        elif type(value) is str:
            shouldAdd = len(value) > 0 and value != Transformer.NOT_PROVIDED
        else:
            shouldAdd = True
    return shouldAdd

def coreRecordAsSolrDoc(coreMetadata: typing.Dict) -> typing.Dict:
    """
    Args:
        coreMetadata: the iSamples Core metadata dictionary

    Returns: The coreMetadata in solr document format, suitable for posting to the solr JSON api
    (https://solr.apache.org/guide/8_1/json-request-api.html)
    """

    # Before preparing the document in solr format, strip out any whitespace in string values
    for k, v in coreMetadata.items():
        if type(v) is str:
            coreMetadata[k] = v.strip()

    doc = {
        "id": coreMetadata["sampleidentifier"],
        "isb_core_id": coreMetadata["@id"],
    }
    if _shouldAddMetadataValueToSolrDoc(coreMetadata, "label"):
        doc["label"] = coreMetadata["label"]
    if _shouldAddMetadataValueToSolrDoc(coreMetadata, "description"):
        doc["description"] = coreMetadata["description"]
    if _shouldAddMetadataValueToSolrDoc(coreMetadata, "hasContextCategory"):
        doc["hasContextCategory"] = coreMetadata["hasContextCategory"]
    if _shouldAddMetadataValueToSolrDoc(coreMetadata, "hasMaterialCategory"):
        doc["hasMaterialCategory"] = coreMetadata["hasMaterialCategory"]
    if _shouldAddMetadataValueToSolrDoc(coreMetadata, "hasSpecimenCategory"):
        doc["hasSpecimenCategory"] = coreMetadata["hasSpecimenCategory"]
    if _shouldAddMetadataValueToSolrDoc(coreMetadata, "keywords"):
        doc["keywords"] = coreMetadata["keywords"]
    if _shouldAddMetadataValueToSolrDoc(coreMetadata, "informalClassification"):
        doc["informalClassification"] = coreMetadata["informalClassification"]
    if _shouldAddMetadataValueToSolrDoc(coreMetadata, "registrant"):
        doc["registrant"] = coreMetadata["registrant"]
    if _shouldAddMetadataValueToSolrDoc(coreMetadata, "samplingPurpose"):
        doc["samplingPurpose"] = coreMetadata["samplingPurpose"]
    if "producedBy" in coreMetadata:
        handle_produced_by_fields(coreMetadata, doc)
    if "curation" in coreMetadata:
        handle_curation_fields(coreMetadata, doc)

    return doc

def handle_curation_fields(coreMetadata, doc):
    curation = coreMetadata["curation"]
    if _shouldAddMetadataValueToSolrDoc(curation, "label"):
        doc["curation_label"] = curation["label"]
    if _shouldAddMetadataValueToSolrDoc(curation, "description"):
        doc["curation_description"] = curation["description"]
    if _shouldAddMetadataValueToSolrDoc(curation, "accessConstraints"):
        doc["curation_accessConstraints"] = curation["accessConstraints"]
    if _shouldAddMetadataValueToSolrDoc(curation, "location"):
        doc["curation_location"] = curation["location"]
    if _shouldAddMetadataValueToSolrDoc(curation, "responsibility"):
        doc["curation_responsibility"] = curation["responsibility"]

def handle_produced_by_fields(coreMetadata, doc):
    # The solr index flattens subdictionaries, so check the keys explicitly in the subdictionary to see if they should be added to the index
    producedBy = coreMetadata["producedBy"]
    if _shouldAddMetadataValueToSolrDoc(producedBy, "label"):
        doc["producedBy_label"] = producedBy["label"]
    if _shouldAddMetadataValueToSolrDoc(producedBy, "description"):
        doc["producedBy_description"] = producedBy["description"]
    if _shouldAddMetadataValueToSolrDoc(producedBy, "responsibility"):
        doc["producedBy_responsibility"] = producedBy["responsibility"]
    if _shouldAddMetadataValueToSolrDoc(producedBy, "hasFeatureOfInterest"):
        doc["producedBy_hasFeatureOfInterest"] = producedBy["hasFeatureOfInterest"]
    if _shouldAddMetadataValueToSolrDoc(producedBy, "resultTime"):
        raw_date_str = producedBy["resultTime"]
        date_time = parsed_date(raw_date_str)
        if date_time is not None:
            doc["producedBy_resultTime"] = datetimeToSolrStr(date_time)
    if _shouldAddMetadataValueToSolrDoc(producedBy, "@id"):
        doc["producedBy_isb_core_id"] = producedBy["@id"]
    if "samplingSite" in producedBy:
        samplingSite = producedBy["samplingSite"]
        if _shouldAddMetadataValueToSolrDoc(samplingSite, "description"):
            doc["producedBy_samplingSite_description"] = samplingSite["description"]
        if _shouldAddMetadataValueToSolrDoc(samplingSite, "label"):
            doc["producedBy_samplingSite_label"] = samplingSite["label"]
        if _shouldAddMetadataValueToSolrDoc(samplingSite, "placeName"):
            doc["producedBy_samplingSite_placeName"] = samplingSite["placeName"]

        if "location" in samplingSite:
            location = samplingSite["location"]
            if _shouldAddMetadataValueToSolrDoc(location, "elevation"):
                location_str = location["elevation"]
                match = ELEVATION_PATTERN.match(location_str)
                if match is not None:
                    doc["producedBy_samplingSite_location_elevationInMeters"] = float(match.group(1))
            if _shouldAddMetadataValueToSolrDoc(location, "latitude") and _shouldAddMetadataValueToSolrDoc(location,
                                                                                                           "longitude"):
                doc["producedBy_samplingSite_location_latlon"] = f"{location['latitude']},{location['longitude']}"


def parsed_date(raw_date_str):
    # TODO: https://github.com/isamplesorg/isamples_inabox/issues/24
    date_time = dateparser.parse(raw_date_str, date_formats=RECOGNIZED_DATE_FORMATS, settings=DATEPARSER_SETTINGS)
    return date_time


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
    L = getLogger()
    headers = {"Content-Type":"application/json"}
    data = json.dumps(records).encode("utf-8")
    params = {"overwrite": "true"}
    _url = f"{url}update"
    L.debug("Going to post data %s to url %s", str(data), str(_url))
    res = rsession.post(_url, headers=headers, data=data, params=params)
    L.debug("post status: %s", res.status_code)
    L.debug("Solr update: %s", res.text)
    if res.status_code != 200:
        L.error(res.text)
        #TODO: something more elegant for error handling
        raise ValueError()
    else:
        L.debug("Successfully posted data %s to url %s", str(data), str(_url))

def solrCommit(rsession, url):
    L = getLogger()
    headers = {"Content-Type":"application/json"}
    params = {"commit":"true"}
    _url = f"{url}update"
    res = rsession.get(_url, headers=headers, params=params)
    L.debug("Solr commit: %s", res.text)


class IdentifierIterator:
    def __init__(
        self,
        offset: int = 0,
        max_entries: int = -1,
        date_start: datetime.datetime = None,
        date_end: datetime.datetime = None,
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
        except IndexError as e:
            raise StopIteration
        except KeyError as e:
            raise StopIteration
        except TypeError as e:
            raise StopIteration
        except ValueError as e:
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
