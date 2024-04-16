import json
import typing
import requests
import time
from isamples_metadata.solr_field_constants import SOLR_SOURCE, SOLR_DESCRIPTION, \
    SOLR_ISB_CORE_ID, SOLR_SEARCH_TEXT, SOLR_SOURCE_UPDATED_TIME, \
    SOLR_INDEX_UPDATED_TIME, SOLR_LABEL, SOLR_HAS_CONTEXT_CATEGORY, SOLR_HAS_CONTEXT_CATEGORY_CONFIDENCE, \
    SOLR_HAS_MATERIAL_CATEGORY, SOLR_HAS_MATERIAL_CATEGORY_CONFIDENCE, SOLR_HAS_SPECIMEN_CATEGORY, \
    SOLR_HAS_SPECIMEN_CATEGORY_CONFIDENCE, SOLR_KEYWORDS, SOLR_INFORMAL_CLASSIFICATION, SOLR_PRODUCED_BY_ISB_CORE_ID, \
    SOLR_PRODUCED_BY_LABEL, SOLR_PRODUCED_BY_DESCRIPTION, SOLR_PRODUCED_BY_DESCRIPTION_TEXT, \
    SOLR_PRODUCED_BY_FEATURE_OF_INTEREST, SOLR_PRODUCED_BY_RESPONSIBILITY, SOLR_PRODUCED_BY_RESULT_TIME, \
    SOLR_PRODUCED_BY_SAMPLING_SITE_DESCRIPTION, SOLR_PRODUCED_BY_SAMPLING_SITE_DESCRIPTION_TEXT, \
    SOLR_PRODUCED_BY_SAMPLING_SITE_LABEL, SOLR_PRODUCED_BY_SAMPLING_SITE_ELEVATION_IN_METERS, \
    SOLR_PRODUCED_BY_SAMPLING_SITE_PLACE_NAME, SOLR_REGISTRANT, SOLR_SAMPLING_PURPOSE, SOLR_CURATION_LABEL, \
    SOLR_CURATION_DESCRIPTION, SOLR_CURATION_DESCRIPTION_TEXT, SOLR_CURATION_ACCESS_CONSTRAINTS, SOLR_CURATION_LOCATION, \
    SOLR_CURATION_RESPONSIBILITY, SOLR_RELATED_RESOURCE_ISB_CORE_ID, SOLR_PRODUCED_BY_SAMPLING_SITE_LOCATION_LL, \
    SOLR_PRODUCED_BY_SAMPLING_SITE_LOCATION_BB, SOLR_PRODUCED_BY_SAMPLING_SITE_LOCATION_RPT, \
    SOLR_PRODUCED_BY_SAMPLING_SITE_LOCATION_LATITUDE, SOLR_PRODUCED_BY_SAMPLING_SITE_LOCATION_LONGITUDE, \
    SOLR_PRODUCED_BY_RESULT_TIME_RANGE, SOLR_COMPLIES_WITH, SOLR_AUTHORIZED_BY, SOLR_DESCRIPTION_TEXT

# Sleep for 10 seconds to allow for the Solr Docker container to start up.
time.sleep(10)

COLLECTION_NAME = "isb_core_records"

CREATE_COLLECTION_API = f"http://solr:8983/solr/admin/collections?action=CREATE&name={COLLECTION_NAME}&numShards=1&replicationFactor=1"
CONFIG_API = f"http://solr:8983/solr/{COLLECTION_NAME}/config"
SOLR_API = f"http://solr:8983/api/collections/{COLLECTION_NAME}/"
MEDIA_JSON = "application/json"


def pj(o):
    print(json.dumps(o, indent=2))


def listFields():
    headers = {"Accept": MEDIA_JSON}
    _schema = requests.get(f"{SOLR_API}schema", headers=headers).json()
    return _schema.get("schema", {}).get("fields")


def listFieldTypes():
    headers = {"Accept": MEDIA_JSON}
    _schema = requests.get(f"{SOLR_API}schema", headers=headers).json()
    return _schema.get("schema", {}).get("fieldTypes")


def createField(
    fname, ftype="string", stored=True, indexed=True, default=None, multivalued=False, docValues=False
):
    print(f"going to create field {fname}")
    headers = {"Content-Type": MEDIA_JSON}
    data = {
        "add-field": {
            "name": fname,
            "type": ftype,
            "stored": stored,
            "indexed": indexed,
        }
    }
    if multivalued:
        data["add-field"]["multiValued"] = multivalued
    if docValues:
        data["add-field"]["docValues"] = True
    if default is not None:
        data["add-field"]["default"] = default
    data = json.dumps(data).encode("utf-8")
    res = requests.post(f"{SOLR_API}schema", headers=headers, data=data)
    pj(res.json())


def deleteField(fname):
    headers = {"Content-Type": MEDIA_JSON}
    data = {
        "delete-field": {
            "name": fname,
        }
    }
    data = json.dumps(data).encode("utf-8")
    res = requests.post(f"{SOLR_API}schema", headers=headers, data=data)
    pj(res.json())


def createCopyField(source, dest, maxChars=None):
    headers = {"Content-Type": MEDIA_JSON}
    copyFieldData = {"source": source, "dest": [dest]}
    if maxChars is not None:
        copyFieldData["maxChars"] = maxChars
    data = {"add-copy-field": copyFieldData}
    data = json.dumps(data).encode("utf-8")
    res = requests.post(f"{SOLR_API}schema", headers=headers, data=data)
    pj(res.json())


def deleteCopyField(source, dest):
    headers = {"Content-Type": MEDIA_JSON}
    copyFieldData = {"source": source, "dest": [dest]}
    data = {"delete-copy-field": copyFieldData}
    data = json.dumps(data).encode("utf-8")
    res = requests.post(f"{SOLR_API}schema", headers=headers, data=data)
    pj(res.json())


def replaceFieldType(field_type_dict: typing.Dict):
    headers = {"Content-Type": MEDIA_JSON}
    data = {"replace-field-type": field_type_dict}
    encoded_data = json.dumps(data).encode("utf-8")
    res = requests.post(f"{SOLR_API}schema", headers=headers, data=encoded_data)
    pj(res.json())


def addFieldType(field_type_dict: typing.Dict):
    headers = {"Content-Type": MEDIA_JSON}
    data = {"add-field-type": field_type_dict}
    encoded_data = json.dumps(data).encode("utf-8")
    res = requests.post(f"{SOLR_API}schema", headers=headers, data=encoded_data)
    pj(res.json())


def addDynamicField(dynamic_field_dict: typing.Dict):
    headers = {"Content-Type": MEDIA_JSON}
    data = {"add-dynamic-field": dynamic_field_dict}
    encoded_data = json.dumps(data).encode("utf-8")
    res = requests.post(f"{SOLR_API}schema", headers=headers, data=encoded_data)
    pj(res.json())


def createCollection():
    print(f"Going to attempt to create collection {COLLECTION_NAME}")
    headers = {"Content-Type": MEDIA_JSON}
    res = requests.get(f"{CREATE_COLLECTION_API}", headers=headers)
    print("Response is: " + str(res))
    if res.status_code == 400:
        print("Collection already exists.  Exiting.")
        exit(0)
    pj(res.json())
    # Make sure to disable auto field creation, as this is known harmful behavior!
    data = {
        "set-user-property": {
            "update.autoCreateFields": "false"
        }
    }
    encoded_data = json.dumps(data).encode("utf-8")
    config_res = requests.post(CONFIG_API, headers=headers, data=encoded_data)
    pj(config_res.json())


print("Going to create collection in create_isb_core_schema")
#############
createCollection()
print("Going to create fields in create_isb_core_schema")
# Internal iSamples bookkeeping columns
createField(SOLR_ISB_CORE_ID, "string", True, True, None)
# Catch-all search field that all text should copy to
createField(SOLR_SEARCH_TEXT, "text_en", True, True, None, True)
createField(SOLR_SOURCE, "string", True, True, None)
createCopyField(SOLR_SOURCE, SOLR_SEARCH_TEXT)
# The time the record was last updated in the source db
createField(SOLR_SOURCE_UPDATED_TIME, "pdate", True, True, None)
# The time the record was last updated in the iSamples index
createField(SOLR_INDEX_UPDATED_TIME, "pdate", True, True, None)
#############


createField(SOLR_LABEL, "string", True, True, None)
createCopyField(SOLR_LABEL, SOLR_SEARCH_TEXT)
createField(SOLR_DESCRIPTION, "string", True, True, None)
createField(SOLR_DESCRIPTION_TEXT, "text_en", True, True, None)
createCopyField(SOLR_DESCRIPTION, "description_text")
createCopyField(SOLR_DESCRIPTION, SOLR_SEARCH_TEXT)
createField(SOLR_HAS_CONTEXT_CATEGORY, "string", True, True, None, True)
createField(SOLR_HAS_CONTEXT_CATEGORY_CONFIDENCE, "pfloat", True, True, None, True)
createField(SOLR_HAS_MATERIAL_CATEGORY, "string", True, True, None, True)
createField(SOLR_HAS_MATERIAL_CATEGORY_CONFIDENCE, "pfloat", True, True, None, True)
createField(SOLR_HAS_SPECIMEN_CATEGORY, "string", True, True, None, True)
createField(SOLR_HAS_SPECIMEN_CATEGORY_CONFIDENCE, "pfloat", True, True, None, True)
createField(SOLR_KEYWORDS, "string", True, True, None, True)
createCopyField(SOLR_KEYWORDS, SOLR_SEARCH_TEXT)
createField(SOLR_INFORMAL_CLASSIFICATION, "string", True, True, None, True)
createCopyField(SOLR_INFORMAL_CLASSIFICATION, SOLR_SEARCH_TEXT)
createField(SOLR_PRODUCED_BY_ISB_CORE_ID, "string", True, True, None)
createField(SOLR_PRODUCED_BY_LABEL, "string", True, True, None)
createCopyField(SOLR_PRODUCED_BY_LABEL, SOLR_SEARCH_TEXT)
createField(SOLR_PRODUCED_BY_DESCRIPTION, "string", True, True, None)
createField(SOLR_PRODUCED_BY_DESCRIPTION_TEXT, "text_en", True, True, None)
createCopyField(SOLR_PRODUCED_BY_DESCRIPTION, SOLR_PRODUCED_BY_DESCRIPTION_TEXT)
createCopyField(SOLR_PRODUCED_BY_DESCRIPTION, SOLR_SEARCH_TEXT)
createField(SOLR_PRODUCED_BY_FEATURE_OF_INTEREST, "string", True, True, None)
createCopyField(SOLR_PRODUCED_BY_FEATURE_OF_INTEREST, SOLR_SEARCH_TEXT)
createField(SOLR_PRODUCED_BY_RESPONSIBILITY, "string", True, True, None, True)
createCopyField(SOLR_PRODUCED_BY_RESPONSIBILITY, SOLR_SEARCH_TEXT)
createField(SOLR_PRODUCED_BY_RESULT_TIME, "pdate", True, True, None)
createField(SOLR_PRODUCED_BY_SAMPLING_SITE_DESCRIPTION, "string", True, True, None)
createCopyField(SOLR_PRODUCED_BY_SAMPLING_SITE_DESCRIPTION, SOLR_SEARCH_TEXT)
createField(
    SOLR_PRODUCED_BY_SAMPLING_SITE_DESCRIPTION_TEXT, "text_en", True, True, None
)
createCopyField(SOLR_PRODUCED_BY_SAMPLING_SITE_DESCRIPTION, SOLR_PRODUCED_BY_SAMPLING_SITE_DESCRIPTION_TEXT)
createField(SOLR_PRODUCED_BY_SAMPLING_SITE_LABEL, "string", True, True, None)
createCopyField(SOLR_PRODUCED_BY_SAMPLING_SITE_LABEL, SOLR_SEARCH_TEXT)
createField(SOLR_PRODUCED_BY_SAMPLING_SITE_ELEVATION_IN_METERS, "pfloat", True, True, None)
createField(SOLR_PRODUCED_BY_SAMPLING_SITE_PLACE_NAME, "string", True, True, None, True)
createCopyField(SOLR_PRODUCED_BY_SAMPLING_SITE_PLACE_NAME, SOLR_SEARCH_TEXT)
createField(SOLR_REGISTRANT, "string", True, True, None, True)
createCopyField(SOLR_REGISTRANT, SOLR_SEARCH_TEXT)
createField(SOLR_SAMPLING_PURPOSE, "string", True, True, None, True)
createCopyField(SOLR_SAMPLING_PURPOSE, SOLR_SEARCH_TEXT)
createField(SOLR_CURATION_LABEL, "string", True, True, None)
createCopyField(SOLR_CURATION_LABEL, SOLR_SEARCH_TEXT)
createField(SOLR_CURATION_DESCRIPTION, "string", True, True, None)
createField(SOLR_CURATION_DESCRIPTION_TEXT, "text_en", True, True, None)
createCopyField(SOLR_CURATION_DESCRIPTION, SOLR_CURATION_DESCRIPTION_TEXT)
createCopyField(SOLR_CURATION_DESCRIPTION, SOLR_SEARCH_TEXT)
createField(SOLR_CURATION_ACCESS_CONSTRAINTS, "string", True, True, None)
createField(SOLR_CURATION_LOCATION, "string", True, True, None)
createCopyField(SOLR_CURATION_LOCATION, SOLR_SEARCH_TEXT)
createField(SOLR_CURATION_RESPONSIBILITY, "string", True, True, None)
createField(SOLR_RELATED_RESOURCE_ISB_CORE_ID, "string", True, True, None, True)

replaceFieldType({
    "name": "location_rpt",
    "class": "solr.SpatialRecursivePrefixTreeFieldType",
    "geo": True,
    "omitNorms": True,
    "omitTermFreqAndPositions": True,
    "spatialContextFactory": "JTS",
    "termOffsets": False,
    "termPositions": False,
    "omitPositions": True,
    "autoIndex": True
})
addFieldType({
    "name": "bbox",
    "class": "solr.BBoxField",
    "geo": True,
    "numberType": "pdouble",
    "distanceUnits": "kilometers"
})
addDynamicField({
    "name": "*_ll",
    "type": "location",
    "indexed": True,
    "stored": True
})
addDynamicField({
    "name": "*_bb",
    "type": "bbox"
})
addDynamicField({
    "name": "*_rpt",
    "type": "location_rpt",
    "multiValued": True,
    "indexed": True,
    "stored": True
})

createField(SOLR_PRODUCED_BY_SAMPLING_SITE_LOCATION_LL, "location", True, True, None)
createField(SOLR_PRODUCED_BY_SAMPLING_SITE_LOCATION_BB, "bbox", True, True, None)
createField(SOLR_PRODUCED_BY_SAMPLING_SITE_LOCATION_RPT, "location_rpt", True, True, None)
createField(SOLR_PRODUCED_BY_SAMPLING_SITE_LOCATION_LATITUDE, "pfloat", True, True, None)
createField(SOLR_PRODUCED_BY_SAMPLING_SITE_LOCATION_LONGITUDE, "pfloat", True, True, None)

addFieldType({
    "name": "date_range",
    "class": "solr.DateRangeField",
    "omitNorms": True,
})
createField(SOLR_PRODUCED_BY_RESULT_TIME_RANGE, "date_range", True, True, None)

createField("producedBy_samplingSite_location_h3_0", "string", False, False, None, False, True)
createField("producedBy_samplingSite_location_h3_1", "string", False, False, None, False, True)
createField("producedBy_samplingSite_location_h3_2", "string", False, False, None, False, True)
createField("producedBy_samplingSite_location_h3_3", "string", False, False, None, False, True)
createField("producedBy_samplingSite_location_h3_4", "string", False, False, None, False, True)
createField("producedBy_samplingSite_location_h3_5", "string", False, False, None, False, True)
createField("producedBy_samplingSite_location_h3_6", "string", False, False, None, False, True)
createField("producedBy_samplingSite_location_h3_7", "string", False, False, None, False, True)
createField("producedBy_samplingSite_location_h3_8", "string", False, False, None, False, True)
createField("producedBy_samplingSite_location_h3_9", "string", False, False, None, False, True)
createField("producedBy_samplingSite_location_h3_10", "string", False, False, None, False, True)
createField("producedBy_samplingSite_location_h3_11", "string", False, False, None, False, True)
createField("producedBy_samplingSite_location_h3_12", "string", False, False, None, False, True)
createField("producedBy_samplingSite_location_h3_13", "string", False, False, None, False, True)
createField("producedBy_samplingSite_location_h3_14", "string", False, False, None, False, True)
createField("producedBy_samplingSite_location_h3_15", "string", False, False, None, False, True)
createField("producedBy_samplingSite_location_cesium_height", "pfloat", True, True, None)
# Nested document support
# Note that the solr docs indicate we need these fields, but they already existed in our schema, keeping here for
# reference purposes
# createField("_root_", "string", False, True)
# addFieldType({
#     "name": "_nest_path_",
#     "class": "solr.NestPathField",
# })
# createField("_nest_path_", "nest_path", True, True)
createField("_nest_parent_", "string", True, True)
createField("relation_target", "string", True, True)
createField("relation_type", "string", True, True)
# permit information fields
createField(SOLR_COMPLIES_WITH, "string", True, True, None, True)
createField(SOLR_AUTHORIZED_BY, "string", True, True, None, True)

pj(listFields())
