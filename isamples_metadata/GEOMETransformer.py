from __future__ import annotations
import datetime
import logging
import typing
import re
from typing import Optional
import requests
import isamples_metadata
from isamples_metadata.Transformer import (
    Transformer,
)

PERMIT_STRINGS_TO_IGNORE = ['nan', 'na', 'no data', 'unknown', 'none_required']

TISSUE_ENTITY = "Tissue"
JSON_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S%z"

complies_with_str = r"complies(?:_| )?(?:with)?:([^\s]+)(\s+authorized(?:_| )?(?:by)?:(.*))?"
PERMIT_STRUCTURED_TEXT_COMPLIES_WITH_PATTERN = re.compile(complies_with_str, re.IGNORECASE)

authorized_by_str = r"authorized(?:_| )?(?:by)?:([^\s]+)(\s+complies(?:_| )?(?:with)?:(.*))?"
PERMIT_STRUCTURED_TEXT_AUTHORIZED_BY_PATTERN = re.compile(authorized_by_str, re.IGNORECASE)


class GEOMETransformer(Transformer):
    """Concrete transformer class for going from a GEOME record to an iSamples record"""

    def __init__(
        self, source_record: typing.Dict, last_updated_time: Optional[datetime.datetime] = None
    ):
        super().__init__(source_record)
        self._child_transformers = []
        self._last_updated_time = last_updated_time
        children = self._get_children()
        for child_record in children:
            entity = child_record.get("entity")
            if entity == TISSUE_ENTITY:
                self._child_transformers.append(
                    GEOMEChildTransformer(
                        source_record, child_record, last_updated_time
                    )
                )

    ARK_PREFIX = "ark:/"

    def _source_record_main_record(self) -> typing.Dict:
        # The sub-record for the main record, as opposed to the parent or children records
        return self.source_record["record"]

    def _source_record_parent_record(self) -> typing.Dict:
        # The sub-record for the parent record
        return self.source_record["parent"]

    def _id_minus_prefix(self) -> str:
        return self._source_record_main_record()["bcid"].removeprefix(self.ARK_PREFIX)

    def id_string(self) -> str:
        return f"metadata/{self._id_minus_prefix()}"

    def sample_label(self) -> str:
        main_record = self._source_record_main_record()
        label_components = []
        scientific_name = main_record.get("scientificName")
        if scientific_name is not None:
            label_components.append(scientific_name)
        sample_id = main_record.get("materialSampleID")
        if sample_id is not None:
            label_components.append(sample_id)
        return " ".join(label_components)

    def sample_identifier_string(self) -> str:
        return self._source_record_main_record()["bcid"]

    def sample_description(self) -> str:
        description_pieces: list[str] = []
        main_record = self._source_record_main_record()

        self._transform_key_to_label(
            "minimumChronometricAgeReferenceSystem", main_record, description_pieces
        )
        self._transform_key_to_label(
            "maximumChronometricAgeReferenceSystem", main_record, description_pieces
        )
        self._transform_key_to_label(
            "verbatimAgeValue", main_record, description_pieces
        )
        self._transform_key_to_label("ageValue", main_record, description_pieces)
        self._transform_key_to_label("basisOfRecord", main_record, description_pieces)
        self._transform_key_to_label(
            "dynamicProperties", main_record, description_pieces
        )
        self._transform_key_to_label(
            "establishmentMeans", main_record, description_pieces
        )
        self._transform_key_to_label(
            "genbankSpecimenVoucher", main_record, description_pieces
        )
        self._transform_key_to_label(
            "identificationRemarks", main_record, description_pieces
        )
        self._transform_key_to_label(
            "identificationVerificationStatus", main_record, description_pieces
        )
        self._transform_key_to_label("individualCount", main_record, description_pieces)
        self._transform_key_to_label(
            "morphospeciesDescription", main_record, description_pieces
        )
        self._transform_key_to_label(
            "nomenclaturalCode", main_record, description_pieces
        )
        self._transform_key_to_label(
            "occurrenceRemarks", main_record, description_pieces
        )
        self._transform_key_to_label("organismID", main_record, description_pieces)
        self._transform_key_to_label(
            "organismQuantity", main_record, description_pieces
        )
        self._transform_key_to_label(
            "organismQuantityType", main_record, description_pieces
        )
        self._transform_key_to_label("organismRemarks", main_record, description_pieces)
        self._transform_key_to_label(
            "otherCatalogNumbers", main_record, description_pieces
        )
        self._transform_key_to_label(
            "previousIdentifications", main_record, description_pieces
        )
        self._transform_key_to_label("taxonRemarks", main_record, description_pieces)
        self._transform_key_to_label("typeStatus", main_record, description_pieces)
        self._transform_key_to_label(
            "verbatimLifeStage", main_record, description_pieces
        )
        self._transform_key_to_label("weight", main_record, description_pieces)
        self._transform_key_to_label("weightUnits", main_record, description_pieces)
        self._transform_key_to_label("lengthUnits", main_record, description_pieces)
        self._transform_key_to_label("length", main_record, description_pieces)
        self._transform_key_to_label("sex", main_record, description_pieces)

        return Transformer.DESCRIPTION_SEPARATOR.join(description_pieces)

    def has_context_categories(self) -> typing.List[str]:
        ranks = ["kingdom", "phylum", "genus"]
        ranks_to_value = {rank: None for rank in ranks}
        for rank in ranks:
            value = self._source_record_main_record().get(rank)
            if value != "unidentified":
                ranks_to_value[rank] = value
        url = "https://api.gbif.org/v1/species/match?"
        for rank, value in ranks_to_value.items():
            if value:
                url += rank + "=" + str(value) + "&"
        url = url.strip("&")
        # use GBIF api to get kingdom value
        r = requests.get(url)
        json = r.json()
        kingdom = json["kingdom"]
        if not kingdom:
            # remove parameters
            # from the most specific rank
            for rank in reversed(ranks):
                value = ranks_to_value[rank]
                if value:
                    query_param = rank + "=" + str(value) + "&"
                    url = url[:-len(query_param)]  # remove query param
                    r = requests.get(url)
                    json = r.json()
                    kingdom = json["kingdom"]
                    if kingdom:
                        break

        return kingdom

    def has_material_categories(self) -> typing.List[str]:
        # TODO: implement
        # ["'Organic material' unless record/entity, record/basisOfRecord, or record/collectionCode indicate otherwise"]
        return ["Organic material"]

    def has_specimen_categories(self) -> typing.List[str]:
        return ["Whole organism"]

    def informal_classification(self) -> typing.List[str]:
        main_record = self._source_record_main_record()
        scientific_name = main_record.get("scientificName")
        if scientific_name is None:
            informal_classification_pieces = []
            genus = main_record.get("genus")
            if genus is not None:
                informal_classification_pieces.append(genus)
            epithet = main_record.get("specificEpithet")
            if epithet is not None:
                informal_classification_pieces.append(epithet)
            return informal_classification_pieces
        else:
            return [scientific_name]

    def _place_names(self, only_general: bool) -> typing.List[str]:
        parent_record = self._source_record_parent_record()
        if parent_record is not None:
            place_names = []
            if not only_general:
                if "locality" in parent_record:
                    place_names.append(parent_record["locality"])
            if "county" in parent_record:
                place_names.append(parent_record["county"])
            if "stateProvince" in parent_record:
                place_names.append(parent_record["stateProvince"])
            if "island" in parent_record:
                place_names.append(parent_record["island"])
            if "islandGroup" in parent_record:
                place_names.append(parent_record["islandGroup"])
            if "country" in parent_record:
                place_names.append(parent_record["country"])
            if "continentOcean" in parent_record:
                place_names.append(parent_record["continentOcean"])
            return place_names
        return []

    def keywords(self) -> typing.List[str]:
        # "JSON array of values from record/ -order, -phylum, -family, -class, and parent/ -country, -county,
        # -stateProvince, -continentOcean... (place names more general that the locality or most specific
        # rank place name) "
        keywords = self._place_names(True)
        parent_record = self._source_record_parent_record()
        microhabitat = parent_record.get("microHabitat")
        if microhabitat is not None:
            keywords.append(microhabitat)
        order = self._source_record_main_record().get("order")
        if order is not None:
            keywords.append(order)
        phylum = self._source_record_main_record().get("phylum")
        if phylum is not None:
            keywords.append(phylum)
        family = self._source_record_main_record().get("family")
        if family is not None:
            keywords.append(family)
        classname = self._source_record_main_record().get("class")
        if classname is not None:
            keywords.append(classname)
        return keywords

    def produced_by_id_string(self) -> str:
        main_record = self._source_record_main_record()
        if main_record is not None:
            return main_record["bcid"]
        return Transformer.NOT_PROVIDED

    def produced_by_label(self) -> str:
        parent_record = self._source_record_parent_record()
        if parent_record is not None:
            label_pieces = []
            event_id = parent_record.get("eventID")
            if event_id is not None:
                label_pieces.append(event_id)
            expedition_code = parent_record.get("expeditionCode")
            if expedition_code is not None:
                label_pieces.append(expedition_code)
            return " ".join(label_pieces)
        return Transformer.NOT_PROVIDED

    def produced_by_description(self) -> str:
        parent_record = self._source_record_parent_record()
        if parent_record is not None:
            description_pieces = []
            event_remarks = parent_record.get("eventRemarks")
            if event_remarks is not None:
                description_pieces.append(event_remarks)
            self._transform_key_to_label(
                "samplingProtocol", parent_record, description_pieces
            )
            self._transform_key_to_label(
                "permitInformation", parent_record, description_pieces
            )
            self._transform_key_to_label(
                "expeditionCode", parent_record, description_pieces
            )
            self._transform_key_to_label(
                "taxTeam", parent_record, description_pieces, "taxonomy team"
            )
            self._transform_key_to_label("projectId", parent_record, description_pieces)
            return Transformer.DESCRIPTION_SEPARATOR.join(description_pieces)
        return Transformer.NOT_PROVIDED

    def produced_by_feature_of_interest(self) -> str:
        # TODO: implement
        # "[infer from specimen category, locality; need to so some unique values analysis]"
        parent_record = self._source_record_parent_record()
        if parent_record is not None:
            microhabitat = parent_record.get("microhabitat")
            if microhabitat is not None:
                return f"microhabitat: {microhabitat}"
        return Transformer.NOT_PROVIDED

    def produced_by_responsibilities(self) -> typing.List[str]:
        parent_record = self._source_record_parent_record()
        if parent_record is not None:
            responsibilities_pieces = []
            collector_list = parent_record.get("collectorList")
            if collector_list is not None:
                # Have to do some goofy checking here because this string-delimited field can either be a singleton
                # or have different delimiters
                if "," in collector_list:
                    for collector in collector_list.split(", "):
                        responsibilities_pieces.append(f"collector: {collector}")
                elif "|" in collector_list:
                    for collector in collector_list.split("|"):
                        responsibilities_pieces.append(f"collector: {collector}")
                else:
                    responsibilities_pieces.append(f"collector :{collector_list}")
            self._transform_key_to_label(
                "principalInvestigator", parent_record, responsibilities_pieces
            )
            self._transform_key_to_label(
                "identifiedBy", parent_record, responsibilities_pieces
            )
            self._transform_key_to_label(
                "taxTeam", parent_record, responsibilities_pieces, "taxonomy team"
            )
            self._transform_key_to_label(
                "eventEnteredBy",
                parent_record,
                responsibilities_pieces,
                "event registrant",
            )
            return responsibilities_pieces
        return []

    def produced_by_result_time(self) -> str:
        parent_record = self._source_record_parent_record()
        if parent_record is not None:
            return self._formatted_date(
                parent_record.get("yearCollected", ""),
                parent_record.get("monthCollected", ""),
                parent_record.get("dayCollected", ""),
            )
        return Transformer.NOT_PROVIDED

    def sampling_site_description(self) -> str:
        parent_record = self._source_record_parent_record()
        if parent_record is not None:
            habitat = parent_record.get("habitat")
            if habitat is not None:
                return habitat
            else:
                depth_to_bottom = parent_record.get("depthOfBottomInMeters")
                if depth_to_bottom is not None:
                    return f"Depth to bottom {depth_to_bottom} m"
        return Transformer.NOT_PROVIDED

    def sampling_site_label(self) -> str:
        parent_record = self._source_record_parent_record()
        if parent_record is not None:
            return parent_record.get("locality", Transformer.NOT_PROVIDED)
        return Transformer.NOT_PROVIDED

    def sampling_site_elevation(self) -> str:
        # Note that this is subject to revision based on the outcome of
        # https://github.com/isamplesorg/metadata/issues/35
        parent_record = self._source_record_parent_record()
        if parent_record is not None:
            depth = parent_record.get("maximumDepthInMeters")
            if depth is not None:
                return f"{depth} m"
        return Transformer.NOT_PROVIDED

    def sampling_site_latitude(self) -> typing.Optional[typing.SupportsFloat]:
        return _content_latitude(self.source_record)

    def sampling_site_longitude(self) -> typing.Optional[typing.SupportsFloat]:
        return _content_longitude(self.source_record)

    def sampling_site_place_names(self) -> typing.List:
        return self._place_names(False)

    def sample_registrant(self) -> str:
        return self._source_record_main_record().get(
            "sampleEnteredBy", Transformer.NOT_PROVIDED
        )

    def sample_sampling_purpose(self) -> str:
        # TODO: implement
        return Transformer.NOT_PROVIDED

    # region Curation

    def curation_label(self) -> str:
        return Transformer.NOT_PROVIDED

    def curation_description(self) -> str:
        curation_description_pieces: list[str] = []
        main_record = self._source_record_main_record()
        self._transform_key_to_label(
            "fixative", main_record, curation_description_pieces
        )
        self._transform_key_to_label(
            "preservative", main_record, curation_description_pieces
        )
        self._transform_key_to_label(
            "modifiedBy", main_record, curation_description_pieces, "record modifiedBy"
        )
        self._transform_key_to_label(
            "modifiedReason", main_record, curation_description_pieces, "modifiedReason"
        )

        sample_identified_pieces = []
        year_identified = main_record.get("yearIdentified")
        if year_identified is not None:
            sample_identified_pieces.append(year_identified)
        month_identified = main_record.get("monthIdentified")
        if month_identified is not None:
            sample_identified_pieces.append(month_identified.zfill(2))
        day_identified = main_record.get("dayIdentified")
        if day_identified is not None:
            sample_identified_pieces.append(day_identified.zfill(2))
        if len(sample_identified_pieces) > 0:
            curation_description_pieces.append(
                f"sample identified: {'-'.join(sample_identified_pieces)}"
            )
        if len(curation_description_pieces) > 0:
            return "; ".join(curation_description_pieces)
        return Transformer.NOT_PROVIDED

    def curation_access_constraints(self) -> str:
        return Transformer.NOT_PROVIDED

    def curation_location(self) -> str:
        return self._source_record_main_record().get(
            "institutionCode", Transformer.NOT_PROVIDED
        )

    def curation_responsibility(self) -> str:
        if "institutionCode" in self._source_record_main_record():
            institution_code = self._source_record_main_record()["institutionCode"]
            return f"curator:{institution_code}"
        return Transformer.NOT_PROVIDED

    # endregion

    def _get_children(self) -> typing.List[typing.Dict]:
        children = []
        if "children" in self.source_record:
            children = self.source_record["children"]
        return children

    def related_resources(self) -> typing.List[typing.Dict]:
        related_resources = []
        for child in self._get_children():
            child_resource = {}
            entity = child["entity"]
            if entity == TISSUE_ENTITY:
                child_resource["label"] = "subsample tissue"
                child_resource["relationship"] = "subsample"
                child_resource["target"] = child["bcid"]
                related_resources.append(child_resource)
        return related_resources

    @property
    def child_transformers(self) -> typing.List[GEOMEChildTransformer]:
        return self._child_transformers

    def last_updated_time(self) -> typing.Optional[str]:
        if self._last_updated_time is not None:
            return self._last_updated_time.strftime(JSON_TIME_FORMAT)
        else:
            return None

    def _parent_permit_information(self) -> Optional[str]:
        parent_record = self._source_record_parent_record()
        if parent_record is not None:
            return parent_record.get("permitInformation", Transformer.NOT_PROVIDED)

    def authorized_by(self) -> typing.List[str]:
        permit_information = self._parent_permit_information()
        if permit_information is not None:
            parsed_permit_information = GEOMETransformer.parse_permit_freetext(permit_information)
            return parsed_permit_information["authorizedBy"]
        return []

    def complies_with(self) -> typing.List[str]:
        # Don't have this information
        return []

    @staticmethod
    def _format_result_object(authorized_by: list[str]) -> dict[str, list[str]]:
        return {"authorizedBy": authorized_by, "compliesWith": []}

    @staticmethod
    def parse_permit_text(text: str) -> dict[str, list[str]]:
        structured_text = GEOMETransformer.parse_permit_structured_text(text)
        if len(structured_text) > 0:
            return structured_text
        else:
            return GEOMETransformer.parse_permit_freetext(text)

    @staticmethod
    def _split_delimited_text(text: str) -> list[str]:
        return re.split(";|,", text)

    @staticmethod
    def parse_permit_structured_text(text: str) -> dict[str, list[str]]:
        match = PERMIT_STRUCTURED_TEXT_AUTHORIZED_BY_PATTERN.match(text)
        result = {}
        authorized_by_str = None
        complies_with_str = None
        if match is not None:
            authorized_by_str = match.group(1)
            complies_with_str = match.group(3)
        else:
            match = PERMIT_STRUCTURED_TEXT_COMPLIES_WITH_PATTERN.match(text)
            if match is not None:
                complies_with_str = match.group(1)
                authorized_by_str = match.group(3)
        if authorized_by_str is not None:
            authorized_by_list = GEOMETransformer._split_delimited_text(authorized_by_str)
            result["authorizedBy"] = authorized_by_list
        if complies_with_str is not None:
            complies_with_list = GEOMETransformer._split_delimited_text(complies_with_str)
            result["compliesWith"] = complies_with_list
        return result

    @staticmethod
    def parse_permit_freetext(text: str) -> dict[str, list[str]]:
        original_string = str(text)

        # If the string is NA
        if original_string.lower() in PERMIT_STRINGS_TO_IGNORE:
            return GEOMETransformer._format_result_object([])

        # Remove quotes
        original_string = re.sub(r'\"', "", original_string)

        slash_n = len(re.findall(r'/', original_string))
        comma_n = len(re.findall(r'\,', original_string))

        # e.g. DAFF/DEA
        if slash_n == 1 and original_string.replace(" ", "") == original_string:
            return GEOMETransformer._format_result_object(original_string.split("/"))

        # If there are multiple slash "/", we need to split string by " and " or ", "
        if slash_n > 1 and not comma_n > 1:
            # replace typo error "/ " to "/"
            original_string = re.sub("/ ", "/", original_string)

            if re.findall(' and ', original_string):
                return GEOMETransformer._format_result_object(original_string.split(" and "))
            else:
                return GEOMETransformer._format_result_object(original_string.split(", "))

        # Split string by semicolon, ";" but ignore long string with multiple separator
        if re.findall("; ", original_string) and not comma_n > 1:
            return GEOMETransformer._format_result_object(original_string.split("; "))

        return GEOMETransformer._format_result_object([original_string])

    @staticmethod
    def geo_to_h3(content: typing.Dict, resolution: int = Transformer.DEFAULT_H3_RESOLUTION) -> typing.Optional[str]:
        return isamples_metadata.Transformer.geo_to_h3(_content_latitude(content), _content_longitude(content), resolution)

    def h3_function(self) -> typing.Callable:
        return GEOMETransformer.geo_to_h3


class GEOMEChildTransformer(GEOMETransformer):
    """GEOME child record subclass transformer -- uses some fields from the parent and some from the child"""

    def __init__(
        self,
        source_record: typing.Dict,
        child_record: typing.Dict,
        last_updated_time: Optional[datetime.datetime],
    ):
        self.source_record = source_record
        self.child_record = child_record
        self._last_updated_time = last_updated_time

    def _id_minus_prefix(self) -> str:
        return self.child_record["bcid"].removeprefix(self.ARK_PREFIX)

    def sample_label(self) -> str:
        return self.child_record["tissueID"]

    def sample_identifier_string(self) -> str:
        return self.child_record["bcid"]

    def sample_description(self) -> str:
        # TODO
        return ""

    def has_specimen_categories(self) -> typing.List[str]:
        return ["Organism part"]

    def produced_by_label(self) -> str:
        return f"tissue subsample from {self._source_record_main_record()['materialSampleID']}"

    def produced_by_description(self) -> str:
        description_pieces: list[str] = []
        self._transform_key_to_label(
            "tissueCatalogNumber", self.child_record, description_pieces
        )
        return Transformer.DESCRIPTION_SEPARATOR.join(description_pieces)

    def produced_by_feature_of_interest(self) -> str:
        return ""

    def produced_by_responsibilities(self) -> typing.List[str]:
        # TODO: who did the tissue extract, if available -- where does this live, if anywhere?
        return []

    def produced_by_result_time(self) -> str:
        # TODO: time the tissue extract was done, if available -- where does this live?
        return ""

    def sample_sampling_purpose(self) -> str:
        return "genomic analysis"

    def curation_location(self) -> str:
        curation_pieces = []
        tissue_well = self.child_record.get("tissueWell")
        if tissue_well is not None:
            curation_pieces.append(f"tissueWell: {tissue_well}")
        tissue_plate = self.child_record.get("tissuePlate")
        if tissue_plate is not None:
            curation_pieces.append(f"tissuePlate: {tissue_plate}")
        if len(curation_pieces) > 0:
            return ", ".join(curation_pieces)
        return Transformer.NOT_PROVIDED

    def related_resources(self) -> typing.List[typing.Dict]:
        parent_dict = {}
        main_record = self._source_record_main_record()
        parent_dict["label"] = f"parent sample {main_record.get('materialSampleID')}"
        parent_dict["target"] = main_record.get("bcid", "")
        parent_dict["relationshipType"] = "derived_from"
        return [parent_dict]

    def authorized_by(self) -> typing.List[str]:
        # If present, this information is stored on the parent record
        return []

    def complies_with(self) -> typing.List[str]:
        # If present, this information is stored on the parent record
        return []


# Function to iterate through the identifiers and instantiate the proper GEOME Transformer based on the identifier
# used for lookup
def geome_transformer_for_identifier(
    identifier: str, source_record: typing.Dict
) -> Optional[GEOMETransformer]:
    # Two possibilities:
    # (1) It's the sample, so instantiate the main one
    # (2) It's one of the children, so grab the child transformer
    transformer = GEOMETransformer(source_record, None)
    # Sample identifier string for GEOM is the ARK
    if identifier == transformer.sample_identifier_string():
        return transformer
    else:
        for child_transformer in transformer.child_transformers:
            if identifier == child_transformer.sample_identifier_string():
                return child_transformer
    logging.error(
        "Unable to find transformer for identifier %s in GEOME record %s",
        identifier,
        str(source_record),
    )
    return None


def _geo_location_float_value(content: typing.Dict, key: str) -> typing.Optional[float]:
    parent_record = content.get("parent")
    if parent_record is not None:
        geo_location_str = parent_record.get(key)
        if geo_location_str is not None:
            return float(geo_location_str)
    return None


def _content_latitude(content: typing.Dict) -> typing.Optional[float]:
    return _geo_location_float_value(content, "decimalLatitude")


def _content_longitude(content: typing.Dict) -> typing.Optional[float]:
    return _geo_location_float_value(content, "decimalLongitude")
