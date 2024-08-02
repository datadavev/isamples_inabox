import datetime
import json
import logging
import math
import os
from abc import ABC, abstractmethod
from collections import OrderedDict
from enum import Enum
from typing import Optional

import petl
from petl import Table

import isb_web
from isamples_metadata.metadata_constants import METADATA_PLACE_NAME, METADATA_AUTHORIZED_BY, METADATA_COMPLIES_WITH, \
    METADATA_LONGITUDE, METADATA_LATITUDE, METADATA_RELATED_RESOURCE, \
    METADATA_CURATION_LOCATION, METADATA_ACCESS_CONSTRAINTS, METADATA_CURATION, METADATA_SAMPLING_PURPOSE, \
    METADATA_REGISTRANT, METADATA_SAMPLE_LOCATION, METADATA_ELEVATION, METADATA_SAMPLING_SITE, \
    METADATA_RESULT_TIME, METADATA_HAS_FEATURE_OF_INTEREST, METADATA_DESCRIPTION, METADATA_INFORMAL_CLASSIFICATION, \
    METADATA_KEYWORDS, METADATA_HAS_SPECIMEN_CATEGORY, METADATA_HAS_MATERIAL_CATEGORY, METADATA_HAS_CONTEXT_CATEGORY, \
    METADATA_LABEL, METADATA_SAMPLE_IDENTIFIER, METADATA_RESPONSIBILITY, METADATA_PRODUCED_BY, \
    METADATA_NAME, METADATA_KEYWORD, METADATA_IDENTIFIER, METADATA_ROLE, METADATA_AT_ID, METADATA_TARGET
from isamples_metadata.solr_field_constants import SOLR_PRODUCED_BY_SAMPLING_SITE_PLACE_NAME, SOLR_AUTHORIZED_BY, \
    SOLR_COMPLIES_WITH, SOLR_PRODUCED_BY_SAMPLING_SITE_LOCATION_LONGITUDE, \
    SOLR_PRODUCED_BY_SAMPLING_SITE_LOCATION_LATITUDE, SOLR_RELATED_RESOURCE_ISB_CORE_ID, SOLR_CURATION_RESPONSIBILITY, \
    SOLR_CURATION_LOCATION, SOLR_CURATION_ACCESS_CONSTRAINTS, SOLR_CURATION_DESCRIPTION, SOLR_CURATION_LABEL, \
    SOLR_SAMPLING_PURPOSE, SOLR_REGISTRANT, SOLR_PRODUCED_BY_SAMPLING_SITE_ELEVATION_IN_METERS, \
    SOLR_PRODUCED_BY_SAMPLING_SITE_LABEL, SOLR_PRODUCED_BY_SAMPLING_SITE_DESCRIPTION, SOLR_ID, \
    SOLR_PRODUCED_BY_RESULT_TIME, SOLR_PRODUCED_BY_RESPONSIBILITY, SOLR_PRODUCED_BY_FEATURE_OF_INTEREST, \
    SOLR_PRODUCED_BY_DESCRIPTION, SOLR_PRODUCED_BY_LABEL, SOLR_PRODUCED_BY_ISB_CORE_ID, SOLR_INFORMAL_CLASSIFICATION, \
    SOLR_KEYWORDS, SOLR_HAS_SPECIMEN_CATEGORY, SOLR_HAS_MATERIAL_CATEGORY, SOLR_HAS_CONTEXT_CATEGORY, SOLR_DESCRIPTION, \
    SOLR_LABEL, SOLR_SOURCE, SOLR_TIME_FORMAT, SOLR_ISB_CORE_ID


class ExportTransformException(Exception):
    """Exception subclass for when an error occurs during export transform"""


class TargetExportFormat(Enum):
    """Valid target export formats"""
    CSV = "CSV"
    JSONL = "JSONL"

    # overridden to allow for case insensitivity in query parameter formatting
    @classmethod
    def _missing_(cls, value):
        value = value.upper()
        for member in cls:
            if member.value.upper() == value:
                return member
        return None


class AbstractExportTransformer(ABC):
    @staticmethod
    @abstractmethod
    def transform(table: Table, dest_path_no_extension: str, append: bool) -> list[str]:
        """Transform solr results into a target export format"""
        pass


class CSVExportTransformer(AbstractExportTransformer):
    @staticmethod
    def transform(table: Table, dest_path_no_extension: str, append: bool) -> list[str]:
        dest_path = f"{dest_path_no_extension}.csv"
        if append:
            petl.io.csv.appendcsv(table, dest_path)
        else:
            petl.io.csv.tocsv(table, dest_path)
        return [dest_path]


class JSONExportTransformer(AbstractExportTransformer):

    @staticmethod
    def filter_null_values(obj):
        """
        Recursively filter out null values from a dictionary.
        """
        if isinstance(obj, dict):
            return {k: JSONExportTransformer.filter_null_values(v) for k, v in obj.items() if v is not None}
        elif isinstance(obj, list):
            return [JSONExportTransformer.filter_null_values(elem) for elem in obj if elem is not None]
        else:
            return obj

    @staticmethod
    def transform(table: Table, dest_path_no_extension: str, append: bool, is_sitemap: bool = False, lines_per_file: int = -1) -> list[str]:
        if append:
            raise ValueError("JSON Export doesn't support appending")
        extension = "jsonl"
        if lines_per_file == -1:
            full_file_paths = [f"{dest_path_no_extension}.{extension}"]
        else:
            # Use table length - 1 here since the header row counts as a row in the table (odd, but true)
            num_files = math.ceil((table.len() - 1) / lines_per_file)
            full_file_paths = [os.path.join(dest_path_no_extension, f"sitemap-{current_file_number}.{extension}") for current_file_number in range(0, num_files)]
        dicts_view = petl.util.base.dicts(table)
        rows_generator = (row for row in dicts_view)
        file_path_to_last_id_in_file_paths: dict[str, str] = {}
        last_id_in_file_to_file_paths: dict[str, str] = {}
        for full_file_path in full_file_paths:
            rows_in_file = 0
            last_id_in_file = None
            with open(full_file_path, "w") as file:
                while lines_per_file == -1 or (rows_in_file) < lines_per_file:
                    rows_in_file += 1
                    try:
                        row = next(rows_generator)
                    except StopIteration:
                        break
                    json.dump(JSONExportTransformer.filter_null_values(row), file)
                    last_id_in_file = row.get(METADATA_SAMPLE_IDENTIFIER)
                    file.write("\n")
            if last_id_in_file is not None:
                file_path_to_last_id_in_file_paths[full_file_path] = last_id_in_file
                last_id_in_file_to_file_paths[last_id_in_file] = full_file_path
        if is_sitemap:
            JSONExportTransformer._update_mod_dates_for_sitemap(file_path_to_last_id_in_file_paths,
                                                                last_id_in_file_to_file_paths)
        return full_file_paths

    @staticmethod
    def _update_mod_dates_for_sitemap(file_path_to_last_id_in_file_paths, last_id_in_file_to_file_paths):
        logging.info(f"Going to fetch solr mod dates for {file_path_to_last_id_in_file_paths}")
        last_mod_date_for_ids = isb_web.isb_solr_query.solr_last_mod_date_for_ids(file_path_to_last_id_in_file_paths.values())
        logging.info(f"Received solr mod dates {last_mod_date_for_ids}")
        for id, last_mod_date in last_mod_date_for_ids.items():
            # For sitemap generation we set the mod date of the file to be the solr index updated time of the
            # last record in the file.  This lets the sitemap index properly emit a last mod date on the file.
            date_object = datetime.datetime.strptime(last_mod_date, SOLR_TIME_FORMAT)
            # Convert the datetime to seconds since the epoch
            new_modification_time = date_object.timestamp()
            full_file_path = last_id_in_file_to_file_paths.get(id)
            os.utime(full_file_path, (new_modification_time, new_modification_time))


class SolrResultTransformer:
    def __init__(self, table: Table, format: TargetExportFormat, result_uuid: str, append: bool,
                 is_sitemap: bool = False, lines_per_file: int = -1):
        self._table = table
        self._format = format
        self._result_uuid = result_uuid
        self._append = append
        self._lines_per_file = lines_per_file
        self._is_sitemap = is_sitemap

    def _add_to_dict(self, target_dict: dict, target_key: str, source_dict: dict, source_key: str, default_value: str = ""):
        source_value = source_dict.get(source_key, default_value)
        if source_value is not None:
            target_dict[target_key] = source_value

    def _add_responsibilities_to_container(self,
                                           rec: dict,
                                           responsibility_key_solr: str,
                                           responsibility_key: str,
                                           container: dict,
                                           default_role: Optional[str] = None):
        responsibilities = rec.get(responsibility_key_solr, [])
        responsibility_dicts = []
        for responsibility in responsibilities:
            if ":" in responsibility:
                pieces = responsibility.split(":")
                responsibility_dicts.append({METADATA_ROLE: pieces[0], METADATA_NAME: pieces[1]})
            else:
                responsibility_dicts.append({METADATA_ROLE: default_role, METADATA_NAME: responsibility})
        if len(responsibility_dicts) > 0:
            container[responsibility_key] = responsibility_dicts

    def _curation_dict(self, rec: dict) -> dict:
        curation_dict: dict = {}
        self._add_to_dict(curation_dict, METADATA_LABEL, rec, SOLR_CURATION_LABEL)
        self._add_to_dict(curation_dict, METADATA_DESCRIPTION, rec, SOLR_CURATION_DESCRIPTION)
        self._add_to_dict(curation_dict, METADATA_CURATION_LOCATION, rec, SOLR_CURATION_LOCATION)
        self._add_responsibilities_to_container(rec, SOLR_CURATION_RESPONSIBILITY, METADATA_RESPONSIBILITY, curation_dict, "curator")
        access_constraints = rec.get(SOLR_CURATION_ACCESS_CONSTRAINTS, "").split("|")
        if len(access_constraints) > 0:
            curation_dict[METADATA_ACCESS_CONSTRAINTS] = access_constraints
        return curation_dict

    def _related_resource_dicts(self, rec: dict) -> Optional[list[dict]]:
        related_resource_ids = rec.get(SOLR_RELATED_RESOURCE_ISB_CORE_ID, [])
        if len(related_resource_ids) > 0:
            return [{METADATA_TARGET: related_resource_id} for related_resource_id in related_resource_ids]
        else:
            return None

    def _produced_by_dict(self, rec: dict) -> dict:
        produced_by_dict: dict = {}
        self._add_to_dict(produced_by_dict, METADATA_IDENTIFIER, rec, SOLR_PRODUCED_BY_ISB_CORE_ID)
        self._add_to_dict(produced_by_dict, METADATA_LABEL, rec, SOLR_PRODUCED_BY_LABEL)
        self._add_to_dict(produced_by_dict, METADATA_DESCRIPTION, rec, SOLR_PRODUCED_BY_DESCRIPTION)
        result_time = rec.get(SOLR_PRODUCED_BY_RESULT_TIME)
        if result_time is not None:
            result_time = result_time[:10]
            produced_by_dict[METADATA_RESULT_TIME] = result_time
        self._add_to_dict(produced_by_dict, METADATA_HAS_FEATURE_OF_INTEREST, rec, SOLR_PRODUCED_BY_FEATURE_OF_INTEREST)
        sampling_site_dict: dict = {}
        produced_by_dict[METADATA_SAMPLING_SITE] = sampling_site_dict
        self._add_to_dict(sampling_site_dict, METADATA_DESCRIPTION, rec, SOLR_PRODUCED_BY_SAMPLING_SITE_DESCRIPTION)
        self._add_to_dict(sampling_site_dict, METADATA_LABEL, rec, SOLR_PRODUCED_BY_SAMPLING_SITE_LABEL)
        self._add_to_dict(sampling_site_dict, METADATA_PLACE_NAME, rec, SOLR_PRODUCED_BY_SAMPLING_SITE_PLACE_NAME)
        sample_location_dict: dict = {}
        sampling_site_dict[METADATA_SAMPLE_LOCATION] = sample_location_dict
        self._add_to_dict(sample_location_dict, METADATA_ELEVATION, rec, SOLR_PRODUCED_BY_SAMPLING_SITE_ELEVATION_IN_METERS)
        self._add_to_dict(sample_location_dict, METADATA_LATITUDE, rec, SOLR_PRODUCED_BY_SAMPLING_SITE_LOCATION_LATITUDE)
        self._add_to_dict(sample_location_dict, METADATA_LONGITUDE, rec, SOLR_PRODUCED_BY_SAMPLING_SITE_LOCATION_LONGITUDE)
        self._add_responsibilities_to_container(rec, SOLR_PRODUCED_BY_RESPONSIBILITY, METADATA_RESPONSIBILITY, produced_by_dict)
        return produced_by_dict

    def _formatted_controlled_vocabulary(self, rec: dict, key: str) -> list[dict]:
        # The problem with the current vocabulary output is here.
        # TODO: maybe include the label if we have it?
        values = rec.get(key, [])
        return [{METADATA_IDENTIFIER: value} for value in values]

    def _has_specimen_categories(self, rec: dict) -> list:
        return self._formatted_controlled_vocabulary(rec, SOLR_HAS_SPECIMEN_CATEGORY)

    def _has_material_categories(self, rec: dict) -> list:
        return self._formatted_controlled_vocabulary(rec, SOLR_HAS_MATERIAL_CATEGORY)

    def _has_context_categories(self, rec: dict) -> list:
        return self._formatted_controlled_vocabulary(rec, SOLR_HAS_CONTEXT_CATEGORY)

    def _keywords(self, rec: dict) -> list:
        return [{METADATA_KEYWORD: keyword} for keyword in rec.get(SOLR_KEYWORDS, [])]

    def _registrant_dict(self, rec: dict) -> dict:
        return {METADATA_NAME: rec[SOLR_REGISTRANT][0]}

    def _rename_table_columns_csv(self):
        """Renames the solr columns to the public names in the public metadata schema, while maintaining CSV tabular format"""
        renaming_map = {
            SOLR_ID: METADATA_SAMPLE_IDENTIFIER,
            SOLR_AUTHORIZED_BY: METADATA_AUTHORIZED_BY,
            SOLR_COMPLIES_WITH: METADATA_COMPLIES_WITH,
            SOLR_PRODUCED_BY_SAMPLING_SITE_LOCATION_LONGITUDE: METADATA_LONGITUDE,
            SOLR_PRODUCED_BY_SAMPLING_SITE_LOCATION_LATITUDE: METADATA_LATITUDE,
            SOLR_RELATED_RESOURCE_ISB_CORE_ID: METADATA_RELATED_RESOURCE,
            SOLR_CURATION_RESPONSIBILITY: "curation_responsibility",  # Note that in the metadata this is just "responsibility", but this is a flat export format so we can't use that key by itself
            SOLR_CURATION_LOCATION: METADATA_CURATION_LOCATION,
            SOLR_CURATION_ACCESS_CONSTRAINTS: METADATA_ACCESS_CONSTRAINTS,
            SOLR_CURATION_DESCRIPTION: METADATA_CURATION,
            SOLR_CURATION_LABEL: "curation_label",  # Note that in the metadata this is just "label", but this is a flat export format so we can't use that key by itself
            SOLR_SAMPLING_PURPOSE: METADATA_SAMPLING_PURPOSE,
            SOLR_REGISTRANT: METADATA_REGISTRANT,
            SOLR_PRODUCED_BY_SAMPLING_SITE_PLACE_NAME: METADATA_PLACE_NAME,
            SOLR_PRODUCED_BY_SAMPLING_SITE_ELEVATION_IN_METERS: METADATA_ELEVATION,
            SOLR_PRODUCED_BY_SAMPLING_SITE_LABEL: METADATA_SAMPLE_LOCATION,
            SOLR_PRODUCED_BY_SAMPLING_SITE_DESCRIPTION: METADATA_SAMPLING_SITE,
            SOLR_PRODUCED_BY_RESULT_TIME: METADATA_RESULT_TIME,
            SOLR_PRODUCED_BY_RESPONSIBILITY: "produced_by_responsibility",  # Note that in the metadata this is just "responsibility", but this is a flat export format so we can't use that key by itself
            SOLR_PRODUCED_BY_FEATURE_OF_INTEREST: METADATA_HAS_FEATURE_OF_INTEREST,
            SOLR_PRODUCED_BY_DESCRIPTION: "produced_by_description",  # Note that in the metadata this is just "description", but this is a flat export format so we can't use that key by itself
            SOLR_PRODUCED_BY_LABEL: "produced_by_label",  # Note that in the metadata this is just "label", but this is a flat export format so we can't use that key by itself
            SOLR_PRODUCED_BY_ISB_CORE_ID: "produced_by_id",  # Note that in the metadata this is just "produced_by", but this is a flat export format so we can't use that key by itself
            SOLR_INFORMAL_CLASSIFICATION: METADATA_INFORMAL_CLASSIFICATION,
            SOLR_KEYWORDS: METADATA_KEYWORDS,
            SOLR_HAS_SPECIMEN_CATEGORY: METADATA_HAS_SPECIMEN_CATEGORY,
            SOLR_HAS_MATERIAL_CATEGORY: METADATA_HAS_MATERIAL_CATEGORY,
            SOLR_HAS_CONTEXT_CATEGORY: METADATA_HAS_CONTEXT_CATEGORY,
            SOLR_DESCRIPTION: METADATA_DESCRIPTION,
            SOLR_LABEL: METADATA_LABEL,
            SOLR_SOURCE: "source_collection",  # this isn't present in the exported metadata
        }
        self._table = petl.transform.headers.rename(self._table, renaming_map, strict=False)
        self._table = petl.rename(self._table, renaming_map, strict=False)

    def _rename_table_columns_jsonl(self):
        """Transforms the solr columns to structured json conforming to the public JSON metadata schema"""

        mappings = OrderedDict()
        mappings[METADATA_SAMPLE_IDENTIFIER] = SOLR_ID
        mappings[METADATA_AT_ID] = SOLR_ISB_CORE_ID
        mappings[METADATA_LABEL] = SOLR_LABEL
        mappings[METADATA_DESCRIPTION] = SOLR_DESCRIPTION
        mappings["source_collection"] = SOLR_SOURCE  # this isn't present in the exported metadata
        mappings[METADATA_HAS_SPECIMEN_CATEGORY] = self._has_specimen_categories
        mappings[METADATA_HAS_MATERIAL_CATEGORY] = self._has_material_categories
        mappings[METADATA_HAS_CONTEXT_CATEGORY] = self._has_context_categories
        mappings[METADATA_INFORMAL_CLASSIFICATION] = SOLR_INFORMAL_CLASSIFICATION
        mappings[METADATA_KEYWORDS] = self._keywords
        mappings[METADATA_PRODUCED_BY] = self._produced_by_dict
        mappings[METADATA_REGISTRANT] = self._registrant_dict
        mappings[METADATA_SAMPLING_PURPOSE] = SOLR_SAMPLING_PURPOSE
        mappings[METADATA_CURATION] = self._curation_dict
        mappings[METADATA_RELATED_RESOURCE] = self._related_resource_dicts
        mappings[METADATA_AUTHORIZED_BY] = SOLR_AUTHORIZED_BY
        mappings[METADATA_COMPLIES_WITH] = SOLR_COMPLIES_WITH
        self._table = petl.fieldmap(self._table, mappings)

    def transform(self) -> list[str]:
        """Transforms the table to the destination format.  Return value is the path the output file was written to."""
        if self._format == TargetExportFormat.CSV:
            self._rename_table_columns_csv()
            return CSVExportTransformer.transform(self._table, self._result_uuid, self._append)
        elif self._format == TargetExportFormat.JSONL:
            self._rename_table_columns_jsonl()
            return JSONExportTransformer.transform(self._table, self._result_uuid, self._append, self._is_sitemap, self._lines_per_file)
        else:
            raise ExportTransformException(f"Unsupported export format: {self._format}")
