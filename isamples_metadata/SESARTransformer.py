import typing
import logging
from typing import Callable

import igsn_lib
import isamples_metadata.Transformer
from isamples_metadata.Transformer import (
    Transformer,
    AbstractCategoryMapper,
    StringPairedCategoryMapper,
    StringOrderedCategoryMapper,
    StringEqualityCategoryMapper,
    StringEndsWithCategoryMapper,
    AbstractCategoryMetaMapper, Keyword,
)

from isamples_metadata.taxonomy.metadata_model_client import MODEL_SERVER_CLIENT, PredictionResult
from isamples_metadata.vocabularies import vocabulary_mapper
from isamples_metadata.vocabularies.vocabulary_mapper import VocabularyTerm  # noqa: F401


def fullIgsn(v):
    return f"IGSN:{igsn_lib.normalize(v)}"


class MaterialCategoryMetaMapper(AbstractCategoryMetaMapper):
    _endsWithRockMapper = StringEndsWithCategoryMapper("Rock", "rock", vocabulary_mapper.material_type)
    _endsWithMineralMapper = StringEndsWithCategoryMapper("Mineral", "mineral", vocabulary_mapper.material_type)
    _endsWithAqueousMapper = StringEndsWithCategoryMapper("aqueous", "liquidwater", vocabulary_mapper.material_type)
    _endsWithSedimentMapper = StringEndsWithCategoryMapper("Sediment", "sediment", vocabulary_mapper.material_type)
    _endsWithSoilMapper = StringEndsWithCategoryMapper("Soil", "soil", vocabulary_mapper.material_type)
    _endsWithParticulateMapper = StringEndsWithCategoryMapper(
        "Particulate", "particulate", vocabulary_mapper.material_type
    )
    _endsWithBiologyMapper = StringEndsWithCategoryMapper("Biology", "organicmaterial", vocabulary_mapper.material_type)
    _endsWithSyntheticMapper = StringEndsWithCategoryMapper(
        "Synthetic", "anyanthropogenicmaterial", vocabulary_mapper.material_type
    )
    _equalsRockMapper = StringEqualityCategoryMapper(
        [
            "Glass>Other",
            "Igneous>Other",
            "Igneous>Volcanic>Felsic>NotApplicable",
            "Igneous>Volcanic>Other",
            "Metamorphic>Other",
            "Sedimentary>Other",
            "Xenolithic>Other",
        ],
        "rock",
        vocabulary_mapper.material_type
    )
    _equalsSedimentMapper = StringEqualityCategoryMapper(["Tephra"], "sediment", vocabulary_mapper.material_type)
    _equalsOrganicMaterialMapper = StringEqualityCategoryMapper(
        ["Siderite>Mineral", "Macrobiology>Other", "Organic Material"],
        "organicmaterial",
        vocabulary_mapper.material_type
    )
    _equalsNonAqueousLiquidMaterialMapper = StringEqualityCategoryMapper(
        ["Liquid>organic"], "nonaqueousliquid", vocabulary_mapper.material_type
    )
    _equalsMineralMapper = StringEqualityCategoryMapper(
        [
            "Ore>Other",
            "FeldsparGroup>Other",
            "Epidote>Other",
            "Enstatite>Other",
            "Betpakdalite>Other",
            "Aurichalcite>Other",
            "Augite>Other",
            "Aragonite>Biology",
            "AmphiboleGroup>Other",
            "Actinolite>Other",
        ],
        "mineral",
        vocabulary_mapper.material_type
    )
    _equalsIceMapper = StringEqualityCategoryMapper(["Ice"], "anyice", vocabulary_mapper.material_type)
    _equalsGasMapper = StringEqualityCategoryMapper(["Gas"], "gas", vocabulary_mapper.material_type)
    _equalsBiogenicMapper = StringEqualityCategoryMapper(
        ["Macrobiology>Coral>Biology", "Coral>Biology"], "biogenicnonorganicmaterial", vocabulary_mapper.material_type
    )
    _equalsNaturalSolidMapper = StringEqualityCategoryMapper(
        ["Natural Solid Material"], "earthmaterial", vocabulary_mapper.material_type
    )
    _equalsMixedMapper = StringEqualityCategoryMapper(
        ["Mixed soil, sediment, rock"], "mixedsoilsedimentrock", vocabulary_mapper.material_type
    )
    _equalsMaterialMapper = StringEqualityCategoryMapper(
        ["Material"], "material", vocabulary_mapper.material_type
    )

    @classmethod
    def categories_mappers(cls) -> typing.List[AbstractCategoryMapper]:
        return [
            cls._endsWithRockMapper,
            cls._endsWithMineralMapper,
            cls._endsWithAqueousMapper,
            cls._endsWithSedimentMapper,
            cls._endsWithSoilMapper,
            cls._endsWithParticulateMapper,
            cls._endsWithBiologyMapper,
            cls._endsWithSyntheticMapper,
            cls._equalsRockMapper,
            cls._equalsSedimentMapper,
            cls._equalsIceMapper,
            cls._equalsOrganicMaterialMapper,
            cls._equalsNonAqueousLiquidMaterialMapper,
            cls._equalsMineralMapper,
            cls._equalsIceMapper,
            cls._equalsGasMapper,
            cls._equalsBiogenicMapper,
            cls._equalsNaturalSolidMapper,
            cls._equalsMixedMapper,
            cls._equalsMaterialMapper
        ]

    @classmethod
    def controlled_vocabulary_callable(cls) -> Callable:
        return vocabulary_mapper.material_type


class SpecimenCategoryMetaMapper(AbstractCategoryMetaMapper):
    _otherSolidObjectsMapper = StringEqualityCategoryMapper(
        [
            "Core",
            "Core Half Round",
            "Core Piece",
            "Core Quarter Round",
            "Core Section",
            "Core Section Half",
            "Core Sub-Piece",
            "Core Whole Round",
            "Grab",
            "Individual Sample",
            "Individual Sample>Cube",
            "Individual Sample>Cylinder",
            "Individual Sample>Slab",
            "Individual Sample>Specimen",
            "Oriented Core",
        ],
        "othersolidobject",
        vocabulary_mapper.specimen_type
    )
    _containersWithFluidMapper = StringEqualityCategoryMapper(
        [
            "CTD",
            "Individual Sample>Gas",
            "Individual Sample>Liquid",
        ],
        "fluidincontainer",
        vocabulary_mapper.specimen_type
    )
    _experimentalProductsMapper = StringEqualityCategoryMapper(
        ["Experimental Specimen"], "experimentalproduct", vocabulary_mapper.specimen_type
    )
    _biomeAggregationsMapper = StringEqualityCategoryMapper(
        ["Trawl"], "biomeaggregation", vocabulary_mapper.specimen_type
    )
    _analyticalPreparationsMapper = StringEqualityCategoryMapper(
        [
            "Individual Sample>Bead",
            "Individual Sample>Chemical Fraction",
            "Individual Sample>Culture",
            "Individual Sample>Mechanical Fraction",
            "Individual Sample>Powder",
            "Individual Sample>Smear",
            "Individual Sample>Thin Section",
            "Individual Sample>Toothpick",
            "Individual Sample>U-Channel",
            "Rock Powder",
        ],
        "analyticalpreparation",
        vocabulary_mapper.specimen_type
    )
    _aggregationsMapper = StringEqualityCategoryMapper(
        ["Cuttings", "Dredge"], "anyaggregation", vocabulary_mapper.specimen_type
    )

    @classmethod
    def categories_mappers(cls) -> typing.List[AbstractCategoryMapper]:
        return [
            cls._otherSolidObjectsMapper,
            cls._containersWithFluidMapper,
            cls._experimentalProductsMapper,
            cls._biomeAggregationsMapper,
            cls._analyticalPreparationsMapper,
            cls._aggregationsMapper,
        ]

    @classmethod
    def controlled_vocabulary_callable(cls) -> Callable:
        return vocabulary_mapper.specimen_type


class ContextCategoryMetaMapper(AbstractCategoryMetaMapper):
    _endsWithRockMapper = StringEndsWithCategoryMapper("Rock", "earthinterior", vocabulary_mapper.sampled_feature_type)
    _endsWithMineralMapper = StringEndsWithCategoryMapper("Mineral", "earthinterior", vocabulary_mapper.sampled_feature_type)
    _equalsGasMapper = StringEqualityCategoryMapper(
        ["Gas"], "subsurfacefluidreservoir", vocabulary_mapper.sampled_feature_type
    )
    # This one is actually incorrect as written, we need to use the combo of material and primaryLocationType
    _endsWithSoilMapper = StringEndsWithCategoryMapper(
        "Soil", "subaerialsurfaceenvironment", vocabulary_mapper.sampled_feature_type
    )
    _soilFloodplainMapper = StringPairedCategoryMapper(
        "Microbiology>Soil", "floodplain", "subaerialsurfaceenvironment", vocabulary_mapper.sampled_feature_type
    )
    _soilMapper = StringOrderedCategoryMapper(
        # Order matters here, the generic one needs to be last
        [_soilFloodplainMapper, _endsWithSoilMapper]
    )
    _seaSedimentMapper = StringPairedCategoryMapper(
        "Sediment", "sea", "marinewaterbodybottom", vocabulary_mapper.sampled_feature_type
    )
    _lakeSedimentMapper = StringPairedCategoryMapper(
        "Sediment", "lake", "lakeriverstreambottom", vocabulary_mapper.sampled_feature_type
    )
    _sedimentMapper = StringOrderedCategoryMapper(
        [_seaSedimentMapper, _lakeSedimentMapper]
    )
    _lakeMapper = StringPairedCategoryMapper("", "lake", "terrestrialwaterbody", vocabulary_mapper.sampled_feature_type)
    _mountainLiquidMapper = StringPairedCategoryMapper(
        "Liquid>aqueous", "Mountain", "terrestrialwaterbody", vocabulary_mapper.sampled_feature_type
    )
    _seaMapper = StringPairedCategoryMapper(
        "Liquid>aqueous", "Sea", "marinewaterbody", vocabulary_mapper.sampled_feature_type
    )
    _ventBiologyMapper = StringPairedCategoryMapper("Biology", "Vent", "marinewaterbody", vocabulary_mapper.sampled_feature_type)
    _ventLiquidMapper = StringPairedCategoryMapper(
        "Liquid>aqueous", "Vent", "subsurfacefluidreservoir", vocabulary_mapper.sampled_feature_type
    )
    _floodplainAquiferMapper = StringPairedCategoryMapper(
        "Liquid>aqueous", "floodplain\, aquifer", "subsurfacefluidreservoir", vocabulary_mapper.sampled_feature_type  # noqa: W605
    )
    _creekBankMapper = StringPairedCategoryMapper(
        "Sedimentary>GlacialAndOrPaleosol>Rock",
        "Creek bank",
        "subaerialsurfaceenvironment",
        vocabulary_mapper.sampled_feature_type
    )
    # Note that this represents the combos down to row 109 of
    # https://docs.google.com/spreadsheets/d/1QitBRkWH6YySZnNO-uR7D2rTaQ826WPT_xow9lPdJDM/edit#gid=1251732948
    # Need to continue on from there…

    @classmethod
    def categories_mappers(cls) -> typing.List[AbstractCategoryMapper]:
        return [
            cls._endsWithRockMapper,
            cls._endsWithMineralMapper,
            cls._equalsGasMapper,
            cls._soilMapper,
            cls._sedimentMapper,
            cls._lakeMapper,
            cls._mountainLiquidMapper,
            cls._seaMapper,
            cls._ventBiologyMapper,
            cls._ventLiquidMapper,
            cls._floodplainAquiferMapper,
            cls._creekBankMapper,
        ]

    @classmethod
    def controlled_vocabulary_callable(cls) -> Callable:
        return vocabulary_mapper.sampled_feature_type


class SESARTransformer(Transformer):
    """Concrete transformer class for going from a SESAR record to an iSamples record"""

    def __init__(self, source_record: typing.Dict):
        super().__init__(source_record)
        self._material_prediction_results: typing.Optional[list] = None

    def _source_record_description(self) -> typing.Dict:
        return self.source_record["description"]

    def _supplement_metadata(self) -> typing.Dict:
        description = self._source_record_description()
        if description is not None:
            return self._source_record_description()["supplementMetadata"]
        return {}

    def _primary_location_type(self) -> typing.Optional[str]:
        supplement_metadata = self._supplement_metadata()
        if (
            supplement_metadata is not None
            and "primaryLocationType" in supplement_metadata
        ):
            return supplement_metadata["primaryLocationType"]
        return None

    def _material_type(self) -> str:
        return self._source_record_description().get("material", None)

    @staticmethod
    def _logger():
        return logging.getLogger("isamples_metadata.SESARTransformer")

    def id_string(self):
        return "https://data.isamples.org/digitalsample/{0}/{1}".format(
            self.sample_identifier_scheme(), self.sample_identifier_value()
        )

    def sample_identifier_string(self) -> str:
        return f"{self.sample_identifier_scheme().upper()}:{self.sample_identifier_value()}"

    @staticmethod
    def sample_identifier_scheme() -> str:
        return "igsn"

    def sample_identifier_value(self) -> str:
        return self.source_record["igsn"]

    def sample_label(self) -> str:
        return self._source_record_description()["sampleName"]

    def sample_description(self) -> str:
        # TODO: implement
        return Transformer.NOT_PROVIDED

    def has_context_categories(self) -> list:
        material_type = self._material_type()
        primary_location_type = self._primary_location_type()
        return ContextCategoryMetaMapper.categories(
            material_type, primary_location_type
        )

    def _compute_material_prediction_results(self) -> typing.Optional[typing.List[PredictionResult]]:
        if self._material_type() is not None:
            # Have specified value, won't predict
            return None
        elif self._material_prediction_results is not None:
            # Have already computed, don't predict again
            return self._material_prediction_results
        else:
            self._material_prediction_results = MODEL_SERVER_CLIENT.make_sesar_material_request(self.source_record)
            return self._material_prediction_results

    # def has_material_categories(self) -> typing.List[str]:
    #     material = self._material_type()
    #     return MaterialCategoryMetaMapper.categories(material)
    #
    # def has_material_category_confidences(self, material_categories: list[str]) -> typing.Optional[typing.List[float]]:
    #     return None

    # Disabled pending resolution of https://github.com/isamplesorg/isamples_inabox/issues/255
    def has_material_categories(self) -> list:
        material = self._material_type()
        if not material:
            prediction_results = self._compute_material_prediction_results()
            material_cv = vocabulary_mapper.material_type()
            if prediction_results is not None:
                return [material_cv.term_for_label(prediction.value) for prediction in prediction_results]
            else:
                return []
        return MaterialCategoryMetaMapper.categories(material)

    def has_material_category_confidences(self, material_categories: list) -> typing.Optional[typing.List[float]]:
        prediction_results = self._compute_material_prediction_results()
        if prediction_results is None:
            return None
        else:
            return [prediction.confidence for prediction in prediction_results]

    def has_specimen_categories(self) -> list:
        sample_type = self._source_record_description()["sampleType"]
        return SpecimenCategoryMetaMapper.categories(sample_type)

    def keywords_impl(self) -> typing.List:
        sample_type = self._source_record_description()["sampleType"]
        if len(sample_type) > 0:
            return [Keyword(sample_type)]
        else:
            return []

    def produced_by_id_string(self) -> str:
        parent_id = self._source_record_description().get("parentIdentifier")
        if parent_id is not None:
            return fullIgsn(parent_id)
        return ""

    def _contributor_name_with_role(self, role_name: str):
        contributor_name = ""
        contributors = self._source_record_description()["contributors"]
        if contributors is not None and len(contributors) > 0:
            contributors_with_role = list(
                filter(
                    lambda contributor_dict: contributor_dict["roleName"] == role_name,
                    contributors,
                )
            )
            if len(contributors_with_role) > 0:
                contributor_name = contributors_with_role[0]["contributor"][0]["name"]
        return contributor_name

    def sample_registrant(self) -> str:
        return self._contributor_name_with_role("Sample Registrant")

    def sample_sampling_purpose(self) -> str:
        # TODO: implement
        return ""

    def produced_by_label(self) -> str:
        if "collectionMethod" in self._source_record_description():
            return self._source_record_description()["collectionMethod"]
        else:
            return Transformer.NOT_PROVIDED

    def produced_by_description(self) -> str:  # noqa: C901 -- need to examine computational complexity
        description_components = list()
        description_dict = self._source_record_description()
        if description_dict is not None:
            supplement_metadata = self._supplement_metadata()
            if supplement_metadata is not None:
                if "cruiseFieldPrgrm" in supplement_metadata:
                    description_components.append(
                        "cruiseFieldPrgrm:{0}".format(
                            supplement_metadata["cruiseFieldPrgrm"]
                        )
                    )
                if "launchPlatformName" in supplement_metadata:
                    description_components.append(
                        "launchPlatformName:{0}".format(
                            supplement_metadata["launchPlatformName"]
                        )
                    )

            if "collectionMethod" in description_dict:
                description_components.append(description_dict["collectionMethod"])
            if "description" in description_dict:
                description_components.append(description_dict["description"])

            if supplement_metadata is not None:
                launch_type_str = ""
                if "launchTypeName" in supplement_metadata:
                    launch_type_str += "launch type:{0}, ".format(
                        supplement_metadata["launchTypeName"]
                    )
                if "navigationType" in supplement_metadata:
                    launch_type_str += "navigation type:{0}".format(
                        supplement_metadata["navigationType"]
                    )
                if len(launch_type_str) > 0:
                    description_components.append(launch_type_str)

            return ". ".join(description_components)

        return Transformer.NOT_PROVIDED

    def produced_by_feature_of_interest(self) -> str:
        primary_location_type = self._primary_location_type()
        if primary_location_type is not None:
            return primary_location_type
        return Transformer.NOT_PROVIDED

    def produced_by_responsibilities(self) -> list:
        responsibilities = []
        description_dict = self._source_record_description()
        if "collector" in description_dict:
            collector = description_dict["collector"]
            if collector is not None:
                responsibilities.append(Transformer._responsibility_dict("Collector", collector))

        owner = self._contributor_name_with_role("Sample Owner")
        if len(owner) > 0:
            responsibilities.append(Transformer._responsibility_dict("Sample Owner", owner))

        return responsibilities

    def produced_by_result_time(self) -> str:
        result_time = Transformer.NOT_PROVIDED
        description = self._source_record_description()
        if "collectionStartDate" in description:
            result_time = description["collectionStartDate"]
        elif "log" in description:
            # try reading it out of the log
            result_time = description["log"][0]["timestamp"]
        return result_time

    def sampling_site_description(self) -> str:
        description_dict = self._source_record_description()
        if description_dict is not None:
            supplement_metadata = self._supplement_metadata()
            if (
                supplement_metadata is not None
                and "locationDescription" in supplement_metadata
            ):
                return supplement_metadata["locationDescription"]
        return Transformer.NOT_PROVIDED

    def sampling_site_label(self) -> str:
        # TODO: implement
        return Transformer.NOT_PROVIDED

    def elevation_str(
        self, elevation_value: str, elevation_unit: str
    ) -> str:
        elevation_unit_abbreviation = ""
        if elevation_unit is not None:
            elevation_unit = elevation_unit.lower().strip()
            if elevation_unit == "feet":
                # target elevation for core metadata will always be meters, so convert here
                elevation_value = str(float(elevation_value) / Transformer.FEET_PER_METER)
                elevation_unit_abbreviation = "m"
            elif elevation_unit == "meters":
                elevation_unit_abbreviation = "m"
            else:
                self._logger().error(
                    "Received elevation in unexpected unit: ", elevation_unit
                )
        elevation_str = str(elevation_value)
        if len(elevation_unit_abbreviation) > 0:
            elevation_str += " " + elevation_unit_abbreviation
        return elevation_str

    def sampling_site_elevation(self) -> str:
        supplement_metadata = self._supplement_metadata()
        if supplement_metadata is not None and "elevation" in supplement_metadata:
            elevation_value = supplement_metadata["elevation"]
            elevation_unit = supplement_metadata.get("elevationUnit", "meters")
            return self.elevation_str(elevation_value, elevation_unit)
        return Transformer.NOT_PROVIDED

    def sampling_site_latitude(self) -> typing.Optional[typing.SupportsFloat]:
        return _content_latitude(self.source_record)

    def sampling_site_longitude(self) -> typing.Optional[typing.SupportsFloat]:
        return _content_longitude(self.source_record)

    def sampling_site_place_names(self) -> typing.List:
        place_names = list()
        supplement_metadata = self._supplement_metadata()
        if "primaryLocationName" in supplement_metadata:
            primary_location_name = supplement_metadata["primaryLocationName"]
            place_names.extend(primary_location_name.split("; "))
        if "province" in supplement_metadata:
            place_names.append(supplement_metadata["province"])
        if "county" in supplement_metadata:
            place_names.append(supplement_metadata["county"])
        if "city" in supplement_metadata:
            place_names.append(supplement_metadata["city"])
        return place_names

    def informal_classification(self) -> typing.List[str]:
        """Not currently used for SESAR"""
        return [Transformer.NOT_PROVIDED]

    def last_updated_time(self) -> typing.Optional[str]:
        # Loop through the log and find the "lastUpdated" record
        description = self._source_record_description()
        log = description.get("log")
        if log is not None:
            for record in log:
                if "lastUpdated" == record.get("type"):
                    return record["timestamp"]
        return None

    def authorized_by(self) -> typing.List[str]:
        # Don't have this information
        return []

    def complies_with(self) -> typing.List[str]:
        # Don't have this information
        return []

    def h3_function(self) -> typing.Callable:
        return geo_to_h3


def _geo_location_float_value(source_record: typing.Dict, key_name: str) -> typing.Optional[float]:
    description = source_record.get("description")
    if description is not None:
        geo_location = None
        if "geoLocation" in description:
            geo_location = description["geoLocation"]
        elif "spatialCoverage" in description:
            geo_location = description["spatialCoverage"]
        if geo_location is not None:
            first_geo = geo_location["geo"][0]
            # Ignore things that aren't lat/long for now, e.g.
            # https://github.com/isamplesorg/metadata/issues/20
            if key_name in first_geo:
                string_val = first_geo[key_name]
                if string_val is not None:
                    return float(string_val)
    return None


def _content_latitude(source_record: typing.Dict) -> typing.Optional[float]:
    return _geo_location_float_value(source_record, "latitude")


def _content_longitude(source_record: typing.Dict) -> typing.Optional[float]:
    return _geo_location_float_value(source_record, "longitude")


def geo_to_h3(content: typing.Dict, resolution: int = Transformer.DEFAULT_H3_RESOLUTION) -> typing.Optional[str]:
    return isamples_metadata.Transformer.geo_to_h3(_content_latitude(content), _content_longitude(content), resolution)
