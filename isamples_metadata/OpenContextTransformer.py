import typing
from typing import Optional, Callable
import re

import isamples_metadata.Transformer
from isamples_metadata.Transformer import (
    Transformer,
    AbstractCategoryMetaMapper,
    StringEqualityCategoryMapper,
    AbstractCategoryMapper, Keyword,
)
from isamples_metadata.metadata_exceptions import MissingIdentifierException
from isamples_metadata.taxonomy.metadata_model_client import MODEL_SERVER_CLIENT, PredictionResult
from isamples_metadata.vocabularies import vocabulary_mapper
from isamples_metadata.vocabularies.vocabulary_mapper import VocabularyTerm

AAT_NAME = "Getty Art & Architecture Thesaurus"
GETTY_AAT_REGEX = re.compile("\[([^\]]+)\]")  # noqa: W605


class MaterialCategoryMetaMapper(AbstractCategoryMetaMapper):
    _anthropogenicMaterialMapper = StringEqualityCategoryMapper(
        [
            "Architectural Element",
            "Bulk Ceramic",
            "Glass",
        ],
        "anyanthropogenicmaterial",
        vocabulary_mapper.material_type
    )

    _anthropogenicMetalMapper = StringEqualityCategoryMapper(
        ["Coin"],
        "anthropogenicmetal",
        vocabulary_mapper.material_type
    )

    _biogenicMapper = StringEqualityCategoryMapper(
        ["Animal Bone", "Human Bone", "Non Diagnostic Bone", "Shell"],
        "biogenicnonorganicmaterial",
        vocabulary_mapper.material_type
    )

    _organicMapper = StringEqualityCategoryMapper(
        ["Plant remains"],
        "organicmaterial",
        vocabulary_mapper.material_type
    )

    _materialMapper = StringEqualityCategoryMapper(
        ["Biological record", "Biological subject, Ecofact"],
        "material",
        vocabulary_mapper.material_type
    )

    _rockMapper = StringEqualityCategoryMapper(
        ["Bulk Lithic", "Groundstone"],
        "rock",
        vocabulary_mapper.material_type
    )

    _naturalSolidMaterialMapper = StringEqualityCategoryMapper(
        ["Natural solid material"],
        "earthmaterial",
        vocabulary_mapper.material_type
    )

    _mineralMapper = StringEqualityCategoryMapper(
        ["Mineral"],
        "mineral",
        vocabulary_mapper.material_type
    )

    _notSampleMapper = StringEqualityCategoryMapper(
        [
            "Sample, Collection, or Aggregation",
            "Human Subject",
            "Reference Collection",
        ],
        "material",
        vocabulary_mapper.material_type
    )

    @classmethod
    def categories_mappers(cls) -> typing.List[AbstractCategoryMapper]:
        return [
            cls._anthropogenicMaterialMapper,
            cls._anthropogenicMetalMapper,
            cls._biogenicMapper,
            cls._organicMapper,
            cls._rockMapper,
            cls._naturalSolidMaterialMapper,
            cls._mineralMapper,
            cls._notSampleMapper
        ]

    @classmethod
    def controlled_vocabulary_callable(cls) -> Callable:
        return vocabulary_mapper.material_type


class SpecimenCategoryMetaMapper(AbstractCategoryMetaMapper):
    _organismPartMapper = StringEqualityCategoryMapper(
        [
            "Animal Bone",
            "Human Bone",
            "Non Diagnostic Bone",
        ],
        "organismpart",
        vocabulary_mapper.specimen_type
    )
    _artifactMapper = StringEqualityCategoryMapper(
        [
            "Architectural Element",
            "Bulk Ceramic",
            "Bulk Lithic",
            "Coin",
            "Glass",
            "Groundstone",
            "Pottery",
            "Sculpture",
            "Sample",
        ],
        "artifact",
        vocabulary_mapper.specimen_type
    )
    _biologicalSpecimenMapper = StringEqualityCategoryMapper(
        [
            "Biological record",
            "Biological subject, Ecofact",
            "Plant remains",
        ], "biologicalspecimen", vocabulary_mapper.specimen_type
    )
    _physicalSpecimenMapper = StringEqualityCategoryMapper(
        [
            "Sample, Collection, or Aggregation",
            "Object"
        ],
        "physicalspecimen",
        vocabulary_mapper.specimen_type
    )
    _organismProductMapper = StringEqualityCategoryMapper(
        ["Shell"], "organismproduct", vocabulary_mapper.specimen_type
    )
    _notSampleMapper = StringEqualityCategoryMapper(
        [
            "Sample, Collection, or Aggregation",
            "Human Subject",
            "Reference Collection",
        ],
        "physicalspecimen",
        vocabulary_mapper.specimen_type
    )

    @classmethod
    def categories_mappers(cls) -> typing.List[AbstractCategoryMapper]:
        return [
            cls._organismPartMapper,
            cls._artifactMapper,
            cls._biologicalSpecimenMapper,
            cls._physicalSpecimenMapper,
            cls._organismProductMapper,
            cls._notSampleMapper
        ]

    @classmethod
    def controlled_vocabulary_callable(cls) -> Callable:
        return vocabulary_mapper.specimen_type


class OpenContextTransformer(Transformer):

    def __init__(self, source_record: typing.Dict):
        super().__init__(source_record)
        self._material_prediction_results: typing.Optional[list] = None
        self._specimen_prediction_results: typing.Optional[list] = None

    def _citation_uri(self) -> str:
        citation_uri = self.source_record.get("citation uri")
        if citation_uri is None:
            raise MissingIdentifierException("OpenContext record is missing a citation uri")
        return citation_uri

    def id_string(self) -> str:
        citation_uri = self._citation_uri()
        if citation_uri.startswith("https"):
            metadata_payload = citation_uri.removeprefix(Transformer.N2T_ARK_PREFIX)
        else:
            metadata_payload = citation_uri.removeprefix(Transformer.N2T_ARK_NO_HTTPS_PREFIX)
        return f"metadata/{metadata_payload}"

    def sample_identifier_string(self) -> str:
        return self._citation_uri().removeprefix(Transformer.N2T_PREFIX)

    def sample_label(self) -> str:
        label = self.source_record.get("label", Transformer.NOT_PROVIDED)
        item_category = self._item_category()
        if len(item_category) > 0:
            label = f"{item_category} {label}"
        return label

    def sample_description(self) -> str:
        description_pieces: list[str] = []
        self._transform_key_to_label(
            "early bce/ce", self.source_record, description_pieces, "'early bce/ce'"
        )
        self._transform_key_to_label(
            "late bce/ce", self.source_record, description_pieces, "'late bce/ce'"
        )
        self._transform_key_to_label("updated", self.source_record, description_pieces, "'updated'")
        for consists_of_str in self.source_record.get("Consists of", []):
            self._transform_key_to_label_str(
                OpenContextTransformer._get_oc_str_or_dict_item_label(consists_of_str), description_pieces, "'Consists of'"
            )
        for has_type_str in self.source_record.get("Has type", []):
            self._transform_key_to_label_str(
                OpenContextTransformer._get_oc_str_or_dict_item_label(has_type_str), description_pieces, "'Has type'"
            )
        for has_anatomical_str in self.source_record.get(
            "Has anatomical identification", []
        ):
            self._transform_key_to_label_str(
                OpenContextTransformer._get_oc_str_or_dict_item_label(has_anatomical_str),
                description_pieces,
                "'Has anatomical identification'",
            )
        for temporal_coverage_str in self.source_record.get("Temporal Coverage", []):
            self._transform_key_to_label_str(
                OpenContextTransformer._get_oc_str_or_dict_item_label(temporal_coverage_str),
                description_pieces,
                "'Temporal coverage'",
            )
        return Transformer.DESCRIPTION_SEPARATOR.join(description_pieces)

    @staticmethod
    def _get_oc_str_or_dict_item_label(str_or_dict):
        """A utility method to get a dictionary label or if a string, return the string"""
        # This is a bit messy, but it should be a bit forgiving if the OC API returns
        # dict or string items for certain record attributes.
        if isinstance(str_or_dict, dict):
            # this item is a dictionary.
            return str_or_dict.get("label")
        elif isinstance(str_or_dict, str):
            return str_or_dict
        return str_or_dict

    def _material_type(self) -> typing.Optional[str]:
        for consists_of_dict in self.source_record.get("Consists of", []):
            return OpenContextTransformer._get_oc_str_or_dict_item_label(consists_of_dict)
        return None

    def _specimen_type(self) -> typing.Optional[str]:
        for has_type_dict in self.source_record.get("Has type", []):
            return OpenContextTransformer._get_oc_str_or_dict_item_label(has_type_dict)
        return None

    def sample_registrant(self) -> str:
        return ""

    def sample_sampling_purpose(self) -> str:
        return ""

    def has_context_categories(self) -> typing.List[VocabularyTerm]:
        return [vocabulary_mapper.sampled_feature_type().term_for_key("sf:pasthumanoccupationsite")]

    def _compute_material_prediction_results(self) -> typing.Optional[typing.List[PredictionResult]]:
        item_category = self._item_category()
        to_classify_items = ["Object", "Pottery", "Sample", "Sculpture"]
        if item_category not in to_classify_items:
            # Have specified mapping, won't predict
            return None
        elif self._material_prediction_results is not None:
            # Have already computed, don't predict again
            return self._material_prediction_results
        else:
            self._material_prediction_results = MODEL_SERVER_CLIENT.make_opencontext_material_request(self.source_record)
            return self._material_prediction_results

    def _item_category(self):
        return self.source_record.get("item category", "")

    def has_material_categories(self) -> list[VocabularyTerm]:
        item_category = self._item_category()
        to_classify_items = ["Object", "Pottery", "Sample", "Sculpture"]
        if item_category in to_classify_items:
            prediction_results = self._compute_material_prediction_results()
            if prediction_results is not None:
                return [vocabulary_mapper.material_type().term_for_label(prediction.value) for prediction in prediction_results]
            else:
                return []
        return MaterialCategoryMetaMapper.categories(item_category)

    def has_material_category_confidences(self, material_categories: list[VocabularyTerm]) -> typing.Optional[typing.List[float]]:
        prediction_results = self._compute_material_prediction_results()
        if prediction_results is None:
            material_categories_terms = self.has_material_categories()
            return Transformer._rule_based_confidence_list_for_categories_list(material_categories_terms)
        else:
            return [prediction.confidence for prediction in prediction_results]

    def _compute_specimen_prediction_results(self) -> typing.Optional[typing.List[PredictionResult]]:
        item_category = self._item_category()
        to_classify_items = ["Animal Bone"]
        if item_category not in to_classify_items:
            # Have specified mapping, won't predict
            return None
        elif self._specimen_prediction_results is not None:
            # Have already computed, don't predict again
            return self._specimen_prediction_results
        else:
            self._specimen_prediction_results = MODEL_SERVER_CLIENT.make_opencontext_sample_request(self.source_record)
            return self._specimen_prediction_results

    def has_specimen_categories(self) -> list[VocabularyTerm]:
        item_category = self._item_category()
        to_classify_items = ["Animal Bone"]
        if item_category in to_classify_items:
            prediction_results = self._compute_specimen_prediction_results()
            if prediction_results is not None:
                return [vocabulary_mapper.specimen_type().term_for_label(prediction.value) for prediction in prediction_results]
            else:
                return []
        return SpecimenCategoryMetaMapper.categories(item_category)

    def has_specimen_category_confidences(self, specimen_categories: list[VocabularyTerm]) -> typing.Optional[typing.List[float]]:
        prediction_results = self._compute_specimen_prediction_results()
        if prediction_results is None:
            # Not computed, so default to human entered confidence
            specimen_category_terms = self.has_specimen_categories()
            return Transformer._rule_based_confidence_list_for_categories_list(specimen_category_terms)
        else:
            return [prediction.confidence for prediction in prediction_results]

    def _context_label_str(self) -> str:
        return self.source_record.get("context", {}).get("label", Transformer.NOT_PROVIDED)

    def _context_label_pieces(self) -> typing.List[str]:
        context_label = self._context_label_str()
        if type(context_label) is str and len(context_label) > 0:
            return context_label.split("/")
        else:
            return []

    def _extract_getty_keywords(self) -> list[Keyword]:
        # For the getty terms, we have pairs of keys that look like this:
        #
        #  "inorganic material [getty-aat-300010360]": ["glass (material)"],
        #  "inorganic material [getty-aat-300010360] [URI]": ["https://vocab.getty.edu/aat/300010797"]
        # We don't know all the terms beforehand, so we'll iterate the dictionary keys, and look for things that have
        # the getty-aat key format in them, check to see if it has URI in the key, and build a Keyword that glues
        # them together.
        keywords_by_getty_id: dict[str, Keyword] = {}
        for k, v in self.source_record.items():
            if "getty-aat" in k:
                # extract out the piece of the key that is the getty specific piece
                match = GETTY_AAT_REGEX.search(k)
                if match is not None:
                    getty_key = match.group(1)
                    keyword = keywords_by_getty_id.get(getty_key)
                    if keyword is None:
                        keyword = Keyword("", "", AAT_NAME)
                        keywords_by_getty_id[getty_key] = keyword
                    if "URI" in k:
                        keyword.uri = v[0]
                    else:
                        keyword.value = v[0]
        return list(keywords_by_getty_id.values())

    def _convert_subject_to_keywords(self, subject_key: str) -> list[dict[str, str]]:
        subjects = self.source_record.get(subject_key)
        if subjects is not None:
            metadata_dicts = []
            for subject in subjects:
                if type(subject) is dict:
                    keyword = Keyword(subject.get("label", subject.get("id")))
                elif type(subject) is str:
                    keyword = Keyword(subject)
                metadata_dicts.append(keyword.metadata_dict())
            return metadata_dicts
        else:
            return []

    def keywords(self) -> typing.List[dict[str, str]]:
        getty_keywords = self._extract_getty_keywords()
        keyword_dicts = [keyword.metadata_dict() for keyword in getty_keywords]
        keyword_dicts.extend(self._convert_subject_to_keywords("Subject"))
        keyword_dicts.extend(self._convert_subject_to_keywords("Coverage"))
        keyword_dicts.extend(self._convert_subject_to_keywords("Temporal Coverage"))
        # Also need to grab the context label, context href, and make a keyword object from them
        return keyword_dicts

    def produced_by_id_string(self) -> str:
        return Transformer.NOT_PROVIDED

    def _project_dict(self) -> dict:
        return self.source_record.get("project", {})

    def produced_by_label(self) -> str:
        return self._project_dict().get("label", Transformer.NOT_PROVIDED)

    def produced_by_description(self) -> str:
        return self._project_dict().get("id", Transformer.NOT_PROVIDED)

    def produced_by_feature_of_interest(self) -> str:
        return Transformer.NOT_PROVIDED

    def produced_by_responsibilities(self) -> typing.List[dict[str, str]]:
        # from ekansa:
        # "Creator" is typically a project PI (Principle Investigator). They may or may not be the person that
        # collected the sample. If given, a "Contributor" is the person that originally collected or first
        # described the specimen.
        responsibilities = []
        creators = self.source_record.get("Creator")
        if creators is not None:
            for creator in creators:
                responsibilities.append(Transformer._responsibility_dict("creator", OpenContextTransformer._get_oc_str_or_dict_item_label(creator)))
        contributors = self.source_record.get("Contributor")
        if contributors is not None:
            for contributor in contributors:
                responsibilities.append(Transformer._responsibility_dict("collector", OpenContextTransformer._get_oc_str_or_dict_item_label(contributor)))
        return responsibilities

    def produced_by_result_time(self) -> str:
        return self.source_record.get("published", Transformer.NOT_PROVIDED)

    def sampling_site_description(self) -> str:
        explicit_sampling_site = self._explicit_sampling_site()
        if explicit_sampling_site is not None:
            return explicit_sampling_site.get("identifier")
        return Transformer.NOT_PROVIDED

    def sampling_site_label(self) -> str:
        explicit_sampling_site = self._explicit_sampling_site()
        if explicit_sampling_site is not None:
            return explicit_sampling_site.get("label")
        return self._context_label_str()

    def _explicit_sampling_site(self):
        return self.source_record.get("isam:SamplingSite")

    def sampling_site_elevation(self) -> str:
        return Transformer.NOT_PROVIDED

    def sampling_site_latitude(self) -> Optional[typing.SupportsFloat]:
        return _content_latitude(self.source_record)

    def sampling_site_longitude(self) -> Optional[typing.SupportsFloat]:
        return _content_longitude(self.source_record)

    def sampling_site_place_names(self) -> typing.List:
        return self._context_label_pieces()

    def informal_classification(self) -> typing.List[str]:
        classifications = []
        for consists_of_dict in self.source_record.get("Has taxonomic identifier", []):
            classifications.append(OpenContextTransformer._get_oc_str_or_dict_item_label(consists_of_dict))
        return classifications

    def last_updated_time(self) -> Optional[str]:
        return self.source_record.get("updated", None)

    def authorized_by(self) -> typing.List[str]:
        # Don't have this information
        return []

    def complies_with(self) -> typing.List[str]:
        # Don't have this information
        return []

    def h3_function(self) -> typing.Callable:
        return geo_to_h3


def _content_latitude(content: typing.Dict) -> Optional[float]:
    return content.get("latitude", None)


def _content_longitude(content: typing.Dict) -> Optional[float]:
    return content.get("longitude", None)


def geo_to_h3(content: typing.Dict, resolution: int = Transformer.DEFAULT_H3_RESOLUTION) -> Optional[str]:
    return isamples_metadata.Transformer.geo_to_h3(_content_latitude(content), _content_longitude(content), resolution)
