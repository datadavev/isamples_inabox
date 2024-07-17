import typing

from isamples_metadata.Transformer import Transformer
from isamples_metadata.vocabularies.vocabulary_mapper import VocabularyTerm


class CoreJSONTransformer(Transformer):

    def transform(self, include_h3: bool = True) -> typing.Dict:
        return self.source_record

    # Note that all methods below here are actually unused but the superclass declares them as abstract so we
    # need to provide implementations for them.

    def id_string(self) -> str:
        return ""

    def sample_identifier_string(self) -> str:
        return ""

    def sample_label(self) -> str:
        return ""

    def sample_description(self) -> str:
        return ""

    def sample_registrant(self) -> str:
        return ""

    def sample_sampling_purpose(self) -> str:
        return ""

    def has_context_categories(self) -> typing.List[VocabularyTerm]:
        return []

    def has_material_categories(self) -> typing.List[VocabularyTerm]:
        return []

    def has_specimen_categories(self) -> typing.List[VocabularyTerm]:
        return []

    def informal_classification(self) -> typing.List[str]:
        return []

    def keywords(self) -> typing.List[dict[str, str]]:
        return []

    def produced_by_id_string(self) -> str:
        return ""

    def produced_by_label(self) -> str:
        return ""

    def produced_by_description(self) -> str:
        return ""

    def produced_by_feature_of_interest(self) -> str:
        return ""

    def produced_by_responsibilities(self) -> typing.List[dict[str, str]]:
        return []

    def produced_by_result_time(self) -> str:
        return ""

    def sampling_site_description(self) -> str:
        return ""

    def sampling_site_label(self) -> str:
        return ""

    def sampling_site_elevation(self) -> str:
        return ""

    def sampling_site_latitude(self) -> typing.Optional[typing.SupportsFloat]:
        return None

    def sampling_site_longitude(self) -> typing.Optional[typing.SupportsFloat]:
        return None

    def sampling_site_place_names(self) -> typing.List:
        return []

    def last_updated_time(self) -> typing.Optional[str]:
        return None

    def authorized_by(self) -> typing.List[str]:
        return []

    def complies_with(self) -> typing.List[str]:
        return []

    def h3_function(self) -> typing.Callable:
        return CoreJSONTransformer.geo_to_h3

    @staticmethod
    def geo_to_h3(content: typing.Dict, resolution: int = Transformer.DEFAULT_H3_RESOLUTION) -> typing.Optional[str]:
        return None
