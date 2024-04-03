import json
from isamples_metadata.vocabularies import vocabulary_mapper
from isamples_metadata.vocabularies.vocabulary_mapper import ControlledVocabulary


def test_controlled_vocabularies():
    sampled_feature_type = "./test_data/controlled_vocabulary_uijson/sampled_feature_type.json"
    with open(sampled_feature_type) as source_file:
        sampled_feature_type_uijson = json.load(source_file)
        sampled_feature_type_vocabulary = ControlledVocabulary(sampled_feature_type_uijson, "sf")
        past_human_activities = sampled_feature_type_vocabulary.term_for_key("sf:pasthumanoccupationsite")
        assert past_human_activities is not None
        metadata_dict = past_human_activities.metadata_dict()
        assert metadata_dict.get("label") is not None
        assert metadata_dict.get("identifier") is not None
        past_human_activities_by_label = sampled_feature_type_vocabulary.term_for_label("Site of past human activities")
        assert past_human_activities_by_label is not None
