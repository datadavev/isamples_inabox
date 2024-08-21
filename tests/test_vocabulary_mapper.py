from isamples_metadata.vocabularies import vocabulary_mapper
from isamples_metadata.vocabularies.vocabulary_mapper import VocabularyTerm


def _assert_on_vocabulary_term(term: VocabularyTerm):
    assert term is not None
    metadata_dict = term.metadata_dict()
    assert metadata_dict.get("label") is not None
    assert metadata_dict.get("identifier") is not None


# Note that all of these are initialized/installed in the controlled_vocabularies() session fixture in conftest.py


def test_sampled_feature():
    sampled_feature_type_vocabulary = vocabulary_mapper.SAMPLED_FEATURE_TYPE
    past_human_activities = sampled_feature_type_vocabulary.term_for_key("sf:pasthumanoccupationsite")
    _assert_on_vocabulary_term(past_human_activities)
    past_human_activities_no_namespace = past_human_activities = sampled_feature_type_vocabulary.term_for_key("pasthumanoccupationsite")
    _assert_on_vocabulary_term(past_human_activities_no_namespace)
    past_human_activities_by_label = sampled_feature_type_vocabulary.term_for_label("Site of past human activities")
    _assert_on_vocabulary_term(past_human_activities_by_label)
    # labels should be case-insensitive
    past_human_activities_by_label_lower = sampled_feature_type_vocabulary.term_for_label("site of past human activities")
    _assert_on_vocabulary_term(past_human_activities_by_label_lower)
    assert sampled_feature_type_vocabulary.root_term().uri == "https://w3id.org/isample/vocabulary/sampledfeature/1.0/anysampledfeature"


def test_material_sample_type():
    material_sample_type_vocabulary = vocabulary_mapper.SPECIMEN_TYPE
    whole_organism = material_sample_type_vocabulary.term_for_key("spec:wholeorganism")
    _assert_on_vocabulary_term(whole_organism)
    organism_part = material_sample_type_vocabulary.term_for_key("spec:organismpart")
    _assert_on_vocabulary_term(organism_part)
    assert material_sample_type_vocabulary.root_term().uri == "https://w3id.org/isample/vocabulary/materialsampleobjecttype/1.0/materialsample"


def test_material_type():
    material_type_vocabulary = vocabulary_mapper.MATERIAL_TYPE
    organic_material = material_type_vocabulary.term_for_key("mat:organicmaterial")
    _assert_on_vocabulary_term(organic_material)
    biogenicnonorganicmaterial = material_type_vocabulary.term_for_key("mat:biogenicnonorganicmaterial")
    _assert_on_vocabulary_term(biogenicnonorganicmaterial)
    assert material_type_vocabulary.root_term().uri == "https://w3id.org/isample/vocabulary/material/1.0/material"
