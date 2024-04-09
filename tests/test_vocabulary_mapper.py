import json
from isamples_metadata.vocabularies.vocabulary_mapper import ControlledVocabulary, VocabularyTerm


def _construct_controlled_vocabulary(filename: str, prefix: str) -> ControlledVocabulary:
    with open(f"./test_data/controlled_vocabulary_uijson/{filename}") as source_file:
        uijson = json.load(source_file)
        return ControlledVocabulary(uijson, prefix)


def _assert_on_vocabulary_term(term: VocabularyTerm):
    assert term is not None
    metadata_dict = term.metadata_dict()
    assert metadata_dict.get("label") is not None
    assert metadata_dict.get("identifier") is not None


def test_sampled_feature():
    sampled_feature_type_vocabulary = _construct_controlled_vocabulary("sampled_feature_type.json", "sf")
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
    material_sample_type_vocabulary = _construct_controlled_vocabulary("material_sample_type.json", "spec")
    whole_organism = material_sample_type_vocabulary.term_for_key("spec:wholeorganism")
    _assert_on_vocabulary_term(whole_organism)
    organism_part = material_sample_type_vocabulary.term_for_key("spec:organismpart")
    _assert_on_vocabulary_term(organism_part)
    assert material_sample_type_vocabulary.root_term().uri == "https://w3id.org/isample/vocabulary/specimentype/1.0/physicalspecimen"


def test_material_type():
    material_type_vocabulary = _construct_controlled_vocabulary("material_type.json", "mat")
    organic_material = material_type_vocabulary.term_for_key("mat:organicmaterial")
    _assert_on_vocabulary_term(organic_material)
    biogenicnonorganicmaterial = material_type_vocabulary.term_for_key("mat:biogenicnonorganicmaterial")
    _assert_on_vocabulary_term(biogenicnonorganicmaterial)
    assert material_type_vocabulary.root_term().uri == "https://w3id.org/isample/vocabulary/material/1.0/material"
