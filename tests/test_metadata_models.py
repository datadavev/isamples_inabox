from isamples_metadata.metadata_models import (
    MetadataModelLoader,
    SESARMaterialPredictor,
    OpenContextMaterialPredictor,
    OpenContextSamplePredictor
)


def test_sesar_material_model():
    MetadataModelLoader.initialize_models()
    sesar_model = MetadataModelLoader.get_sesar_material_model()
    # load the model predictor
    smp = SESARMaterialPredictor(sesar_model)
    sesar_source_record = {
        "@id": "https://data.geosamples.org/sample/igsn/IE22301MW",
        "igsn": "IE22301MW",
        "@context": "https://raw.githubusercontent.com/IGSN/igsn-json/master/schema.igsn.org/json/registration/v0.1/context.jsonld",
        "registrant": {
            "name": "IEDA",
            "identifiers": {
                "id": "https://www.geosamples.org",
                "kind": "uri"
            }
        },
        "description": {
            "log": [
                {
                    "type": "registered",
                    "timestamp": "2015-11-30 11:27:59"
                },
                {
                    "type": "published",
                    "timestamp": "2015-11-30 12:00:00"
                },
                {
                    "type": "lastUpdated",
                    "timestamp": "2015-11-30 11:27:59"
                }
            ],
            "material": "Sediment",
            "collector": "Evan Solomon",
            "publisher": {
                "@id": "https://www.geosamples.org",
                "url": "https://www.geosamples.org",
                "name": "EarthChem",
                "@type": "Organization",
                "contactPoint": {
                    "url": "https://www.geosamples.org/contact/",
                    "name": "Information Desk",
                    "@type": "ContactPoint",
                    "email": "info@geosamples.org",
                    "contactType": "Customer Service"
                }
            },
            "igsnPrefix": "IE223",
            "sampleName": "01-14A_19_X_4_87_117_136.17",
            "sampleType": "Core Sub-Piece",
            "geoLocation": {
                "geo": [
                    {
                        "@type": "GeoCoordinates",
                        "latitude": "16.0593",
                        "longitude": "82.0937"
                    }
                ],
                "@type": "Place"
            },
            "contributors": [
                {
                    "@type": "Role",
                    "roleName": "Sample Owner",
                    "contributor": [
                        {
                            "name": "Justine Sauvage",
                            "@type": "Person",
                            "givenName": "Justine",
                            "familyName": "Sauvage"
                        }
                    ]
                },
                {
                    "@type": "Role",
                    "roleName": "Sample Registrant",
                    "contributor": [
                        {
                            "name": "Justine Sauvage",
                            "@type": "Person",
                            "givenName": "Justine",
                            "familyName": "Sauvage"
                        }
                    ]
                },
                {
                    "@type": "Role",
                    "roleName": "Sample Archive Contact",
                    "contributor": [
                        {
                            "@type": "Person"
                        }
                    ]
                }
            ],
            "collectionMethod": "Coring>PistonCorer",
            "parentIdentifier": "IE22301LL",
            "supplementMetadata": {
                "purpose": "water and solid chemistry/sedimentology/microbiology",
                "document": [

                ],
                "sampleId": 4086441,
                "childIGSN": [

                ],
                "otherName": [

                ],
                "siblingIGSN": [
                    "IE22301N9",
                    "IE22301NA",
                    "IE22301NB",
                    "IE22301NC",
                    "IE22301N6",
                    "IE22301N7",
                    "IE22301N8",
                    "IE22301N3",
                    "IE22301N4",
                    "IE22301N5",
                    "IE22301ND",
                    "IE22301NE",
                    "IE22301NF",
                    "IE22301NG",
                    "IE22301NH",
                    "IE22301NI",
                    "IE22301NJ",
                    "IE22301NK",
                    "IE22301NN",
                    "IE22301NO",
                    "IE22301NP",
                    "IE22301NR",
                    "IE22301NS",
                    "IE22301NT",
                    "IE22301NU",
                    "IE22301NV",
                    "IE22301NW",
                    "IE22301NX",
                    "IE22301NY",
                    "IE22301NZ",
                    "IE22301O0",
                    "IE22301O1",
                    "IE22301O2",
                    "IE22301O3",
                    "IE22301O4",
                    "IE22301LW",
                    "IE22301LX",
                    "IE22301LY",
                    "IE22301LU",
                    "IE22301LV",
                    "IE22301LM",
                    "IE22301LN",
                    "IE22301LO",
                    "IE22301LP",
                    "IE22301LQ",
                    "IE22301LR",
                    "IE22301LS",
                    "IE22301LT",
                    "IE22301M0",
                    "IE22301M1",
                    "IE22301M2",
                    "IE22301M3",
                    "IE22301M4",
                    "IE22301M5",
                    "IE22301M6",
                    "IE22301M7",
                    "IE22301M8",
                    "IE22301M9",
                    "IE22301MA",
                    "IE22301MB",
                    "IE22301MD",
                    "IE22301ME",
                    "IE22301MF",
                    "IE22301MG",
                    "IE22301MH",
                    "IE22301MJ",
                    "IE22301MK",
                    "IE22301ML",
                    "IE22301MM",
                    "IE22301MN",
                    "IE22301MO",
                    "IE22301MP",
                    "IE22301MQ",
                    "IE22301MR",
                    "IE22301MS",
                    "IE22301MT",
                    "IE22301MU",
                    "IE22301MV",
                    "IE22301MX",
                    "IE22301MY",
                    "IE22301MZ",
                    "IE22301N0",
                    "IE22301LZ",
                    "IE22301MI",
                    "IE22301O5",
                    "IE22301O6",
                    "IE22301O7",
                    "IE22301O8",
                    "IE22301O9",
                    "IE22301NM",
                    "IE22301NQ",
                    "IE22301N1",
                    "IE22301NL",
                    "IE22301MC",
                    "IE22301N2"
                ],
                "platformType": "Ship",
                "navigationType": "GPS",
                "publicationUrl": [

                ],
                "cruiseFieldPrgrm": "NGHP01",
                "externalSampleId": "01-14A_19_X_4_87_117_136.17",
                "currentArchiveContact": "esolomn@u.washington.edu"
            }
        }
    }
    prediction = smp.predict_material_type(sesar_source_record)
    assert prediction is not None


def test_opencontext_material_model():
    MetadataModelLoader.initialize_models()
    oc_model = MetadataModelLoader.get_oc_material_model()
    # load the model predictor
    ocmp = OpenContextMaterialPredictor(oc_model)
    oc_source_record = {
        "uri": "http://opencontext.org/subjects/fba17d13-1242-407e-8f62-33ca9d79221f",
        "citation uri": "https://n2t.net/ark:/28722/k2b570022",
        "label": "11208190",
        "project label": "Historic Fort Snelling",
        "project uri": "http://opencontext.org/projects/fab0532a-2953-4f13-aa97-8a9d7e992dbe",
        "context label": "United States/Minnesota/Fort Snelling/Fort Snelling Short Barracks/Base of stairwell",
        "context uri": "http://opencontext.org/subjects/803733ae-6e29-4370-b3c7-5761e6d64bb8",
        "latitude": 44.893131,
        "longitude": -93.181061,
        "early bce/ce": 1820.0,
        "late bce/ce": 1950.0,
        "item category": "Object",
        "published": "2016-06-23T19:15:39Z",
        "updated": "2021-06-27T19:54:46Z",
        "Consists of": [{
            "id": "http://vocab.getty.edu/aat/300011845",
            "label": "leather"
        }],
        "Creator": [{
            "id": "http://opencontext.org/persons/c2c92a0a-4acb-40c0-822d-e20e85fa10a1",
            "label": "Nancy Hoffman"
        }],
        "Has type": [{
            "id": "http://vocab.getty.edu/aat/300117130",
            "label": "fragments (object portions)"
        }]
    }
    prediction = ocmp.predict_material_type(oc_source_record)
    assert prediction is not None


def test_opencontext_sample_model():
    MetadataModelLoader.initialize_models()
    oc_model = MetadataModelLoader.get_oc_sample_model()
    # load the model predictor
    ocsp = OpenContextSamplePredictor(oc_model)
    oc_source_record = {
        "uri": "http://opencontext.org/subjects/fba17d13-1242-407e-8f62-33ca9d79221f",
        "citation uri": "https://n2t.net/ark:/28722/k2b570022",
        "label": "11208190",
        "project label": "Historic Fort Snelling",
        "project uri": "http://opencontext.org/projects/fab0532a-2953-4f13-aa97-8a9d7e992dbe",
        "context label": "United States/Minnesota/Fort Snelling/Fort Snelling Short Barracks/Base of stairwell",
        "context uri": "http://opencontext.org/subjects/803733ae-6e29-4370-b3c7-5761e6d64bb8",
        "latitude": 44.893131,
        "longitude": -93.181061,
        "early bce/ce": 1820.0,
        "late bce/ce": 1950.0,
        "item category": "Object",
        "published": "2016-06-23T19:15:39Z",
        "updated": "2021-06-27T19:54:46Z",
        "Consists of": [{
            "id": "http://vocab.getty.edu/aat/300011845",
            "label": "leather"
        }],
        "Creator": [{
            "id": "http://opencontext.org/persons/c2c92a0a-4acb-40c0-822d-e20e85fa10a1",
            "label": "Nancy Hoffman"
        }],
        "Has type": [{
            "id": "http://vocab.getty.edu/aat/300117130",
            "label": "fragments (object portions)"
        }]
    }
    prediction = ocsp.predict_sample_type(oc_source_record)
    assert prediction is not None
