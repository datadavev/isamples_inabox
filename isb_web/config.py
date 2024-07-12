from pydantic import BaseSettings


class Settings(BaseSettings):
    """
    Settings for the web application. Values are overridden with a
    configuration file "isb_web_config.env", normally located in
    the root folder of the web application startup.
    """

    logging_config: str = "logging.conf"

    # SQLAlchemy database URL string
    # e.g.: postgresql+psycopg2://DBUSER:DBPASS@localhost/isb_1
    database_url: str = "UNSET"

    # The Solr service URL, must end in "/"
    # e.g. http://localhost:8983/solr/isb_core_records/
    solr_url: str = "UNSET"

    thing_url_path: str = "thing"

    stac_item_url_path: str = "stac_item"

    stac_collection_url_path: str = "stac_collection"

    # The URL to the analytics API
    analytics_url: str = "UNSET"

    # The domain to record analytics events for, needs to be configured as a site in plausible.io
    analytics_domain: str = "UNSET"

    # The URL to the datacite API
    datacite_url: str = "https://api.test.datacite.org/"

    # This shouldn't be checked in.  Set by doing export DATACITE_USERNAME=foobar
    datacite_username: str = ""

    # This shouldn't be checked in.  Set by doing export DATACITE_PASSWORD=foobar
    datacite_password: str = ""

    orcid_hostname: str = "orcid.org"

    orcid_token_redirect_uri: str = "http://localhost:8000/orcid_token"

    # This shouldn't be checked in.  Set it by doing export ORCID_CLIENT_ID=foobar
    # This value should be obtained from the orcid API management console.
    orcid_client_id: str = ""

    # This must not ever be checked in.  Set it by doing export ORCID_CLIENT_SECRET=foobar
    # This value should be obtained from the orcid API management console.
    orcid_client_secret: str = ""

    # The base URL for the orcid issuing authority.  Makes it easy to change between sandbox and production orcid.
    orcid_issuer: str = "https://orcid.org"

    # This is a random UUID for encoding the session details in the Session cookie.
    session_middleware_key: str = "81987812-0cf8-459e-b27b-40dabcded856"

    # The destination URL fragment where to drop users after they successfully authenticate in the orcid flow.
    auth_response_redirect_fragment: str = "/isamples_central/ui/#/dois"

    # The destination URL fragment where to drop users after they logout of the management application.
    logout_redirect_fragment: str = "/isamples_central/ui"

    # The list of orcid ids that are allowed to add other orcid ids to the iSB instance.
    orcid_superusers: list[str] = []

    # The authority id used for samples directly created in the iSB instance
    authority_id: str = "iSamples"

    # The absolute path to the FastText model
    fasttext_model_path: str = "UNSET"

    # The absolute path to the BERT model
    sesar_material_model_path: str = "UNSET"
    sesar_material_config_path: str = "UNSET"
    opencontext_material_model_path: str = "UNSET"
    opencontext_material_config_path: str = "UNSET"
    opencontext_sample_model_path: str = "UNSET"
    opencontext_sample_config_path: str = "UNSET"

    # The hypothesis authority to use.  Must be also configured in a running h server.
    hypothesis_authority: str = ""
    # The URL to the running h server.
    hypothesis_server_url: str = ""
    # The client ID to use when signing Hypothesis JWT
    hypothesis_jwt_client_id: str = ""
    # The client secret to use when signing Hypothesis JWT
    hypothesis_jwt_client_secret: str = ""
    # The audience to use when signing Hypothesis JWT
    hypothesis_audience: str = ""

    # List of domain names that will be accepted as valid CORS origins.  '*' is unacceptable because we need to
    # include credentials in JavaScript requests, and '*' is disallowed when you configure things like this.
    cors_allow_origins: list[str] = []

    modelserver_url = "http://localhost:9000/"
    modelserver_lru_cache_size = 10000

    # Whether to prefetch all the taxonomic names at app startup.  Useful for batch processing and reindexing, but
    # uses a lot of memory so shouldn't be enabled by default.
    taxon_cache_enabled: bool = False

    sitemap_dir_prefix: str = "/app/sitemaps"
    sitemap_url_prefix: str = ""
    sitemap_solr_query: str = "*:*"

    class Config:
        env_file = "isb_web_config.env"
        case_sensitive = False
