from typing import Optional, Any

import isamples_frictionless
from fastapi import FastAPI, HTTPException, Depends
from sqlmodel import Session
from starlette.responses import Response, PlainTextResponse

from isb_lib.identifiers import datacite
import json
import logging
import requests
import starlette.config
import starlette.requests
import starlette.middleware.cors
import starlette.middleware.sessions
import starlette.types
import starlette.datastructures
import jwt
import datetime

from isb_lib.models.namespace import Namespace
from isb_lib.utilities import url_utilities
from isb_lib.utilities.url_utilities import full_url_from_suffix
from isb_web import config, sqlmodel_database, auth as isbweb_auth
from isb_web.api_types import MintDataciteIdentifierParams, MintDraftIdentifierParams, ManageOrcidForNamespaceParams, \
    AddNamespaceParams, MintNoidyIdentifierParams
from isb_web.sqlmodel_database import SQLModelDAO

# The FastAPI app that mounts as a sub-app to the main FastAPI app
manage_api = FastAPI()
dao: Optional[SQLModelDAO] = None

# use this for constructing url paths to the main handler rather than the manage handler
main_app: Optional[FastAPI] = None
MANAGE_PREFIX = "/manage"


def get_session():
    with dao.get_session() as session:
        yield session


isbweb_auth.add_auth_middleware_to_app(manage_api, {"/login", "/auth", "/logout", "/hypothesis_jwt"})


@manage_api.post("/mint_datacite_identifier", include_in_schema=False)
def mint_identifier(params: MintDataciteIdentifierParams):
    """Mints an identifier using the datacite API
    Args:
        request: The fastapi request
        params: Class that contains the credentials and the data to post to datacite
    Return: The minted identifier
    """
    post_data = json.dumps(params.datacite_metadata).encode("utf-8")
    result = datacite.create_doi(
        requests.session(),
        post_data,
        config.Settings().datacite_username,
        config.Settings().datacite_password,
    )
    if result is not None:
        return result
    else:
        return "Error minting identifier"


@manage_api.post("/mint_draft_datacite_identifiers", include_in_schema=False)
async def mint_draft_identifiers(params: MintDraftIdentifierParams):
    """Mints draft identifiers using the datacite API
    Args:
        params: Class that contains the credentials, data to post to datacite, and the number of drafts to create
    Return: A list of all the minted DOIs
    """
    post_data = json.dumps(params.datacite_metadata).encode("utf-8")
    dois = await datacite.async_create_draft_dois(
        params.num_drafts,
        None,
        None,
        post_data,
        False,
        config.Settings().datacite_username,
        config.Settings().datacite_password,
    )
    return dois


@manage_api.get("/login")
async def login(request: starlette.requests.Request):
    """
    Initiate OAuth2 login with ORCID
    """
    redirect_uri = request.url_for("auth").__str__()
    # check if login is for annotation purpose, if so add query param
    if "annotation" in request.query_params and request.query_params["annotation"] == "true":
        redirect_uri += "?annotation=true"
    elif "thing" in request.query_params:
        redirect_uri += f"?thing={request.query_params['thing']}"
    elif "raw_jwt" in request.query_params and request.query_params["raw_jwt"] == "true":
        redirect_uri += "?raw_jwt=true"
    orcid_auth_uri = await isbweb_auth.oauth.orcid.authorize_redirect(request, redirect_uri)
    return orcid_auth_uri


@manage_api.get("/auth")
async def auth(request: starlette.requests.Request):
    """
    This method is called back by ORCID oauth. It needs to be in the
    registered callbacks of the ORCID Oauth configuration.
    """

    token = await isbweb_auth.oauth.orcid.authorize_access_token(request)
    token_dict = dict(token)
    request.session["user"] = token_dict
    redirect_url = url_utilities.joined_url(str(request.url), config.Settings().auth_response_redirect_fragment)
    # redirect_url = "http://localhost:3000/#/dois"
    if "raw_jwt" in request.query_params and request.query_params["raw_jwt"] == "true":
        return PlainTextResponse(content=token_dict["id_token"])
    elif "annotation" in request.query_params and request.query_params["annotation"] == "true":
        # if login is for annotation purpose, add access token as query param
        redirect_url += "?access_token=" + token["access_token"]
        return starlette.responses.RedirectResponse(url=redirect_url)
    elif "thing" in request.query_params:
        thingpage_url = full_url_from_suffix(str(request.url), f"/thingpage/{request.query_params.get('thing')}")
        return starlette.responses.RedirectResponse(url=thingpage_url)
    else:
        return starlette.responses.RedirectResponse(url=redirect_url)


@manage_api.get("/logout")
async def logout(request: starlette.requests.Request):
    """
    Logout by removing the cookie from the user session.

    Note that this does not invalidate the JWT, which could continue
    to be used. That's a "feature" of JWTs.
    """
    request.session.pop("user", None)
    redirect_url = url_utilities.joined_url(str(request.url), config.Settings().logout_redirect_fragment)
    return starlette.responses.RedirectResponse(url=redirect_url)


@manage_api.get("/userinfo")
def userinfo(request: starlette.requests.Request):
    user: Optional[dict] = request.session.get("user")
    if user is not None:
        auth_time = ""
        user_info = user.get("userinfo")
        if user_info is not None:
            auth_time = user_info.get("auth_time")
        response_dict = {
            "name": user.get("name"),
            "orcid": user.get("orcid"),
            "id_token": user.get("id_token"),
            "expires_at": user.get("expires_at"),
            "auth_time": auth_time,
            "access_token": user.get("access_token")
        }
    else:
        # I think the middleware should prevent this, but just in case…
        raise HTTPException(404)
    return response_dict


@manage_api.get("/add_orcid_id")
def add_orcid_id(request: starlette.requests.Request, session: Session = Depends(get_session)):
    user: Optional[dict] = request.session.get("user")
    if user is not None:
        orcid_id = request.query_params.get("orcid_id")
        if user.get("orcid") not in config.Settings().orcid_superusers:
            raise HTTPException(401, "orcid id not authorized to add users")
        if orcid_id is None:
            raise HTTPException(401, "orcid_id is a required parameter")
        person = sqlmodel_database.save_person_with_orcid_id(session, orcid_id)
        isbweb_auth.allowed_orcid_ids.append(orcid_id)
        return person.primary_key
    else:
        # I think the middleware should prevent this, but just in case…
        raise HTTPException(401, "no session")


@manage_api.post("/add_namespace")
def add_namespace(params: AddNamespaceParams, request: starlette.requests.Request, session: Session = Depends(get_session)):
    orcid_id = isbweb_auth.orcid_id_from_session_or_scope(request)
    if orcid_id is None:
        raise HTTPException(401, "no session")
    elif orcid_id not in config.Settings().orcid_superusers:
        raise HTTPException(401, "orcid id not authorized to manage")
    else:
        existing_namespace = sqlmodel_database.namespace_with_shoulder(session, params.shoulder)
        if existing_namespace is not None:
            raise HTTPException(409, f"namespace with shoulder f{params.shoulder} alaready exists")
        else:
            namespace = Namespace()
            namespace.shoulder = params.shoulder
            namespace.allowed_people = params.orcid_ids
            namespace = sqlmodel_database.save_or_update_namespace(session, namespace)
            return namespace


@manage_api.get("/manage_orcid_id_for_namespace")
def manage_orcid_id_for_namespace(params: ManageOrcidForNamespaceParams, request: starlette.requests.Request,
                                  session: Session = Depends(get_session)):
    user: Optional[dict] = request.session.get("user")
    if user is not None:
        if user.get("orcid") not in config.Settings().orcid_superusers:
            raise HTTPException(401, "orcid id not authorized to manage users")
        namespace = sqlmodel_database.namespace_with_shoulder(session, params.shoulder)
        if namespace is None:
            raise HTTPException(404, f"unable to locate namespace with shoulder {params.shoulder}")
        if params.is_remove:
            namespace.remove_allowed_person(params.orcid_id)
        else:
            namespace.add_allowed_person(params.orcid_id)
        sqlmodel_database.save_or_update_namespace(session, namespace)
        return namespace
    else:
        # I think the middleware should prevent this, but just in case…
        raise HTTPException(401, "no session")


@manage_api.post("/mint_noidy_identifiers", include_in_schema=False)
def mint_noidy_identifiers(params: MintNoidyIdentifierParams, request: starlette.requests.Request,
                           session: Session = Depends(get_session)) -> Any:
    """Mints identifiers using the noidy API.  Requires an active session.
    Args:
        params: Class that contains the shoulder and number of identifiers to mint
    Return: A list of all the minted identifiers, or a 404 if the namespace doesn't exist.
    """
    orcid_id = isbweb_auth.orcid_id_from_session_or_scope(request)
    if orcid_id is None:
        raise HTTPException(401, "no session")
    namespace = sqlmodel_database.namespace_with_shoulder(session, params.shoulder)
    if namespace is None:
        raise HTTPException(404, f"unable to locate namespace with shoulder {params.shoulder}")
    if namespace.allowed_people is None or orcid_id not in namespace.allowed_people:
        raise HTTPException(401, f"user doesn't have access to {params.shoulder}")
    identifiers = sqlmodel_database.mint_identifiers_in_namespace(session, namespace, params.num_identifiers)
    if params.return_filename is None:
        return identifiers
    else:
        csv_str = isamples_frictionless.insert_identifiers_into_template(identifiers)
        headers = {
            "Content-Disposition": f"inline; filename={params.return_filename}",
            "Access-Control-Expose-Headers": "Content-Disposition"
        }
        return Response(bytes(csv_str, "utf-8"), headers=headers, media_type="text/csv")


@manage_api.post("/hypothesis_jwt", include_in_schema=False)
def hypothesis_jwt(request: starlette.requests.Request, session: Session = Depends(get_session)) -> Optional[str]:
    orcid_id = isbweb_auth.orcid_id_from_session_or_scope(request)
    if orcid_id is None:
        raise HTTPException(401, "no session")
    else:
        # Full documentation of format is here: https://h.readthedocs.io/en/latest/publishers/authorization-grant-tokens/

        # The hypothesis accounts require the '-' to be stripped from orcids.
        # e.g. orcid_id = "0000000321097692"
        orcid_id = orcid_id.replace("-", "")
        client_authority = config.Settings().hypothesis_authority
        client_id = config.Settings().hypothesis_jwt_client_id
        client_secret = config.Settings().hypothesis_jwt_client_secret
        client_audience = config.Settings().hypothesis_audience
        now = datetime.datetime.utcnow()
        userid = f"acct:{orcid_id}@{client_authority}"
        logging.debug(f"Going to sign userid {userid} with client_id {client_id} and client secret {client_secret}")
        payload = {
            "aud": client_audience,
            "iss": client_id,
            "sub": userid,
            "nbf": now,
            "exp": now + datetime.timedelta(minutes=10),
        }
        return jwt.encode(payload, client_secret, algorithm="HS256")
