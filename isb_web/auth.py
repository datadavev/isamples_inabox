import logging
from typing import Optional

import authlib.integrations.starlette_client
import starlette
import starlette.middleware
import starlette.middleware.cors
import starlette.middleware.sessions
import starlette_oauth2_api
from fastapi import FastAPI

from isb_lib.utilities import url_utilities
from isb_web import config

logging.basicConfig(level=logging.DEBUG)
_L = logging.getLogger("auth")

allowed_orcid_ids: list = []


class AuthenticateMiddleware(starlette_oauth2_api.AuthenticateMiddleware):
    """
    Override the __call__ method of the AuthenticateMiddleware to also check
    cookies for auth information. This enables access by either a JWT or the
    authentication information stored in a cookie.
    """

    async def __call__(
        self,
        scope: starlette.types.Scope,
        receive: starlette.types.Receive,
        send: starlette.types.Send,
    ) -> None:
        request = starlette.requests.HTTPConnection(scope)
        last_path_component = url_utilities.last_path_component(request.url)
        if "/" + last_path_component in self._public_paths:
            return await self._app(scope, receive, send)

        token = None
        user = request.session.get("user")

        # Cookie set with auth info
        if user is not None:
            token = user.get("id_token", None)
            orcid_id = user.get("orcid", None)
            if orcid_id not in allowed_orcid_ids:
                return await self._prepare_error_response(
                    "orcid id is not authorized",
                    401,
                    scope,
                    receive,
                    send,
                )

        # check for authorization header and token on it.
        elif "authorization" in request.headers and request.headers[
            "authorization"
        ].startswith("Bearer "):
            token = request.headers["authorization"][len("Bearer "):]

        elif "authorization" in request.headers:
            _L.debug('No "Bearer" in authorization header')
            return await self._prepare_error_response(
                'The "authorization" header must start with "Bearer "',
                400,
                scope,
                receive,
                send,
            )
        else:
            _L.debug("No authorization header")
            return await self._prepare_error_response(
                'The request does not contain an "authorization" header',
                400,
                scope,
                receive,
                send,
            )

        try:
            provider, claims = self.claims(token)
            scope["oauth2-claims"] = claims
            scope["oauth2-provider"] = provider
            scope["oauth2-jwt"] = token
        except starlette_oauth2_api.InvalidToken as e:
            return await self._prepare_error_response(
                e.errors, 401, scope, receive, send
            )

        return await self._app(scope, receive, send)


oauth = authlib.integrations.starlette_client.OAuth()

# Registration here is using openid, which is a higher level wrapper
# around the oauth end points. Take a look at the info at the
# server_metadata_url
oauth.register(
    name="orcid",
    client_id=config.Settings().orcid_client_id,
    client_secret=config.Settings().orcid_client_secret,
    server_metadata_url=config.Settings().orcid_issuer + "/.well-known/openid-configuration",
    client_kwargs={"scope": "openid"},
    api_base_url=config.Settings().orcid_issuer,
)


def add_auth_middleware_to_app(app: FastAPI, public_paths: set[str] = set()):
    # https://gitlab.com/jorgecarleitao/starlette-oauth2-api#how-to-use
    app.add_middleware(
        AuthenticateMiddleware,
        providers={
            "orcid": {
                "issuer": config.Settings().orcid_issuer,
                "keys": config.Settings().orcid_issuer + "/oauth/jwks",
                "audience": config.Settings().orcid_client_id,
            }
        },

        public_paths=public_paths,
    )

    app.add_middleware(
        starlette.middleware.sessions.SessionMiddleware,
        secret_key=config.Settings().session_middleware_key,
    )

    # https://www.starlette.io/middleware/#corsmiddleware
    app.add_middleware(
        starlette.middleware.cors.CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["GET", "HEAD", "POST"],
        allow_headers=["authorization"],
    )


def orcid_id_from_session_or_scope(request: starlette.requests.Request) -> Optional[str]:
    user = None
    try:
        user = request.session.get("user")
    except AssertionError:
        # Don't blow up if there's no session.  Just return None.
        return None
    if user is not None:
        return user.get("orcid", None)
    else:
        oauth_claims = request.scope.get("oauth2-claims")
        if oauth_claims is not None:
            return oauth_claims.get("sub")
    return None
