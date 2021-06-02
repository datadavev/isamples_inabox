import os
import uvicorn
import datetime
import typing
import requests
import fastapi.staticfiles
import fastapi.templating
import fastapi.middleware.cors
import sqlalchemy.orm
import accept_types
from isb_web import database
from isb_web import schemas
from isb_web import crud
from isb_web import config
from isamples_metadata.SESARTransformer import SESARTransformer

THIS_PATH = os.path.dirname(os.path.abspath(__file__))
WEB_ROOT = config.Settings().web_root
MEDIA_JSON = "application/json"
MEDIA_NQUADS = "application/n-quads"

app = fastapi.FastAPI(root_path=WEB_ROOT)

app.add_middleware(
    fastapi.middleware.cors.CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount(
    "/static",
    fastapi.staticfiles.StaticFiles(directory=os.path.join(THIS_PATH, "static")),
    name="static",
)
templates = fastapi.templating.Jinja2Templates(
    directory=os.path.join(THIS_PATH, "templates")
)


def predicateToURI(p: str):
    """
    Convert a predicate to a URI for n-quads generation.

    TODO: This is a placeholder implementation.

    Args:
        p: predicate value

    Returns: URI-ified predicate
    """
    if (p.find(":")) >= 0:
        return p
    return f"https://isamples.org/def/predicates/{p}"


def getDb():
    """
    Get an instance of a database session with auto-close

    Returns:
        sqlalchemy.Session
    """
    db = database.SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/thing", response_model=schemas.ThingListMeta)
async def thing_list_metadata(
    db: sqlalchemy.orm.Session = fastapi.Depends((getDb)),
):
    """Information about list identifiers"""
    meta = crud.getThingMeta(db)
    return meta


@app.get("/thing/", response_model=typing.List[schemas.ThingListEntry])
async def thing_list(
    offset: int = 0,
    limit: int = 1000,
    status: int = 200,
    authority: str = None,
    db: sqlalchemy.orm.Session = fastapi.Depends((getDb)),
):
    """List identifiers of all Things on this service"""
    things = crud.getThings(
        db, offset=offset, limit=limit, status=status, authority_id=authority
    )
    return things


@app.get("/thing/types", response_model=typing.List[schemas.ThingType])
async def thing_list_types(
    db: sqlalchemy.orm.Session = fastapi.Depends((getDb)),
):
    """List of types of things with counts"""
    return crud.getSampleTypes(db)


@app.get("/thing/{identifier:path}", response_model=typing.Any)
async def get_thing(
    identifier: str,
    full: bool = False,
    format: str = "original",
    db: sqlalchemy.orm.Session = fastapi.Depends((getDb)),
):
    """Record for the specified identifier"""
    item = crud.getThing(db, identifier)
    if item is None:
        raise fastapi.HTTPException(
            status_code=404, detail=f"Thing not found: {identifier}"
        )
    if full or format == "full":
        return item
    if format == "core":
        authority_id = item.authority_id
        if authority_id == "SESAR":
            content = SESARTransformer(item.resolved_content).transform()
        else:
            raise fastapi.HTTPException(
                status_code=404,
                detail=f"Core format not available for authority_id: {authority_id}",
            )
    else:
        content = item.resolved_content
    return fastapi.responses.JSONResponse(
        content=content, media_type=item.resolved_media_type
    )


@app.get(
    "/related",
    response_model=typing.List[schemas.RelationListMeta],
)
# async def relation_metadata(db: sqlalchemy.orm.Session = fastapi.Depends((getDb))):
async def relation_metadata():
    """List of predicates with counts"""
    # return crud.getPredicateCounts(db)
    session = requests.session()
    return crud.getPredicateCountsSolr(session)


'''
@app.get(
    "/related/",
    response_model=typing.List[schemas.RelationListEntry],
    responses={
        200: {
            "content": {
                MEDIA_NQUADS: {
                    "example": "s p o [name]  .\n"
                    "s p o [name]  .\n"
                    "s p o [name]  .\n"
                }
            }
        }
    },
)
async def get_related(
    db: sqlalchemy.orm.Session = fastapi.Depends((getDb)),
    s: str = None,
    p: str = None,
    o: str = None,
    source: str = None,
    name: str = None,
    offset: int = 0,
    limit: int = 1000,
    accept: typing.Optional[str] = fastapi.Header("application/json"),
):
    """Relations that match provided s, p, o, source, name.

    Each property is optional. Exact matches only.
    """
    return_type = accept_types.get_best_match(accept, [MEDIA_JSON, MEDIA_NQUADS])
    res = crud.getRelations(db, s, p, o, source, name, offset, limit)
    if return_type == MEDIA_NQUADS:
        rows = []
        for row in res:
            quad = [row.s, predicateToURI(row.p), row.o]
            if row.name is not None or row.name != "":
                quad.append(row.name)
            quad.append(".")
            rows.append(" ".join(quad))
        return fastapi.responses.PlainTextResponse(
            "\n".join(rows), media_type=MEDIA_NQUADS
        )
    return res
'''


@app.get(
    "/related/",
    response_model=typing.List[schemas.RelationListEntry],
    responses={
        200: {
            "content": {
                MEDIA_NQUADS: {
                    "example": "s p o [name]  .\n"
                    "s p o [name]  .\n"
                    "s p o [name]  .\n"
                }
            }
        }
    },
)
async def get_related_solr(
    s: str = None,
    p: str = None,
    o: str = None,
    source: str = None,
    name: str = None,
    offset: int = 0,
    limit: int = 1000,
    accept: typing.Optional[str] = fastapi.Header("application/json"),
):
    """Relations that match provided s, p, o, source, name.

    Each property is optional. Exact matches only.
    """
    return_type = accept_types.get_best_match(accept, [MEDIA_JSON, MEDIA_NQUADS])
    rsession = requests.Session()
    res = crud.getRelationsSolr(rsession, s, p, o, source, name, offset, limit)
    if return_type == MEDIA_NQUADS:
        rows = []
        for row in res:
            quad = [row.s, predicateToURI(row.p), row.o]
            if row.name is not None or row.name != "":
                quad.append(row.name)
            quad.append(".")
            rows.append(" ".join(quad))
        return fastapi.responses.PlainTextResponse(
            "\n".join(rows), media_type=MEDIA_NQUADS
        )
    return res


@app.get("/", include_in_schema=False)
async def root(request: fastapi.Request):
    return templates.TemplateResponse("index.html", {"request": request})


def main():
    uvicorn.run("isb_web.main:app", host="0.0.0.0", port=8000, reload=True)


if __name__ == "__main__":
    main()
