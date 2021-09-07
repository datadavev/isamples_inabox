import fastapi
import logging
import uvicorn
from fastapi import Depends
from fastapi.logger import logger as fastapi_logger
from sqlmodel import SQLModel, create_engine, Session, select
import isb_web.config
from isb_lib.models.thing import Thing
from typing import List

app = fastapi.FastAPI()
database_url = isb_web.config.Settings().database_url
# For unit tests, this won't be set, but we provide an alternate in-memory url and override the engine, so don't worry
if database_url != "UNSET":
    engine = create_engine(database_url, echo=True)


@app.on_event("startup")
def on_startup():
    SQLModel.metadata.create_all(engine)


def get_session():
    with Session(engine) as session:
        yield session


@app.get("/thingsqlmodel/", response_model=List[Thing])
def read_things(session: Session = Depends(get_session)):
    statement = select(Thing).limit(10)
    results = session.exec(statement)
    things = results.all()
    return things


def main():
    logging.basicConfig(level=logging.DEBUG)
    uvicorn.run("isb_web.main_sqlmodel:app", host="0.0.0.0", port=8000, reload=True)


if __name__ == "__main__":
    formatter = logging.Formatter(
        "[%(asctime)s.%(msecs)03d] %(levelname)s [%(thread)d] - %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )
    handler = (
        logging.StreamHandler()
    )  # RotatingFileHandler('/log/abc.log', backupCount=0)
    logging.getLogger().setLevel(logging.NOTSET)
    fastapi_logger.addHandler(handler)
    handler.setFormatter(formatter)

    fastapi_logger.info("****************** Starting Server *****************")
    main()