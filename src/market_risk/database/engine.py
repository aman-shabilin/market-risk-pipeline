from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker, Session


class Base(DeclarativeBase):
    pass


def get_engine(database_url: str) -> Engine:
    connect_args = {}
    if "sqlite" in database_url:
        connect_args["check_same_thread"] = False
    return create_engine(database_url, connect_args=connect_args)


def get_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)
