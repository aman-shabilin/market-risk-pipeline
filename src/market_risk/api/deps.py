from collections.abc import Generator

from fastapi import Request
from sqlalchemy.orm import Session

from market_risk.api.cache import CacheBackend
from market_risk.database.repository import MarketDataRepository


def get_db(request: Request) -> Generator[Session, None, None]:
    session_factory = request.app.state.session_factory
    session = session_factory()
    try:
        yield session
    finally:
        session.close()


def get_repository(request: Request) -> Generator[MarketDataRepository, None, None]:
    session_factory = request.app.state.session_factory
    session = session_factory()
    try:
        yield MarketDataRepository(session)
    finally:
        session.close()


def get_cache(request: Request) -> CacheBackend:
    cache: CacheBackend = request.app.state.cache
    return cache
