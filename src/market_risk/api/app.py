from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from market_risk.api.cache import build_cache
from market_risk.api.routes import health, ingest, metrics, portfolio, prices
from market_risk.config import Settings
from market_risk.database.engine import Base, get_engine, get_session_factory


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    settings: Settings = app.state.settings
    engine = get_engine(settings.database_url)
    Base.metadata.create_all(bind=engine)
    app.state.engine = engine
    app.state.session_factory = get_session_factory(engine)
    app.state.cache = build_cache(settings.redis_url)
    yield
    engine.dispose()


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    app = FastAPI(
        title="Market Risk API",
        version="0.1.0",
        description="Ingest market data, compute risk metrics, serve via REST",
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.state.settings = settings
    app.include_router(health.router)
    app.include_router(metrics.router)
    app.include_router(ingest.router)
    app.include_router(portfolio.router)
    app.include_router(prices.router)
    return app
