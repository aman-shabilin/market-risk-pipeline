from fastapi import APIRouter, Depends, Request

from market_risk.api.deps import get_repository
from market_risk.database.repository import MarketDataRepository
from market_risk.ingestion import get_data_source
from market_risk.pipeline.orchestrator import PipelineOrchestrator
from market_risk.schemas.metrics import PipelineStatus

router = APIRouter(prefix="/api/v1/ingest", tags=["ingest"])


@router.post("/", response_model=PipelineStatus)
def trigger_ingest(
    request: Request,
    ticker: str | None = None,
    repo: MarketDataRepository = Depends(get_repository),
) -> PipelineStatus:
    settings = request.app.state.settings
    source = get_data_source(settings)
    orchestrator = PipelineOrchestrator(source=source, repository=repo)
    result = orchestrator.run(ticker_filter=ticker)

    return PipelineStatus(
        last_run=result.finished_at,
        rows_ingested=result.rows_ingested,
        errors=result.validation_errors,
        tickers_processed=result.tickers_processed,
    )
