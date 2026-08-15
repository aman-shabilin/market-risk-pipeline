.PHONY: install test lint run docker-up ingest

install:
	pip install -e ".[dev]"

test:
	pytest --cov=market_risk -v

lint:
	ruff check src/ tests/
	mypy src/

run:
	uvicorn market_risk.api.app:create_app --factory --reload

docker-up:
	docker compose up --build

ingest:
	market-risk-ingest
