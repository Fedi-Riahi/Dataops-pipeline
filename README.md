# Iowa corn yield platform (DataOps project)

Daily pipeline: NASA POWER weather + USDA NASS county yields -> DuckDB -> dbt -> dashboard.

## Run
    cp .env.example .env     # fill NASS_API_KEY and passwords
    docker compose up --build

- Prefect UI: http://localhost:4200
- MinIO console: http://localhost:9001
- Dashboard: http://localhost:8501

Trigger one run now: `make run-now`. Tests: `make test`.

## Layout
    ingestion/      API clients, upsert, contract validation
    contracts/      one data contract per source (YAML)
    orchestration/  Prefect flow (daily 06:00) + failure alert
    dbt/            staging -> intermediate -> marts, with tests
    dashboard/      Streamlit app
    tests/          pytest

## Team rules
PRs only, one reviewer, CI green before merge. No secrets in the repo.
