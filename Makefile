up:      ## start the whole platform
	docker compose up --build
down:
	docker compose down
test:
	pytest -q
lint:
	ruff check .
run-now: ## trigger the flow once, outside the schedule
	docker compose run --rm pipeline python -c "from orchestration.flows import iowa_daily; iowa_daily()"
