BASE_URL ?= http://localhost:8080
REQUESTS ?= 20000
CONCURRENCY ?= 1000

.PHONY: up down logs test burst burst-small

up:            ## build and start api + postgres (same image we deploy)
	docker compose up -d --build --wait

down:
	docker compose down -v

logs:
	docker compose logs -f api

test:          ## concurrency/integration tests against the compose Postgres
	docker compose up -d --wait db
	DATABASE_URL=postgresql://postgres:postgres@localhost:5432/seats python -m pytest -q

burst:         ## on-sale stampede + reconciliation: make burst BASE_URL=https://...
	python scripts/burst.py $(BASE_URL) --requests $(REQUESTS) --concurrency $(CONCURRENCY)

burst-small:
	python scripts/burst.py $(BASE_URL) --requests 3000 --concurrency 200 --users 1000 --hot-contenders 200
