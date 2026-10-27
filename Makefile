.PHONY: help build up up-local down logs test test-pg lint fmt clean
help:         
	@grep -E '^[a-z-]+:.*##' Makefile | sed 's/:.*##/ -/'
build:          
	docker compose build
up:              
	docker compose up -d
up-local:       
	docker compose --profile local-aws up -d
down:           
	docker compose down
logs:           
	docker compose logs -f airflow-scheduler
test:            
	python -m pytest -q
test-pg:       
	TEST_DATABASE_URL=postgresql+psycopg2://postgres:postgres@localhost:5432/airflow_reddit python -m pytest -q
lint:            
	ruff check .
fmt:           
	ruff check . --fix
clean:          
	find . -name __pycache__ -type d -prune -exec rm -rf {} + ; rm -rf .pytest_cache .ruff_cache
