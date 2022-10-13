.PHONY: setup
setup:
	poetry install
	docker build -t pipelines-airflow-2.3.4 .
	docker build -t watcher-postgres infrastructure/database/

PHONY: run
run:
	AIRFLOW_UID=5999 docker compose -f infrastructure/docker-compose.yaml up -d