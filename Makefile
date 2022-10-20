.PHONY: setup
setup:
	python3 -m venv venv
	source venv/bin/activate && pip install -r requirements.txt && pip install -r requirements-dev.txt
	docker build -t watcher-postgres infrastructure/database/

PHONY: run
run:
	AIRFLOW_UID=5999 docker compose -f infrastructure/docker-compose.yaml up -d