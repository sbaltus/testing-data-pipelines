.PHONY: setup
setup:
	poetry install
	docker build -t pipelines-airflow-2.3.4 .
	docker build -t watcher-postgres infrastructure/database/
	docker build -t minio-for-airflow infrastructure/minio/

PHONY: run
run:
	docker compose up -d


	AIRFLOW_UID=5999 docker exec -it infrastructure-airflow-webserver-1 airflow connections add 's3_default' \
		--conn-json '{
			"conn_type": "s3",
			"extra": {
				"aws_access_key_id":"XINiUgIaWj9HFuNy",
				"aws_secret_access_key": "lBVVloDy37POHvf3qzhXCTFaXF86TXsl",
				"host": "http://minio:9000"
			}
		}'
