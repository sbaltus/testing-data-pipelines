FROM quay.io/minio/minio

RUN echo "mc admin user add airflow XINiUgIaWj9HFuNy lBVVloDy37POHvf3qzhXCTFaXF86TXsl" >> /usr/bin/docker-entrypoint.sh
RUN echo "mc admin update" >> /usr/bin/docker-entrypoint.sh
CMD ["minio"]