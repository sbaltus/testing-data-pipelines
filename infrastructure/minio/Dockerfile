FROM quay.io/minio/minio


RUN curl https://dl.min.io/client/mc/release/linux-amd64/mc \
      --create-dirs \
      -o /home/minio-binaries/mc
RUN chmod +x /home/minio-binaries/mc

RUN /home/minio-binaries/mc --help
ENV PATH=$PATH:/home/minio-binaries/
RUN mc --help
RUN echo "mc config host add local http://localhost:9000 minioadmin minioadmin" >> /usr/bin/docker-entrypoint.sh
RUN echo "mc admin user add airflow XINiUgIaWj9HFuNy lBVVloDy37POHvf3qzhXCTFaXF86TXsl" >> /usr/bin/docker-entrypoint.sh
CMD ["minio"]