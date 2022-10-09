FROM postgis/postgis:14-master

ENV POSTGRES_DB watcher

ARG TARGET=/opt/project

COPY watcher.sql /docker-entrypoint-initdb.d/

EXPOSE 5432
CMD ["postgres"]