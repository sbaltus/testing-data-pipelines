CREATE SCHEMA IF NOT EXISTS watcher;
CREATE TABLE watcher.departments_info(
    region_code varchar,
    region_name varchar,
    department_code varchar,
    department_name varchar,
    department_type varchar,
    department_status varchar,
    siren varchar,
    geometry geometry(GEOMETRY,4326),
    centroid geometry(GEOMETRY,4326),
    population INTEGER,
    processing_date DATE,
    census_year INTEGER
)