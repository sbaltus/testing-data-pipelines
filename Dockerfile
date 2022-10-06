################
# Builder part
################
FROM python:3.10-buster AS builder

ARG TARGET=/opt/project
WORKDIR ${TARGET}

# Copy the whole repository
COPY . .

WORKDIR ${TARGET}/testing-data-pipelines

# Install Poetry, all dev dependencies then build pipelines package
ENV POETRY_HOME=/usr/local/
RUN curl -sSL https://install.python-poetry.org | python3 - && \
   poetry build -f wheel

################
# Final image
################
FROM apache/airflow:2.3.4-python3.10

ARG WHL=testing_data_pipelines-*-py3-none-any.whl
ARG TARGET=/opt/project


# Take the package we built during the "builder part" without all dev dependencies
COPY --from=builder ${TARGET}/dist/${WHL} /tmp/lib/

# Install the package.
RUN pip install /tmp/lib/${WHL}
