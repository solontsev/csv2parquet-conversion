FROM python:3.11-bullseye

RUN apt-get update &&\
    apt-get install -y --no-install-recommends openjdk-11-jre && \
    apt-get clean

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

WORKDIR /tmp

COPY 69M_reddit_accounts.csv ./

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY duckdb_export.py polars_export.py polars_export.py pyspark_export.py pyarrow_export.py ./
