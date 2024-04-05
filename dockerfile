FROM python:3.10-slim

RUN pip install poetry

COPY . .
COPY ./data/transaction.csv /opt/data/transaction.csv

RUN poetry config virtualenvs.create false
RUN poetry install

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64/
RUN export JAVA_HOME
