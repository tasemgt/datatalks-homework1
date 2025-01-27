FROM python:3.12.8

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app

COPY data_ingest.py data_ingest.py

ENTRYPOINT [ "python", "data_ingest.py" ]
