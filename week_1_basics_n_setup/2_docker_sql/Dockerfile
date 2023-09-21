FROM python:3.11.5

RUN pip install --upgrade pip
RUN pip install pandas pyarrow sqlalchemy "psycopg[binary]" requests

WORKDIR /app
COPY ingest_data.py ingest_data.py

ENTRYPOINT [ "python", "ingest_data.py" ]
