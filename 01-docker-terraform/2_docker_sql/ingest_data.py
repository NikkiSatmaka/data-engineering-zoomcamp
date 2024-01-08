#!/usr/bin/env python
# coding: utf-8

import argparse
import time

import pandas as pd
import pyarrow.parquet as pq
import requests
from sqlalchemy import create_engine


def parse_args():
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")
    parser.add_argument("--user", required=True, help="user name for postgres")
    parser.add_argument("--password", required=True, help="password for postgres")
    parser.add_argument("--host", required=True, help="host for postgres")
    parser.add_argument("--port", required=True, help="port for postgres")
    parser.add_argument("--db", required=True, help="database name for postgres")
    parser.add_argument(
        "--table_name",
        required=True,
        help="name of the table where we will write the results to",
    )
    parser.add_argument("--url", required=True, help="url of the parquet file")
    args = parser.parse_args()
    return args


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = "output.parquet"

    with requests.Session() as s:
        r = s.get(url)

    with open(csv_name, "wb") as f:
        f.write(r.content)

    engine = create_engine(f"postgresql+psycopg://{user}:{password}@{host}:{port}/{db}")

    parquet_file = pq.ParquetFile(csv_name)
    parquet_iter = parquet_file.iter_batches(batch_size=100000)

    df = next(parquet_iter).to_pandas()

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")

    df.to_sql(name=table_name, con=engine, if_exists="append")

    while True:
        try:
            t_start = time.perf_counter()

            df = next(parquet_iter).to_pandas()

            df.to_sql(name=table_name, con=engine, if_exists="append")

            t_end = time.perf_counter()

            print(f"inserted another chunk, took {t_end - t_start:.3f} second(s)")

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break


if __name__ == "__main__":
    args = parse_args()

    main(args)
