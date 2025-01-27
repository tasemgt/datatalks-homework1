
import os
import argparse
import pandas as pd
import sqlalchemy as sql
from time import time


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    if url.endswith('.csv.gz'):
        csv_name = 'green_taxi_data.csv.gz'
    else:
        csv_name = 'green_taxi_data.csv'

    os.system(f"wget {url} -O {csv_name}") #Download csv pipeline..

    # Create sql engine for connection
    engine = sql.create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Pandas DDL (Data definition Lang) that defines how the table should look like from data the data frame..
    # print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))

    # Read into pandas df in chunks
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    # Get next chunk of data
    df = next(df_iter)

    # Format date to datetime
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    # Take first row (column names...)
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    # Append first chunk to database table
    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True:
        t_start = time()
        
        df = next(df_iter)

        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

        # Append subsequent n chunks
        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('Chunk added..., took %.3f seconds' %(t_end - t_start))

def zone_ingest(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    
    csv_name = 'taxi_zone_lookup.csv'

    os.system(f"wget {url} -O {csv_name}") #Download csv pipeline..

    # Create sql engine for connection
    engine = sql.create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df = pd.read_csv(csv_name)

    # Take first row (column names...)
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    # Append first chunk to database table
    df.to_sql(name=table_name, con=engine, if_exists='append')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingests CSV data to Postgres')

    # user, password, host, port, database name, table name, url_of_csv
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password name for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port number for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of table where we\'ll write the results to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()
    # print(args.accumulate(args.integers))
    # main(args)
    zone_ingest(args)