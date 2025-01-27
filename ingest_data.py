import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse
import os

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    lookup_table = params.lookup_table
    url = params.url
    lookup_url = params.lookup_url

    csv_name = 'output.csv.gz'
    lookup_name = 'lookup.csv'

    os.system(f"wget {url} -O {csv_name}")
    os.system(f"wget {lookup_url} -O {lookup_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, compression='gzip', iterator=True, chunksize=100000)
    df_look_iter = pd.read_csv(lookup_name, iterator=True, chunksize=100000)

    df = next(df_iter)
    df_look = next(df_look_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df_look.head(n=0).to_sql(name=lookup_table, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')
    df_look.to_sql(name=lookup_table, con=engine, if_exists='append')

    while True: 
        try:
            print('tarting ingestion of taxi data')
            t_start = time()
            df = next(df_iter)

            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
        
            df.to_sql(name='green_taxi_trips', con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))
        except StopIteration:
            print('completed')
            break

    while True: 
        try:
            print('Starting ingestion of lookup table')
            t_start = time()
            df_look = next(df_look_iter)

            df_look.to_sql(name='lookup_table', con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))
        except StopIteration:
            print('completed')
            break

if __name__ == "__main__":

    #user, password, host, port, database name, table name
    #url of the csv
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")

    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where results will be written to')
    parser.add_argument('--lookup_table', help='name of the lookup table destination')
    parser.add_argument('--url', help='url of the csv file')
    parser.add_argument('--lookup_url', help='url of the lookup table csv file')

    args = parser.parse_args()

    main(args)

