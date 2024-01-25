import os
import pandas as pd
import argparse
from time import time
from sqlalchemy import create_engine


def main(params):
    
    #unpack params
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    tbl_name = params.tbl_name
    url = params.url

    #download csv
    #include the gz extension to make sure pandas can open
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f'wget {url} -O {csv_name}')

    #create connection to postgres to create valid statement
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    #convert datetimes
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=tbl_name, con=engine, if_exists='replace')

    #import first chunk
    df.to_sql(name=tbl_name, con=engine, if_exists='append')

    #import chunk by chunk 
    while True:
        try:
            t_start = time()
            
            df = next(df_iter)

            #convert datetimes
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=tbl_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk..., took %.3f seconds' % (t_end - t_start))

        except StopIteration:
            print('Finished ingesting data into the postrges database')
            break

if __name__ == '__main__':

    #Parse Args#
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='hostname of postgres')
    parser.add_argument('--port', help='port of postgres')
    parser.add_argument('--db', help='postgress database name')
    parser.add_argument('--tbl_name', help='table name of where to write results')
    parser.add_argument('--url', help='url of the csv location')

    args = parser.parse_args()

    main(args)