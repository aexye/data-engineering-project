import argparse
import pandas as pd
from sqlalchemy import create_engine
from time import time
import os

def main(params):

    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = 'output.csv'

    os.system('wget {} -O {}'.format(url, csv_name))

    engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(user, password, host, port, db))


    df_iter = pd.read_csv(csv_name, low_memory=False, iterator=True, chunksize=100000, compression='gzip')

    for df in df_iter:
        start_time = time()
        df.to_sql(name=table_name, con=engine, if_exists='append')
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        end_time = time()
        print('inserted data and it took {} sec'.format(end_time-start_time))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest data from csv files to database')

    #user, password, host, port, database, table, url

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()
    main(args)
    
    
    




