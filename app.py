# Will Harris
import click
import pandas as pd
import psycopg2
import time
import os

@click.group()
def cli():
    pass

############################

def detectNaN(n):
    return "null" if (str(n).lower() == ('NaN').lower()) else n

def detectNone(n):
    return "null" if len(str(n)) > 1 else f"'{n}'"

############################

database = 'postgres'
user = 'postgres'
password = 'postgres'

@cli.command()
def pgsql_test():
    connection = psycopg2.connect(database=database, user=user, password=password)
    cursor = connection.cursor()
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT VERSION();")
        version = cursor.fetchone()
        print(version)
    except psycopg2.Error as e:
        error = e.pgcode
        print(error)
    finally:
        cursor.close()
        connection.close()

@cli.command()
def pgsql_create_table():
    connection = psycopg2.connect(database=database, user=user, password=password)
    cursor = connection.cursor()
    try:
        cursor = connection.cursor()
        cursor.execute("drop table if exists test_table;")
        cursor.execute("create table if not exists test_table(id bigserial, name varchar(500));")
        connection.commit()
        print('create table [test_table]')
    except psycopg2.Error as e:
        error = e.pgcode
        print(error)
    finally:
        cursor.close()
        connection.close()

@cli.command()
@click.option("--name", default="AWESOMEO-PRO335")
def pgsql_insert_test_record(name):
    connection = psycopg2.connect(database=database, user=user, password=password)
    cursor = connection.cursor()
    try:
        cursor = connection.cursor()
        cursor.execute(f"insert into test_table(name) values ('{name}');")
        connection.commit()
        print('inserted record into [test_table]')
    except psycopg2.Error as e:
        print(f"error:{e}")
    finally:
        cursor.close()
        connection.close()

@cli.command()
@click.option("--file", default='2018\green_tripdata_2018-12.parquet')
def process(file):
    df = pd.read_parquet(file, engine='pyarrow')
    print(df)
    df = df.reset_index()  

    print('Opening connection to database...')
    connection = psycopg2.connect(database=database, user=user, password=password)
    cursor = connection.cursor()
    print('Connection opened! Starting import...')
    start = time.time()
    try:
        cursor = connection.cursor()
        for index, row in df.iterrows():
            cursor.execute(f"insert into taxi_data(vendorID, lpep_pickup, lpep_dropoff, passenger_count, trip_distance, ratecodeID, storefwdflag, pu_locationID, do_locationID, payment_type, fare_amount, extra, mta_tax, tip_amount, toll_amount, improvement_surcharge, total_amount, congestion_surcharge, trip_type, file_source) values ({row['VendorID']}, '{row['lpep_pickup_datetime']}', '{row['lpep_dropoff_datetime']}', {detectNaN(row['passenger_count'])}, {detectNaN(row['trip_distance'])}, {detectNaN(row['RatecodeID'])}, {detectNone(row['store_and_fwd_flag'])}, {row['PULocationID']}, {row['DOLocationID']}, {detectNaN(row['payment_type'])}, {detectNaN(row['fare_amount'])}, {detectNaN(row['extra'])}, {detectNaN(row['mta_tax'])}, {detectNaN(row['tip_amount'])}, {detectNaN(row['tolls_amount'])}, {detectNaN(row['improvement_surcharge'])}, {detectNaN(row['total_amount'])}, {detectNone(row['congestion_surcharge'])}, {detectNaN(row['trip_type'])}, 12);")
    except psycopg2.Error as e:
        print(f"error:{e}")
    finally:
        connection.commit()
        end =  time.time()
        print(f'Import completed in {str((end - start) * 1000)} milliseconds! Closing connection...')
        cursor.close()
        connection.close()

if __name__ == '__main__':
	cli()