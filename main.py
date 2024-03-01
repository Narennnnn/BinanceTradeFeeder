import os
from pprint import pp, pprint
from numpy import float16
import pandas as pd
import psycopg2.extras
import requests
import gzip
import re
import shutil
import time
from datetime import datetime, timedelta
import traceback


# host = "localhost"
# port = "5432"
# user = "postgres"
# password = "pass"
# database = "postgres"
# table_name = "trade"
#
#
# # Create the initial PostgreSQL connection
# connection = psycopg2.connect(
#     host=host,
#     port=port,
#     user=user,
#     password=password,
#     database=database
# )
# cursor = connection.cursor()
#
# # Function to create or replace the table schema
# def create_table(cursor):
#     # Define the SQL statement to drop the existing table if it exists
#     # drop_table_query = f"DROP TABLE IF EXISTS {table_name}"
#
#     extension_query = "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\""
#
#     # Define the SQL statement to create the new table
#     create_table_query = f"""
#     CREATE TABLE {table_name} (
#         id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
#         timestamp BIGINT,
#         symbol VARCHAR(255),
#         side VARCHAR(255),
#         sizeQuote DOUBLE PRECISION,
#         openTime BIGINT,
#         closeTime BIGINT,
#         lowPrice DOUBLE PRECISION,
#         highPrice DOUBLE PRECISION,
#         openPrice DOUBLE PRECISION,
#         closePrice DOUBLE PRECISION,
#         volumeQuote DOUBLE PRECISION,
#         market VARCHAR(255),
#         exchange VARCHAR(255)
#     )
#     """
#
#
#     try:
#         # cursor.execute(drop_table_query)
#         cursor.execute(extension_query)
#         cursor.execute(create_table_query)
#         connection.commit()
#     except psycopg2.Error as e:
#         print(f"Error creating or replacing the table: {e}")
#
# # create_table(cursor)
#
# # Function to insert rows into the PostgreSQL database and log
# def insert_rows(data, cursor, connection):
#     insert_query = f"INSERT INTO {table_name} (timestamp, symbol, side, sizeQuote, openTime, closeTime, lowPrice, highPrice, openPrice, closePrice, volumeQuote, market, exchange) VALUES %s"
#
#     try:
#         psycopg2.extras.execute_values(cursor, insert_query, data)
#         connection.commit()
#         print(f"Inserted {len(data)} rows.")
#     except psycopg2.Error as e:
#         connection.rollback()
#         print(f"Error inserting data: {e}")


base_directory = r"D:\Tonnochy\binance-public-data\python\data\futures\um\daily"

# List of coins
coins = ["ADAUSDT", "BTCUSDT", "ETHUSDT", "XRPUSDT", "SOLUSDT", "DOGEUSDT", "USDM"]

# Function to log exceptions to a text file
def log_exception(exception):
    with open("exception_log.txt", "a") as log_file:
        log_file.write("Exception details:\n")
        log_file.write(f"{datetime.now()} - {exception}\n")
        traceback.print_exc(file=log_file)
        log_file.write("\n\n")

# Function to print zip file names
def print_zip_files(directory):
    try:
        for root, dirs, files in os.walk(directory):
            for file in files:
                if file.endswith(".zip"):
                    print(os.path.join(root, file))
    except Exception as e:
        log_exception(f"Error while processing {directory}: {e}")

# Iterate through coins and print zip file names for Aggr Trades
for coin in coins:
    agg_trades_directory = os.path.join(base_directory, "aggTrades", coin)
    print(f"\nAggTrades for {coin}:")
    if os.path.exists(agg_trades_directory):
        print_zip_files(agg_trades_directory)
    else:
        log_exception(f"Directory not found: {agg_trades_directory}")

# Iterate through coins and print zip file names for Klines
for coin in coins:
    klines_directory = os.path.join(base_directory, "klines", coin)
    print(f"\nKlines for {coin}:")
    if os.path.exists(klines_directory):
        print_zip_files(klines_directory)
    else:
        log_exception(f"Directory not found: {klines_directory}")
