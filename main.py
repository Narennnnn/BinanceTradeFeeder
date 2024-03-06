import os
import pandas as pd
import zipfile
from datetime import datetime
import traceback
import psycopg2.extras
import numpy as np

# Connection details
host = "localhost"
port = "5432"
user = "postgres"
password = "pass"
database = "postgres"
table_name = "TradesHistorical"
# base_directory is where I have downloaded zip locally from date 25 feb to 28 feb for testing using Binance-Public-Data repo
base_directory = r"D:\Tonnochy\binance-public-data\python\data\futures\um\daily"
# List of coins we need to these also ADAUSDT BTCUSDT ETHUSDT XRPUSDT SOLUSDT DOGEUSDT USDM
coins = ["ADAUSDT", "BTCUSDT"]


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
        zip_files = []
        for root, dirs, files in os.walk(directory):
            for file in files:
                if file.endswith(".zip"):
                    zip_files.append(os.path.join(root, file))
        return zip_files
    except Exception as e:
        log_exception(f"Error while processing {directory}: {e}")
        return []


def find_closest_aggr_trade_timestamp(aggr_df, closetime):
    return aggr_df['transact_time'].iloc[(aggr_df['transact_time'] - closetime).abs().idxmin()]


def extract_and_insert_trades_historical(aggr_zip_file, klines_zip_file, coin, connection, cursor):
    try:
        # Extract data from Aggr Trades ZIP file
        with zipfile.ZipFile(aggr_zip_file, 'r') as aggr_zip_ref:
            aggr_df = pd.read_csv(aggr_zip_ref.open(aggr_zip_ref.namelist()[0]))

        # Extract data from Klines ZIP file
        with zipfile.ZipFile(klines_zip_file, 'r') as klines_zip_ref:
            klines_df = pd.read_csv(klines_zip_ref.open(klines_zip_ref.namelist()[0]))

        # Combine data and insert into the database
        for index, kline_row in klines_df.iterrows():
            # Extract relevant Kline data
            closetime = int(kline_row.loc['close_time'])
            timestamp = int(find_closest_aggr_trade_timestamp(aggr_df, closetime))
            # Extract relevant Aggr Trades data
            aggr_data = aggr_df.loc[aggr_df['transact_time'] == timestamp].iloc[0]
            symbol = coin
            dummy = aggr_data['is_buyer_maker']
            # print(type(dummy))
            side = "BUY" if dummy == np.bool_(True) else "SELL"
            sizequote = float(aggr_data['quantity'])
            opentime = int(kline_row.loc['open_time'])
            lowprice = kline_row.loc['low']
            highprice = kline_row.loc['high']
            openprice = kline_row.loc['open']
            closeprice = kline_row.loc['close']
            volumequote = kline_row.loc['quote_volume']

            market = "LINEAR"
            exchange = "BINANCE"
            insert_query = f"INSERT INTO TradesHistorical (timestamp, symbol, side, sizequote, opentime, closetime, lowprice, highprice, openprice, closeprice, volumequote, market, exchange) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(insert_query, (
                timestamp, symbol, side, sizequote, opentime, closetime, lowprice, highprice, openprice, closeprice,
                volumequote, market, exchange))

        connection.commit()
        print(
            f"Successfully extracted and inserted contents from Aggr Trades: {aggr_zip_file} and Klines: {klines_zip_file}")
    except Exception as e:
        log_exception(f"Error while extracting and inserting data: {e}")


# Create the initial PostgreSQL connection
connection = psycopg2.connect(
    host=host,
    port=port,
    user=user,
    password=password,
    database=database
)
cursor = connection.cursor()

# Iterate through coins and extract/insert contents for Trades Historical
for coin in coins:
    aggr_directory = os.path.join(base_directory, "aggTrades", coin)
    klines_directory = os.path.join(base_directory, "klines", coin)
    print(f"\nTrades Historical for {coin}:")
    aggr_zip_files = print_zip_files(aggr_directory)
    klines_zip_files = print_zip_files(klines_directory)

    # Assuming the ZIP files are in the same order for Aggr Trades and Klines
    for aggr_zip_file, klines_zip_file in zip(aggr_zip_files, klines_zip_files):
        extract_and_insert_trades_historical(aggr_zip_file, klines_zip_file, coin, connection, cursor)

# Close the cursor and connection after all data is processed
cursor.close()
connection.close()
