import os
import pandas as pd
import zipfile
from datetime import datetime
import traceback
import psycopg2.extras

# Connection details
host = "localhost"
port = "5432"
user = "postgres"
password = "pass"
database = "postgres"
table_name = "aggrtradedata"

base_directory = r"D:\Tonnochy\binance-public-data\python\data\futures\um\daily"
# List of coins
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


def extract_and_insert_zip(zip_file, connection, cursor):
    try:
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            for file_name in zip_ref.namelist():
                with zip_ref.open(file_name) as file:
                    df = pd.read_csv(file)

                    connection.autocommit = False

                    try:
                        for index, row in df.iterrows():
                            agg_trade_id = int(row['agg_trade_id'])
                            price = float(row['price'])
                            quantity = float(row['quantity'])  # Adjusted type to float for quantity
                            first_trade_id = int(row['first_trade_id'])
                            last_trade_id = int(row['last_trade_id'])
                            transact_time = int(row['transact_time'])
                            is_buyer_maker = row['is_buyer_maker'] == 'True'

                            insert_query = f"INSERT INTO {table_name} (agg_trade_id, price, quantity, first_trade_id, last_trade_id, transact_time, is_buyer_maker) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                            cursor.execute(insert_query, (agg_trade_id, price, quantity, first_trade_id, last_trade_id, transact_time, is_buyer_maker))
                        connection.commit()
                    except Exception as e:
                        connection.rollback()
                        log_exception(f"Error while inserting data from {zip_file}: {e}")
            print(f"Successfully extracted and inserted contents from {zip_file}")
    except Exception as e:
        log_exception(f"Error while extracting and inserting {zip_file}: {e}")



# Create the initial PostgreSQL connection
connection = psycopg2.connect(
    host=host,
    port=port,
    user=user,
    password=password,
    database=database
)
cursor = connection.cursor()

# Iterate through coins and extract/insert contents for Klines
for coin in coins:
    klines_directory = os.path.join(base_directory, "aggTrades", coin)
    print(f"\nAgg Trades for {coin}:")
    zip_files = print_zip_files(klines_directory)

    for zip_file in zip_files:
        extract_and_insert_zip(zip_file, connection, cursor)

# Close the cursor and connection after all data is processed
cursor.close()
connection.close()
