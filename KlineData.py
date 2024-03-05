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
table_name = "KlineData"

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
                    # Read the CSV file using pandas
                    df = pd.read_csv(file)

                    # Begin a new transaction
                    connection.autocommit = False  # Ensure autocommit is disabled for manual transaction control

                    try:
                        # Insert data into the database
                        for index, row in df.iterrows():
                            open_time = int(row['open_time'])
                            close_time = row['close_time']
                            open = float(row['open'])
                            high = float(row['close'])
                            low = float(row['low'])
                            close = float(row['close'])
                            quote_volume = float(row['quote_volume'])
                            volume = row['volume']
                            count = row['count']
                            taker_buy_volume = row['taker_buy_volume']
                            taker_buy_quote_volume = row['taker_buy_quote_volume']
                            ignore = row['ignore']
                            insert_query = f"INSERT INTO {table_name} (open_time, close_time, open, high, low, close, quote_volume, volume, count, taker_buy_volume, taker_buy_quote_volume, ignore) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                            cursor.execute(insert_query, (open_time, close_time, open, high, low, close, quote_volume, volume, count, taker_buy_volume, taker_buy_quote_volume, ignore))
                        connection.commit()  # Commit the transaction
                    except Exception as e:
                        connection.rollback()  # Rollback in case of error
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
    klines_directory = os.path.join(base_directory, "klines", coin)
    print(f"\nKlines for {coin}:")
    zip_files = print_zip_files(klines_directory)

    for zip_file in zip_files:
        extract_and_insert_zip(zip_file, connection, cursor)

# Close the cursor and connection after all data is processed
cursor.close()
connection.close()
