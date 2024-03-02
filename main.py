import os
import pandas as pd
import zipfile
from datetime import datetime
import traceback
from pprint import pprint
import psycopg2.extras

base_directory = r"D:\Tonnochy\binance-public-data\python\data\futures\um\daily"
# List of coins
coins = ["ADAUSDT", "BTCUSDT"]
# Output file for printing values
output_file = "output_values.txt"

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

# Function to extract and print the contents of a zip file
def extract_and_print_zip(zip_file, output_file):
    try:
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            for file_name in zip_ref.namelist():
                with zip_ref.open(file_name) as file:
                    content = file.read().decode('utf-8')
                    with open(output_file, 'a') as output:
                        output.write(f"\n\nContents of {file_name} in {zip_file}:\n")
                        output.write(content)
        print(f"Successfully extracted and printed contents from {zip_file}")
    except Exception as e:
        log_exception(f"Error while extracting and printing {zip_file}: {e}")

# Iterate through coins and extract/print contents for Aggr Trades
for coin in coins:
    agg_trades_directory = os.path.join(base_directory, "aggTrades", coin)
    print(f"\nAggTrades for {coin}:")
    zip_files = print_zip_files(agg_trades_directory)
    for zip_file in zip_files:
        extract_and_print_zip(zip_file, output_file)

# Iterate through coins and extract/print contents for Klines
for coin in coins:
    klines_directory = os.path.join(base_directory, "klines", coin)
    print(f"\nKlines for {coin}:")
    zip_files = print_zip_files(klines_directory)
    for zip_file in zip_files:
        extract_and_print_zip(zip_file, output_file)
