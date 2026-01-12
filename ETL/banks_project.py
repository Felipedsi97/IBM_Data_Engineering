# Code for ETL operations on Country-GDP data

# Importing the required libraries
import glob 
import pandas as pd 
import xml.etree.ElementTree as ET 
from datetime import datetime 
from bs4 import BeautifulSoup
import requests
import numpy as np
import sqlite3
import csv
import logging


log_file = "code_log.txt"
logging.basicConfig(filename="bank_project.log", level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ' : ' + message + '\n') 


def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            data_dict = {   "Rank": col[0].get_text(strip=True),
                            "Bank name": col[1].find_all("a")[-1].get_text(strip=True),
                            "MC_USD_Billion": col[2].get_text(strip=True)}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
    df["MC_USD_Billion"] = df["MC_USD_Billion"].astype(float)
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    exchange_rate = {}
    with open(csv_path, "r") as f:   # replace with your filename
        reader = csv.DictReader(f)
        for row in reader:
            exchange_rate[row["Currency"]] = float(row["Rate"])

    df['MC_GBP_Billion'] = (df['MC_USD_Billion'] * exchange_rate['GBP']).round(2)
    df['MC_EUR_Billion'] = (df['MC_USD_Billion'] * exchange_rate['EUR']).round(2)
    df['MC_INR_Billion'] = (df['MC_USD_Billion'] * exchange_rate['INR']).round(2)

    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    '''This function saves the final DataFrame to a database table
    with the provided name. Function returns nothing.'''
    try:
        # Load DataFrame into the table using the provided connection
        df.to_sql(table_name, sql_connection, if_exists="replace", index=False)

        # Rename column inside the DB (example: "Bank name" → "Name")
        cursor = sql_connection.cursor()
        cursor.execute(f'ALTER TABLE {table_name} RENAME COLUMN "Bank name" TO Name;')
        sql_connection.commit()

        # Log success
        logging.info(f"DataFrame successfully loaded into '{table_name}'. "
                     f"Rows inserted: {len(df)}")
    except Exception as e:
        logging.error(f"Error loading DataFrame into '{table_name}': {e}")

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    try:
        cursor = sql_connection.cursor()
        cursor.execute(query_statement)
        results = cursor.fetchall()

        # Print the query statement
        print("Query Statement:")
        print(query_statement)
        print("\nQuery Output:")

        # Print the results row by row
        for row in results:
            print(row)

    except Exception as e:
        print(f"Error executing query: {e}")

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Rank", "Bank name", "MC_USD_Billion"]
table_name = 'Largest_banks'
db_name = 'Banks.db'
csv_path = './exchange_rate.csv'
output_path = './generated_csv'
log_progress('Preliminaries complete. Initiating ETL process')
df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')
df = transform(df, csv_path)
log_progress('Data transformation complete. Initiating loading process')
load_to_csv(df, output_path)
log_progress('Data saved to CSV file')
sql_connection = sqlite3.connect(db_name)
log_progress('SQL Connection initiated.')
load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as table. Running the query')
query_statement = f"SELECT * from {table_name}"
run_query(query_statement, sql_connection)
query_statement = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
run_query(query_statement, sql_connection)
query_statement = f"SELECT Name from {table_name} LIMIT 5"
run_query(query_statement, sql_connection)
log_progress('Process Complete.')
sql_connection.close()
log_progress('Server Connection closed.')