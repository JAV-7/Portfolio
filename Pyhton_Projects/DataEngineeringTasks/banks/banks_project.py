import pandas as pd 
import numpy as np 
import sqlite3 as sql 
import requests
from bs4 import BeautifulSoup
from datetime import datetime

"""
This task simulates ETL process.
"""

url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
csv_path = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
table_attribs = ['Name', 'MC_USD_Billion']
output_path = './Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
log_file = 'code_log.txt'

def log_progress(message):
    timestamp_format = '%Y-%h-%d - %H:%M:%S'
    now = datetime.now() # Retrieve datetime
    timestamp = now.strftime(timestamp_format) # strftime() transforms the date and time to the specified string format
    """
    with open() as f...
            open() is a function used to manage files, its parameters are the route of the file and
            the mode in which it will be opened, in this case 'a' is used to append.
            Additionally, the open() function by its own dows not close the file, thus, close()
            has to be called as well.

            The "with" statement closes the file automatically. It calls two other methods,
            __enter()__ and __exit()__ 
    """     
    with open(log_file,'a') as f: 
        # The write method is used to write on the file
        f.write(timestamp + ':' + message + '\n')


def extract(url, table_attribs):
    html_content = requests.get(url).text
    # Retrieve content of an url in text form
    data = BeautifulSoup(html_content, 'html.parser')
    # Parse the content of the url with the parser 'html.parser'
    df = pd.DataFrame(columns = table_attribs)
    # Create a new DataFrame with the specified columns
    tables = data.find_all('tbody')
    # Find all the 'tbody' tags in the parsed content
    rows = tables[0].find_all('tr')
    # Find all the 'tr' tags in the first 'tbody' tag
    for row in rows:
        col = row.find_all('td')
        # Find all the 'td' tags in the 'tr' tag
        if(len(col) != 0):
            if col[1].find('a') is not None:
                data_dict = {
                    'Name': col[1].get_text(strip = True),
                    'MC_USD_Billion': col[2].get_text(strip=True)}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index = True)
    return df 


# The function transform() takes a DataFrame and a path to a csv file as parameters.
def transform(df, csv_path):
    dataframe = pd.read_csv(csv_path)
    # Read the csv file
    rate_exchange = dataframe.set_index('Currency').to_dict()['Rate']
    # Set the 'Currency' column as the index and the 'Rate' column as the values
    df['MC_USD_Billion'] = pd.to_numeric(df['MC_USD_Billion'], errors='coerce')
    # Convert the 'MC_USD_Billion' column to numeric values. errors='coerce' means
    df['MC_EUR_Billion'] = [np.round(x*rate_exchange['EUR'],2) for x in df['MC_USD_Billion']]
    # Apply the rate of exchange to the 'MC_USD_Billion' column and round to 'MC_EUR_Billion'
    df['MC_GBP_Billion'] = [np.round(x*rate_exchange['GBP'],2) for x in df['MC_USD_Billion']]
    # Apply the rate of exchange to the 'MC_USD_Billion' column and round to 'MC_GBP_Billion'
    df['MC_INR_Billion'] = [np.round(x*rate_exchange['INR'],2) for x in df['MC_USD_Billion']]
    # Apply the rate of exchange to the 'MC_USD_Billion' column and round to 'MC_INR_Billion'
    return df 


def load_to_csv(df, output_path):
    df.to_csv(output_path)
    # Save the DataFrame to a csv file


def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    # Save the DataFrame to a database table


def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

log_progress('Declaring known values')
log_progress('Call extract() function')
df = extract(url, table_attribs)
print(df)
log_progress('Call tranform() function')
df = transform(df, csv_path)
log_progress('Call load_to_csv()')
load_to_csv(df, output_path)
log_progress('Initiate SQLite3')
log_progress('Call load_to_db()')
sql_connection = sql.connect(db_name)
load_to_db(df, sql_connection, table_name)
log_progress('Call run_query()')
query_statement = f"SELECT * from {table_name}"
run_query(query_statement, sql_connection)
query_statement = f"SELECT AVG(MC_GBP_BILLION) FROM {table_name}"
run_query(query_statement, sql_connection)
query_statement = f"SELECT Name from {table_name} LIMIT 5"
run_query(query_statement, sql_connection)
log_progress('Close SQLite connection')
