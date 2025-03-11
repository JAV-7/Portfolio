# Francisco Javier Ramos Jimenez
# 
import pandas as pd
"""
Pandas is used for handling and manipulating tabular data.
"""
import numpy as np
"""
NumPy is used for numerical operations, particularly rounding GDP values.
"""
from bs4 import BeautifulSoup
"""
BeautifulSoup is used for web scraping to extract the GDP table from Wikipedia.
"""
import sqlite3 as sql
"""
SQLite3 is used to store the extracted and transformed data in a database.
"""
import requests
"""
Requests is used to fetch the webpage containing GDP data.
"""
import datetime
"""
Datetime is used for logging progress with timestamps.
"""

# URL of the archived Wikipedia page containing GDP data
url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'

# Column names for the extracted table
table_atributs = ["Country", "GDP_USD_Millions"]

# Database and table details
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = './Countries_by_GDP.csv'

def extract(url, table_atributs):
    """
    Extracts GDP data from the given Wikipedia page.
    Parses the HTML, finds the relevant table, and extracts country names and GDP values.
    """
    html_text = requests.get(url).text  # Fetch the webpage content
    data = BeautifulSoup(html_text, 'html.parser')  # Parse the HTML
    df = pd.DataFrame(columns=table_atributs)  # Initialize an empty DataFrame

    # Locate all tables on the page
    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')  # Select the table containing GDP data

    # Iterate through table rows and extract relevant data
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            if col[0].find('a') is not None and '—' not in col[2]:  # Ensure valid country and GDP values
                data_dict = {
                    "Country": col[0].a.contents[0],  # Extract country name
                    "GDP_USD_millions": col[2].contents[0]  # Extract GDP value
                }
                df1 = pd.DataFrame(data_dict, index=[0])  # Convert dictionary to DataFrame
                df = pd.concat([df, df1], ignore_index=True)  # Append to the main DataFrame

    return df

def transform(df):
    """
    Transforms the extracted GDP data:
    - Converts GDP from string to float
    - Rounds all GDP values to 100 (which seems incorrect, consider fixing)
    - Renames the column from 'GDP_USD_millions' to 'GDP_USD_billions'
    """
    GDP_list = df["GDP_USD_millions"].tolist()
    
    # Convert GDP values from string (with commas) to float
    GDP_list = [float(("".join(x.split(',')))) for x in GDP_list]
    
    # Incorrect transformation: Overwrites all GDP values with 100.00
    GDP_list = [np.round(100, 2) for x in GDP_list]  

    df["GDP_USD_millions"] = GDP_list
    df = df.rename(columns={"GDP_USD_millions": "GDP_USD_billions"})  # Rename column

    return df

def load_to_csv(df, csv_path):
    """
    Loads the transformed DataFrame into a CSV file.
    """
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    """
    Loads the transformed DataFrame into an SQLite database table.
    If the table exists, it is replaced with the new data.
    """
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    """
    Executes a given SQL query on the SQLite database and prints the results.
    """
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

def log_progress(message):
    """
    Logs the progress of the ETL process with a timestamp.
    Appends messages to a log file for tracking execution history.
    """
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Format for timestamps
    now = datetime.datetime.now()
    timestamp = now.strftime(timestamp_format)  # Get the current timestamp

    with open("./etl_project_log.txt", "a") as f:
        f.write(timestamp + ":" + message + '\n')  # Append log message

log_progress('Preliminaries complete. Initiating ETL process')
df = extract(url, table_atributs)
print(df)
log_progress('Data extraction complete. Initiating Transformation process')
df = transform(df)
log_progress('Data transformation complete. Initiating loading process')
load_to_csv(df, csv_path)
log_progress('Data saved to CSV file')
sql_connection = sql.connect('World_Economies.db')
log_progress('SQL Connection initiated.')
load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as table. Running the query')
query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
run_query(query_statement, sql_connection)
log_progress('Process Complete.')
sql_connection.close()