import sqlite3
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import numpy as np
import requests


def log_progress(message):
    timestamp_format = "%Y-%h-%d-%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("./code_log.txt","a") as f:
        f.write(timestamp + ":" + message + "\n")

def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page, "html.parser")
    df = pd.DataFrame(columns = table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
                MCtxt = col[2].contents[0][:-1]
                MC = float(MCtxt)
                data_dict = {"Bank_Name" : col[1].find('a', recursive=False).contents[0],
                "Market Cap" : MC}
                df1 = pd.DataFrame(data_dict, index=[0])
                if df1 is not None and not df1.empty:
                    df = pd.concat([df,df1], ignore_index=True)
                
    return df

def transform(df, csv_path):
    df1 = pd.read_csv(csv_path)
    rs_dict = df1.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*rs_dict['GBP'],2) for x in df['Market Cap']]
    df['MC_EUR_Billion'] = [np.round(x*rs_dict['EUR'],2) for x in df['Market Cap']]
    df['MC_INR_Billion'] = [np.round(x*rs_dict['INR'],2) for x in df['Market Cap']]
    return df

def load_to_csv(df, output_path):
    df.to_csv(output_path)


def load_to_db(df, sql_connects, table_name):
    df.to_sql(table_name, sql_connects, if_exists='replace', index=False)

def run_queries(sql_connects, query):
    print(query)
    query_output = pd.read_sql(query, sql_connects)
    print(query_output)
    
    
url = "https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ["Bank_Name", "Market Cap"]
csv_path = "exchange_rate.csv"
output_path = "transformed.csv"
table_name = "Largest_banks"
db_name = "Banks.db"


log_progress("Preliminaries complete. Initiating ETL process")
df = extract(url,table_attribs)

log_progress("Data extraction complete. Initiating Transformation process")
df = transform(df, csv_path)

log_progress("Data transformation complete. Initiating Loading process")
load_to_csv(df,output_path)
log_progress("Data saved to CSV file")

sql_connects = sqlite3.connect(db_name)
log_progress("SQL Connection initiated")
load_to_db(df, sql_connects, table_name)
log_progress("Data loaded to Database as a table, Executing queries")

query_1 = f"select * from {table_name}"
query_2 = f"select AVG({df.columns[2]}) from {table_name}"
query_3 = f"select {df.columns[0]} from {table_name} LIMIT 5"

run_queries(sql_connects, query_1)
run_queries(sql_connects, query_2)
run_queries(sql_connects, query_3)
log_progress("Process Complete")


sql_connects.close()

log_progress("Server Connection closed")




