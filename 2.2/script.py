import psycopg2
import os
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, insert

def create_conn(db_name):
    conn = 
    return conn

if __name__ == '__main__':

    conn = psycopg2.connect(dbname="dwh", user="postgres", password="", host="127.0.0.1", port="5435")
    cursor = conn.cursor()
    conn.autocommit = True
    engine = create_engine('postgresql://postgres:@localhost:5435/dwh')

    #читаем данные
    df = pd.read_csv("files_dwh\\deal_info.csv", delimiter = ",", encoding='latin-1')
    df.columns =df.columns.str.lower()

    table_name = "deal_info"
    df = df.drop_duplicates()
    df.to_sql(table_name, con=engine, if_exists="append", index=False, schema="rd")
  

    df = pd.read_csv("files_dwh\\product_info.csv", delimiter = ",", encoding='cp1251')
    df.columns =df.columns.str.lower()
    df['effective_from_date'] = pd.to_datetime(df['effective_from_date'])
    df['effective_to_date'] = pd.to_date(df['effective_to_date'])

    table_name = "product"
    df = df.drop_duplicates()
    df.to_sql(table_name, con=engine, if_exists="replace", index=False, schema="rd")
