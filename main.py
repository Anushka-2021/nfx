import psycopg2
import os
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, insert
from sqlalchemy.dialects.postgresql import insert
import time

if __name__ == '__main__':
    print_hi('PyCharm')

    conn = psycopg2.connect(dbname="nfx_cp1", user="postgres", password="", host="127.0.0.1", port="5435")
    cursor = conn.cursor()
    conn.autocommit = True

    start_time = time.time()

    log = ["'postgres'", "'nfx_cp1'", "'insert'"]
    sql = "INSERT INTO \"LOGS\".log_table(start_timestamp, username, database, action) VALUES (to_timestamp(" + str(start_time) + "), " + ", ".join(str(elem) for elem in log) + ") RETURNING id;"
    print(sql)
    cursor.execute(sql)
    log_id = cursor.fetchone()[0]

    #читаем данные
    directory = os.fsencode("files")
    for file in os.listdir(directory):
        filename = os.fsdecode(file)


        if(filename == 'md_currency_d.csv'):
            dataset = pd.read_csv("files\\"+filename, delimiter = ";", encoding = "ISO-8859-1")
        else:
            dataset = pd.read_csv("files\\"+filename, delimiter = ";")

        print(dataset.columns)
        dataset.columns =dataset.columns.str.lower()

        print(dataset)
        engine = create_engine('postgresql://postgres:@localhost:5435/nfx_cp1')
        table_name = Path(filename).stem

        def postgres_upsert(table, conn, keys, data_iter):
            data = [dict(zip(keys, row)) for row in data_iter]
            insert_statement = insert(table.table).values(data)
            if(filename == 'ft_posting_f.csv'):
                upsert_statement = insert_statement
            else:
                upsert_statement = insert_statement.on_conflict_do_update(
                    constraint=f"{table.table.name}_pkey",
                    set_={c.key: c for c in insert_statement.excluded},
                )
            conn.execute(upsert_statement)

        if(table_name=="md_exchange_rate_d"):
            dataset = dataset.drop_duplicates(subset=['data_actual_date', 'currency_rk'])
        else:
            dataset = dataset.drop_duplicates()

        if(table_name == "md_currency_d"):
            dataset['currency_code'] = dataset['currency_code'].astype('Int64')

        print(dataset)
        dataset.to_sql(table_name, con=engine, if_exists="append", index=False, method=postgres_upsert, schema="DS")
        print(time.time()-start_time)


    time.sleep(5)

    end_time = time.time()
    duration = end_time - start_time
    end_time = str(end_time)
    start_time = str(start_time)
    duration = str(duration)
    print(type(str(end_time)))

    sql = "UPDATE \"LOGS\".log_table SET end_timestamp = to_timestamp(" + end_time + "), duration =  to_timestamp(" + end_time + ") - to_timestamp(" + start_time + ") WHERE id = " + str(log_id)
    cursor.execute(sql)
    cursor.close()
    conn.close()
