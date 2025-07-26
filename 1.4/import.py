import pandas as pd
from sqlalchemy import create_engine
import time


if __name__ == '__main__':

    start_time = time.time()
    engine = create_engine('postgresql://postgres:@localhost:5435/nfx_cp1')

    dataset = pd.read_csv('export_dm_f101_round_f.csv', delimiter = ";")
    dataset.columns =dataset.columns.str.lower()
    dataset = dataset.drop_duplicates()
    dataset.to_sql('dm_f101_round_f_v2', con=engine, if_exists="append", index=False, schema="DM")

    end_time = time.time()
    log = [str(start_time),
           'postgres',
           'Null',
           str(end_time - start_time),
           'nfx_cp1',
           'insert',
           '\"DM\".dm_f101_round_f_v2',
           str(end_time)
           ]

    with open('export_log.txt', 'a+') as log_file:
        log_file.write(';'.join(log) + '\n')
