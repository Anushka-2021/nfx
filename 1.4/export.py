import pandas as pd
from sqlalchemy import create_engine
import time


if __name__ == '__main__':

    start_time = time.time()
    engine = create_engine('postgresql://postgres:@localhost:5435/nfx_cp1')

    df = pd.read_sql_query("SELECT * FROM \"DM\".dm_f101_round_f", con = engine)
    df.to_csv('export_dm_f101_round_f.csv', index = False, sep=';')

    end_time = time.time()
    log = [str(start_time),
           'postgres',
           'SELECT * FROM \"DM\".dm_f101_round_f',
           str(end_time - start_time),
           'nfx_cp1',
           'select',
           '\"DM\".dm_f101_round_f',
           str(end_time)
           ]

    with open('export_log.txt', 'a+') as log_file:
        log_file.write(';'.join(log) + '\n')
