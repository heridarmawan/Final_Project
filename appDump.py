#! /usr/bin/python


import pandas as pd
import numpy as np

from sqlalchemy import create_engine

if __name__ == "__main__":
    # from sqlalchemy import create_engine
    username = 'postgres'
    password = 'admin'
    database = 'postgres'
    ip = '172.19.218.86'

    try:
        engine = create_engine(f"postgresql://{username}:{password}@{ip}:5432/{database}")
        print(f"[INFO] Sukses konek PostgreSQL")
    except:
        print(f"[INFO] Belum Sukses konek PostgreSQL")

    list_filename = ['customer','product','transaction']
    for file in list_filename:
        pd.read_csv(f"bigdata_{file}.csv").to_sql(f"bigdata_{file}", con=engine)
        print(f"INFO, SUKSES DUMP FILE {file}")