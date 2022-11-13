
from configparser import ConfigParser
#from pathlib import Path
import pandas as pd
import connection
import sqlparse
import json
from datetime import datetime
import os

from pyspark import SparkContext
from pyspark.sql import SparkSession

if __name__ == '__main__':
    filetime = datetime.now().strftime('%Y%m%d')
    print(f"[INFO] Service ETL is Starting .....")


    conf = connection.config('postgresql')
    conn, engine = connection.psql_conn(conf)
    cursor = conn.cursor()

    conf = connection.config('spark')
    spark = connection.spark_conn(app="etlCountry",config=conf)

    path_query = os.getcwd()+'/Queries/'

    query = sqlparse.format(
    open(
        path_query+'TopCountry.sql','r'
        ).read(), strip_comments=True).strip()

    try:
        print(f"[INFO] Service ETL is Running .....")
        df = pd.read_sql(query, engine)

        #insert mart
        cursor.execute(query)
        conn.commit()
        df.to_sql('topCountry', engine, if_exists='append', index=False)
        print(f"[INFO] Update Mart Success .....")

        #create CSV
        SparkDF = spark.createDataFrame(df)
        SparkDF.toPandas() \
                .to_csv(f"TopCountry.csv", index=False)

        print(f"[INFO] Service ETL is Success .....")
    except:
        print(f"[INFO] Service ETL is Failed .....")
    
