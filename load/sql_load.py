import os
import pandas as pd
from sqlalchemy import create_engine, text, insert, MetaData, Table
from typing import Optional, Dict, Any


class SQLLoader:
    def __init__(self, host: str = 'localhost', port: int = 5432, database: str = 'postgres', username: str = 'postgres', password_env_var: str = 'POSTGRESQL_PASSWORD'):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = os.getenv(password_env_var)

        # PostgreSQL connection string using psycopg2
        self.connection_url = f'postgresql+psycopg2://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}'
        self.engine = create_engine(self.connection_url)

        print("SQLManager initialized")
        self.load()
        

    def load(self) -> None:
        with self.engine.connect() as connection:
            print("Connection successful!")

    def query(self, query: str = 'SELECT * FROM exchanges', **kwargs: Any) -> list:
        with self.engine.connect() as connection:
            result = connection.execute(text(query))
            rows = result.fetchall()
            return rows

    def query_full(self, query: str = 'SELECT * FROM exchanges', **kwargs: Any) -> list[Dict[str, Any]]:
        with self.engine.connect() as connection:
            result = connection.execute(text(query))
            rows = result.fetchall()
            columns = result.keys()
            return [dict(zip(columns, row)) for row in rows]
  

    def read_sql_to_df(self, table_name, schema=None, **kwargs):
        with self.engine.connect() as connection:
            self.df = pd.read_sql_table(table_name, con=connection, schema=schema)
        print(self.df.head(10))
        return self.df







    def insert_df_to_sql(self, df=None, index = False,  schema='crypto', table_name='ohlcv_daily', if_exists='append', **kwargs):
        if df is None:
            print("No data frame seleceted")
            
        # add check to ensure that the shemcma does exist , BEFORE it can write to the table 

        # add check to ensure that the the column names are found

        
       
        df.to_sql(table_name, schema=schema, con=self.engine, if_exists=if_exists, index=index)
        print("Data inserted successfully.")

