# c:\Data_Tools\Open_Data_Manager\sql_con.py

import os
import urllib
import pyodbc
import pandas as pd
from sqlalchemy import create_engine, text, insert, MetaData, Table
from typing import Optional, Dict, Any


class SQLManager:
    def __init__(self, server: str = 'SLAUGHTERSLOTH\\SQLEXPRESS', database: str = 'KrypticKrabDB', driver: str = '{ODBC Driver 17 for SQL Server}', username: str = 'AZURE_SQL_USERNAME', password: str = 'AZURE_SQL_PASSWORD'):
        self.server = server
        self.database = database
        self.driver = driver
        self.username = os.getenv(username)
        self.password = os.getenv(password)

        self.connection_string = f'DRIVER={self.driver};SERVER={self.server};PORT=1433;DATABASE={self.database};UID={self.username};PWD={self.password}'
        self.params = urllib.parse.quote_plus(self.connection_string)  # Encode the connection string
        self.engine = create_engine(f'mssql+pyodbc:///?odbc_connect={self.params}')

        self.load()

    def load(self) -> None:
        with self.engine.connect() as connection:
            print("Connection successful!")

    def query(self, query: str = 'SELECT * FROM Exchanges', **kwargs: Any) -> list:
        with self.engine.connect() as connection:
            result = connection.execute(text(query))
            rows = result.fetchall()
            return rows

    def sql_read_df(self, table_name: str, schema: Optional[str] = None, **kwargs: Any) -> pd.DataFrame:
        with self.engine.connect() as connection:
            df = pd.read_sql_table(table_name, con=connection, schema=schema)
            return df

    def insert(self, table_name: str, query: Dict[str, Any], **kwargs: Any) -> None:
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=self.engine)
        stmt = insert(table).values(query)
        with self.engine.connect() as connection:
            connection.execute(stmt)
            connection.commit()  # ensures the query is committed
            print(f"Inserted data into {table_name}: {query}")

    def sql_insert_df(self, df: Optional[pd.DataFrame] = None, index: bool = False, schema: str = 'crypto', table_name: str = 'ohlcv_daily', if_exists: str = 'append', **kwargs: Any) -> None:
        if df is None:
            print("No data frame selected, using last data frame")
            df = self.df

        df.to_sql(table_name, schema=schema, con=self.engine, if_exists=if_exists, index=index)
        print("Data inserted successfully.")



