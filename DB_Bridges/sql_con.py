import os
import pandas as pd
from sqlalchemy import create_engine, text, insert, MetaData, Table
from typing import Optional, Dict, Any


class SQLManager:
    def __init__(self, host: str = 'localhost', port: int = 5432, database: str = 'postgres', username: str = 'postgres', password_env_var: str = 'POSTGRESQL_PASSWORD'):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = os.getenv(password_env_var)

        # PostgreSQL connection string using psycopg2
        self.connection_url = f'postgresql+psycopg2://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}'
        self.engine = create_engine(self.connection_url)

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
            

        df.to_sql(table_name, schema=schema, con=self.engine, if_exists=if_exists, index=index)
        print("Data inserted successfully.")

###
    # def df_to_sql(self, df, table_name, schema='public'):
    #     """
    #     Insert DataFrame to PostgreSQL using psycopg2 with verification
    #     """
    #     import psycopg2
    #     from psycopg2.extras import execute_values

    #     # Print what we're trying to insert
    #     print("\nAttempting to insert DataFrame:")
    #     print(df.head())
    #     print("\nDataFrame shape:", df.shape)

    #     db_params = {
    #         'dbname': self.database,
    #         'user': self.username,
    #         'password': self.password,
    #         'host': self.host,
    #         'port': self.port
    #     }

    #     # Get the column names from your dataframe
    #     columns = df.columns.tolist()
    #     print("\nColumns being inserted:", columns)

    #     # Convert dataframe to values list
    #     values = [tuple(x) for x in df.to_numpy()]
    #     print("\nFirst row values:", values[0] if values else "No values")

    #     # Generate the INSERT statement
    #     insert_query = f"INSERT INTO {schema}.{table_name} ({','.join(columns)}) VALUES %s"
    #     print("\nInsert query:", insert_query)

    #     try:
    #         # Connect and insert
    #         with psycopg2.connect(**db_params) as conn:
    #             with conn.cursor() as cur:
    #                 # First, let's check if the table exists and its structure
    #                 cur.execute(f"""
    #                     SELECT column_name, data_type
    #                     FROM information_schema.columns
    #                     WHERE table_schema = '{schema}'
    #                     AND table_name = '{table_name}'
    #                 """)
    #                 table_structure = cur.fetchall()
    #                 print("\nTable structure:", table_structure)

    #                 # Perform the insert
    #                 execute_values(cur, insert_query, values)

    #                 # Verify the insertion
    #                 cur.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}")
    #                 count_after = cur.fetchone()[0]

    #                 # Get the last inserted row
    #                 cur.execute(f"SELECT * FROM {schema}.{table_name} ORDER BY exchange_id DESC LIMIT 1")
    #                 last_row = cur.fetchone()

    #             conn.commit()

    #         print(f"\nTotal rows in table after insertion: {count_after}")
    #         print(f"Last row in table: {last_row}")
    #         return True

    #     except Exception as e:
    #         print(f"\nError inserting data: {str(e)}")
    #         return False