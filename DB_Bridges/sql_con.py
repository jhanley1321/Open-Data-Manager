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
  

    def sql_to_df(self, table_name, schema=None, **kwargs):
        with self.engine.connect() as connection:
            self.df = pd.read_sql_table(table_name, con=connection, schema=schema)
        return self.df





    # Move and refactor later 

    def wrangle_df(self, df1: pd.DataFrame, col1: str, df2: pd.DataFrame, col2: str, join: str = 'inner', csv: Optional[str] = None, columns: Optional[list] = None) -> pd.DataFrame:
        """
        Updates the values in df1 based on the matching values in df2 by merging the DataFrames.

        Args:
        df1 (pd.DataFrame): The DataFrame to be updated.
        col1 (str): The column in df1 that needs to be updated.
        df2 (pd.DataFrame): The DataFrame containing the values to swap with.
        col2 (str): The column in df2 that contains the values to swap with.
        join (str): The type of join to perform (default is 'inner').
        csv (Optional[str]): The name of the CSV file to write the resulting DataFrame to. If None, no file is written.
        columns (Optional[list]): The list of columns to include in the final DataFrame. If None, all columns are included.

        Returns:
        pd.DataFrame: The updated DataFrame.
        """
        # Ensure the specified columns exist in the DataFrames
        if col1 not in df1.columns:
            raise KeyError(f"Column '{col1}' not found in df1")
        if col2 not in df2.columns:
            raise KeyError(f"Column '{col2}' not found in df2")

        # Print the columns of both DataFrames for debugging
        print("Columns in df1:", df1.columns)
        print("Columns in df2:", df2.columns)

        # Ensure the data types of the merge columns are the same
        df1[col1] = df1[col1].astype(str)
        df2[col2] = df2[col2].astype(str)

        # Merge the DataFrames on the specified key with a left join
        merged_df = pd.merge(df1, df2, left_on=col1, right_on=col2, how=join)

        # Drop the key column from the merged DataFrame to avoid duplication
        if col2 != col1:
            merged_df.drop(columns=[col2], inplace=True)

        # Ensure the 'date' column is in the proper date format
        if 'date' in merged_df.columns:
            merged_df['date'] = pd.to_datetime(merged_df['date']).dt.date

        # Select and order the columns if specified
        if columns:
            merged_df = merged_df[columns]

        # Print the resulting DataFrame for debugging
        print("Merged DataFrame:")
        print(merged_df)

        # Write the resulting DataFrame to a CSV file if a filename is provided
        if csv:
            data_folder = os.path.join(os.getcwd(), 'Data')
            os.makedirs(data_folder, exist_ok=True)
            csv_path = os.path.join(data_folder, csv)
            merged_df.to_csv(csv_path, index=False)
            print(f"DataFrame written to {csv_path}")

        return merged_df




    def df_to_sql(self, df=None, index = False,  schema='crypto', table_name='ohlcv_daily', if_exists='append', **kwargs):
        if df is None:
            print("No data frame seleceted")
            

        df.to_sql(table_name, schema=schema, con=self.engine, if_exists=if_exists, index=index)
        print("Data inserted successfully.")


    def sql_insert(self, table_name, query,**kwargs):
    
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=self.engine)
        stmt = insert(table).values(query)
        with self.engine.connect() as connection:
            connection.execute(stmt)
            connection.commit() # ensures the query is commited 
            print(f"Inserted data into {table_name}: {query}")



    def df_to_sql(self, df=None, index=False, schema='crypto', table_name='ohlcv_daily', if_exists='append', **kwargs):
        if df is None or df.empty:
            print("No data frame selected or DataFrame is empty")
            return

        try:
            # Ensure the schema and table name are correct
            full_table_name = f"{schema}.{table_name}" if schema else table_name
            print(f"Inserting data into table: {full_table_name}")

            # Insert the DataFrame into the database
            df.to_sql(table_name, schema=schema, con=self.engine, if_exists=if_exists, index=index)
            print("Data inserted successfully.")
        except Exception as e:
            print(f"Error inserting data into table {full_table_name}: {str(e)}")