import os
# import asyncio
from dotenv import load_dotenv
from exchanges.binance_data import BinanceETL
from db_bridges.sql_con import SQLManager
from db_bridges.psql_con import PSQLManager

# from ccxt_data import CCXTETL


# Load environment variables from .env file
load_dotenv()




def main():
    # Load the crypto class and the crypto
    crypto = BinanceETL()
    df_crypto = crypto.get_ohlcv_clean()
  
    
    # Load the SQL manager class for database operations
    sql = SQLManager()

    # Read the ticker lookup table from the database and convert it into a DataFrame
    df_ticker_table = sql.sql_to_df(query = 'SELECT * FROM public.vw_ticker_lookup')


    # Convert the exchange lookup table from the database and convert it into a DataFrame
    df_exchange_table = sql.sql_to_df(query = 'SELECT * FROM public.vw_exchange_lookup')
    
  

    # Merge the Dataframes together 
    df_ohlcv_ticker = sql.wrangle_df(df1 = df_crypto, col1='ticker', df2=df_ticker_table, col2='ticker_symbol',join='inner')
    columns = ['ticker_id', 'exchange_id', 'date', 'open', 'high', 'low', 'close', 'volume']
    df_ohlcv_ticker_exchange = sql.wrangle_df(df1 = df_ohlcv_ticker, col1='exchange', df2=df_exchange_table, col2='exchange_name', columns=columns)
    print(df_ohlcv_ticker_exchange)
  


    sql.insert_df_to_table(df=df_ohlcv_ticker_exchange,  table_name='exchange',schema='crypto')

    

def main(): 
    import pandas as pd
    data = {
            'exchange_name': ['Coinbase'],
            'active' : [True]
                    }
    df = pd.DataFrame(data)

    print(df)
    sql = SQLManager()
    df = sql.sql_to_df(table_name='exchanges',schema='public')
    print(df)
    
    sql.df_to_sql(df=df, table_name='exchange',schema='public')

    df = sql.sql_to_df(table_name='exchanges',schema='public')
    print(df)
    
 

    



if __name__ == "__main__":
    main()

