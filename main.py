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
    df_tickers = sql.sql_to_df(query = 'SELECT * FROM public.vw_ticker_lookup')


    # Convert the exchange lookup table from the database and convert it into a DataFrame
    df_exchanges = sql.sql_to_df(query = 'SELECT * FROM public.vw_exchange_lookup')
    
  


  


    sql.insert_df_to_table(df=df_ohlcv_ticker_exchange,  table_name='exchange',schema='crypto')

    

def main(): 
    import pandas as pd
    ass = {'asset_type_name' : ['Stock Options'] }
    exc = { 'exchange_name': ['Gemini'] ,
            'active': [True] }
    tick = {'ticker_symbol': ['ETH/USDT'],
            'ticker_name': ['Ethereum'],
            'asset_type_id': [1] ,
            'exchange_id': [1], 
            'trading' : [True] }
    df_ass = pd.DataFrame(ass)
    # print(df_ass)
    df_exc = pd.DataFrame(exc)
    # print(df_exc)
    df_tick = pd.DataFrame(tick)
    
    print(df_tick)

    
    sql = SQLManager()
    
    
    # Asset Types (working)
    # sql.insert_df_to_sql(df=df_ass, table_name='asset_types',schema='public')
    # sql.read_sql_to_df(table_name='asset_types',schema='public')
    




   # Exchanges (workign now )
    # sql.insert_df_to_sql(df=df_exc, table_name='exchanges',schema='public')
    # sql.read_sql_to_df(table_name='exchanges',schema='public')
   
    
   
    
    # Tickers (Working now )
    sql.insert_df_to_sql(df=df_tick, table_name='tickers', schema='public')
    sql.read_sql_to_df( table_name='tickers', schema='public')


    
 

    



if __name__ == "__main__":
    main()

