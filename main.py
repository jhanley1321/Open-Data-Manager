from dotenv import load_dotenv
from data_manager import DataManager



# Load environment variables from .env file
load_dotenv()




def main():
    # Load the crypto class and the crypto
    crypto = DataManager()
    # crypto.get_ohlcv() # temp
    # crypto.clean_ohlcv() # temp
    # print(crypto.df_ohlcv)
    # crypto.get_ohlcv_clean() # meeds fixing 
   
   
   
  
    
    # # Load the SQL manager class for database operations
    # sql = SQLManager()

    # Read the ticker lookup table from the database and convert it into a DataFrame
  
    crypto.read_sql_to_df(table_name='vw_exchange_ticker_asset_lookup', schema='public')
    # crypto.wrangle_ohlcv()
    # crypto.insert_df_to_sql() # off for resting right now
    


    

    # crypto.run()
  

  




    
# Saving for later 
# def main(): 
#     import pandas as pd
#     ass = {'asset_type_name' : ['Stock Options'] }
#     exc = { 'exchange_name': ['Gemini'] ,
#             'active': [True] }
#     tick = {'ticker_symbol': ['WBTC/BTC'],
#             'ticker_name': ['Wrapped Bitcoin'],
#             'asset_type_id': [1] ,
#             'exchange_id': [1], 
#             'trading' : [True] }
#     df_ass = pd.DataFrame(ass)
#     # print(df_ass)
#     df_exc = pd.DataFrame(exc)
#     # print(df_exc)
#     df_tick = pd.DataFrame(tick)
    
#     print(df_tick)

    
#     sql = SQLManager()
    
    
#     # Asset Types (working)
#     # sql.insert_df_to_sql(df=df_ass, table_name='asset_types',schema='public')
#     # sql.read_sql_to_df(table_name='asset_types',schema='public')
    




#    # Exchanges (workign now )
#     # sql.insert_df_to_sql(df=df_exc, table_name='exchanges',schema='public')
#     # sql.read_sql_to_df(table_name='exchanges',schema='public')
   
    
   
    
#     # Tickers (Working now )
#     sql.insert_df_to_sql(df=df_tick, table_name='tickers', schema='public')
#     sql.read_sql_to_df( table_name='tickers', schema='public')


    
 

    



if __name__ == "__main__":
    main()

