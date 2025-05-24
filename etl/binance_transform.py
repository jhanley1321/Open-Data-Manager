
import os
from datetime import datetime, timedelta
import pandas as pd
from binance import Client
from typing import Optional



class BinanceTransform():
    
    def __init__(self, manager=None):
        self.manager = manager
        print("BinanceTransform initialized")
        
        
    
    
    def clean_ohlcv(self,
                        price: Optional[bool] = None,
                        df: Optional[pd.DataFrame] = None,
                        remove_last_n: int = 0) -> pd.DataFrame:
        """
        Cleans and formats cryptocurrency price data.

        Args:
            price (Optional[bool]): If True, returns only date and close price columns.
                                   If None, returns all OHLCV columns. Defaults to None.
            df (Optional[pd.DataFrame]): The DataFrame to clean. If None, tries to use self.df_ohlcv or self.df.
                                        Defaults to None.
            csv (Optional[str]): If provided, saves the cleaned DataFrame to a CSV file with this name.
                                Defaults to None.
            remove_last_n (int): Number of rows to remove from the end of the DataFrame.
                                If 0, no rows are removed. Defaults to 0.

        Returns:
            pd.DataFrame: The cleaned DataFrame.

        Raises:
            ValueError: If no DataFrame is provided and no DataFrame is stored as an attribute.
        """
        print("Cleaning OHLCV data...")
        # Get the DataFrame to work with
        # Remove this check and require a dataframe 
        if self.manager is None or self.manager.df_ohlcv is None:
            raise ValueError("No data available. Run get_ohlcv first.")

        
        # Get the DataFrame from the manager if not provided
        if df is None:
            df = self.manager.df_ohlcv
        else:
            raise ValueError("No DataFrame provided and no DataFrame stored as attribute (df_ohlcv or df)")

        # Create a copy to avoid modifying the original
        df = df.copy()

        # Remove the last n rows if specified
        if remove_last_n > 0:
            if len(df) > remove_last_n:
                df = df.iloc[:-remove_last_n]
                print(f"Removed last {remove_last_n} rows from the DataFrame")
            else:
                print(f"Warning: Cannot remove {remove_last_n} rows as DataFrame only has {len(df)} rows")

        # Add date column from index
        df['date'] = df.index

        # Select and reorder columns based on price parameter
        if price is not None:
            new_order = ['date', 'close']
            df = df[new_order]
        else:
            new_order = ['date', 'open', 'high', 'low', 'close', 'volume']
            df = df[new_order]

        # Add identifying columns
        df["ticker_symbol"] = "BTC/USDT" # make attribue
        df["exchange_name"] = "Binance" # make attritube
        

        # Reset index
        df.reset_index(drop=True, inplace=True)

        # Store as attribute
        self.manager.df_ohlcv = df
        print('Binance Data Cleaned')
        return df


    def get_ohlcv_clean(self,
                    ticker: str = 'BTCUSDT',
                    interval: str = '1d',
                    start_date: str = '5 years ago UTC',
                    price_only: Optional[bool] = None,
                    csv: Optional[str] = None,
                    remove_last_n: int = 0) -> Optional[pd.DataFrame]:
        """
        Complete ETL process for cryptocurrency price data: retrieves and cleans data.

        Args:
            ticker (str): The trading pair symbol (e.g., 'BTCUSDT'). Defaults to 'BTCUSDT'.
            interval (str): The candlestick interval (e.g., '1d', '1h', '15m'). Defaults to '1d'.
            start_date (str): The start date for historical data. Defaults to '5 years ago UTC'.
            price_only (Optional[bool]): If True, returns only date and close price columns.
                                        If None, returns all OHLCV columns. Defaults to None.
            csv (Optional[str]): If provided, saves the cleaned DataFrame to a CSV file with this name.
                                Defaults to None.
            remove_last_n (int): Number of rows to remove from the end of the DataFrame.
                                If 0, no rows are removed. Defaults to 0.

        Returns:
            Optional[pd.DataFrame]: Cleaned DataFrame containing historical price data, or None if process fails.

        Raises:
            ValueError: If the provided ticker or interval is invalid.
        """
        # Get the coin price data
        # May need to update 
        raw_df = self.manager.get_ohlcv(
            ticker=ticker,
            interval=interval,
            start_date=start_date
        )

        # If data retrieval failed, return None
        if raw_df is None:
            print(f"Failed to retrieve data for {ticker}")
            return None

        # Clean the data
        cleaned_df = self.manager.clean_ohlcv(
            price=price_only,
            df=raw_df,
            remove_last_n=remove_last_n
        )

        # Save to CSV if requested
        if csv is not None:
            # Ensure the 'Data' directory exists
            os.makedirs('Data', exist_ok=True)
            # Replace forward slashes with underscores in the filename
            safe_filename = csv.replace('/', '_')
            cleaned_df.to_csv(os.path.join('Data', f'{safe_filename}.csv'), index=False)

        print(f'OHLCV data for {ticker} at {interval} interval has been processed.')
        return cleaned_df


    def wrangle_ohlcv(self, df_ohlcv=None, col_ohlcv=None, df_sql=None, col_sql=None, join='inner'):
        
        if df_ohlcv is None:
            df_ohlcv = self.manager.df_ohlcv
           
        if col_ohlcv is None:
            col_ohlcv = ['ticker_symbol', 'exchange_name']
      

        if df_sql is None:
            df_sql = self.manager.df_sql
         

        if col_sql is None:
            col_sql = ['ticker_symbol', 'exchange_name']  # ['exchange_name', 'ticker_symbol']
           
        # print(df_sql.head())
        # print(df_ohlcv.head())

 
        # For multiple columns, pass lists of column names
        print('Merging DataFrames...')
        df_merged = pd.merge(
            left=df_ohlcv,
            right=df_sql,
            left_on=[col_ohlcv[0], col_ohlcv[1]],  # list of columns from left dataframe
            right_on=[col_sql[0], col_sql[1]],  # list of columns from right dataframe
            how=join
        )


        # Select and order the columns if specified
        columns = ['ticker_id', 'exchange_id', 'date', 'open', 'high', 'low', 'close', 'volume']
        df_merged = df_merged[columns]
        
        # Free up memoery 
        self.df_ohlcv = None
        self.df_sql = None


        self.manager.df_ohlcv_wrangled = df_merged

        print(df_merged)
   

