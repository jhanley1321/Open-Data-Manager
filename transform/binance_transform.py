
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
        df["ticker"] = "BTC/USDT"
        df["exchange"] = "Binance"
        

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
        raw_df = self.get_ohlcv(
            ticker=ticker,
            interval=interval,
            start_date=start_date,
            save_as_attribute=True
        )

        # If data retrieval failed, return None
        if raw_df is None:
            print(f"Failed to retrieve data for {ticker}")
            return None

        # Clean the data
        cleaned_df = self.clean_ohlcv(
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

    def wrangle_ohlcv(self, df1: pd.DataFrame, col1: str, df2: pd.DataFrame, col2: str, join: str = 'inner', csv: Optional[str] = None) -> pd.DataFrame:
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
        columns = ['ticker_id', 'exchange_id', 'date', 'open', 'high', 'low', 'close', 'volume']
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
