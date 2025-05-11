import os
from datetime import datetime, timedelta
import pandas as pd
from binance import Client
from typing import Optional
from dotenv import load_dotenv

class BinanceETL:
    """
    A class to handle ETL (Extract, Transform, Load) operations for Binance cryptocurrency data.

    This class provides functionality to:
    - Connect to the Binance API
    - Retrieve historical cryptocurrency price data
    - Clean and format the data
    - Save the data to CSV files

    Attributes:
        api_key (str): Binance API key
        api_secret (str): Binance API secret
        client (Client): Binance API client instance
        coin_df (pd.DataFrame): Raw cryptocurrency price data
        cleaned_df (pd.DataFrame): Cleaned cryptocurrency price data
    """

    def __init__(self,
                 api_key: str = None,
                 api_secret: str = None,
                 tld: str = 'us'):
        """
        Initialize the BinanceETL instance and establish connection to Binance API.

        Args:
            api_key (str): The Binance API key. Defaults to BINANCE_API_KEY environment variable.
            api_secret (str): The Binance API secret. Defaults to BINANCE_SECRET_KEY environment variable.
            tld (str): Top-level domain for the Binance API. Defaults to 'us' for Binance US.

        Raises:
            EnvironmentError: If API credentials are not provided and not found in environment variables.
            ConnectionError: If connection to the Binance API fails.
        """
        # Load environment variables if not provided
        if api_key is None or api_secret is None:
            load_dotenv()
            api_key = os.getenv('BINANCE_API_KEY')
            api_secret = os.getenv('BINANCE_SECRET_KEY')

        self.build_binance(api_key, api_secret, tld)

    def build_binance(self,
                     api_key: str,
                     api_secret: str,
                     tld: str = 'us') -> None:
        """
        Initializes the Binance API client using API credentials.

        Args:
            api_key (str): The Binance API key. Defaults to BINANCE_API_KEY environment variable.
            api_secret (str): The Binance API secret. Defaults to BINANCE_SECRET_KEY environment variable.
            tld (str): Top-level domain for the Binance API. Defaults to 'us' for Binance US.

        Raises:
            EnvironmentError: If API credentials are not provided and not found in environment variables.
            ConnectionError: If connection to the Binance API fails.
        """
        self.api_key = api_key
        self.api_secret = api_secret

        # Validate credentials
        if not api_key or not api_secret:
            raise EnvironmentError("Binance API credentials not found. Please set BINANCE_API_KEY and BINANCE_SECRET_KEY environment variables or provide them as arguments.")

        # Initialize client
        self.client = Client(api_key=api_key, api_secret=api_secret, tld=tld)

        # Test connection
        try:
            self.client.ping()
            print('Connected to Binance API')
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Binance API: {str(e)}")

    def get_ohlcv(self,
                      ticker: str = 'BTCUSDT',
                      interval: str = '1d',
                      start_date: str = '5 years ago UTC',
                      save_as_attribute: bool = True) -> Optional[pd.DataFrame]:
        """
        Retrieves historical price data for a cryptocurrency from Binance.

        Args:
            ticker (str): The trading pair symbol (e.g., 'BTCUSDT'). Defaults to 'BTCUSDT'.
            interval (str): The candlestick interval (e.g., '1d', '1h', '15m'). Defaults to '1d'.
            start_date (str): The start date for historical data. Defaults to '5 years ago UTC'.
            save_as_attribute (bool): Whether to save the DataFrame as an attribute. Defaults to True.

        Returns:
            Optional[pd.DataFrame]: DataFrame containing historical price data, or None if no data is retrieved.

        Raises:
            AttributeError: If called before establishing a connection with build_binance().
            ValueError: If the provided ticker or interval is invalid.
        """
        try:
            # Retrieve historical price data
            klines = self.client.get_historical_klines(ticker, interval, start_date)

            # Check if data was returned
            if not klines:
                print("No data returned. Please check the ticker, interval, and start_date.")
                return None

            # Convert to DataFrame
            df = pd.DataFrame(klines, columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_asset_volume', 'number_of_trades',
                'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
            ])

            # Convert numeric columns to float
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = df[col].astype(float)

            # Convert timestamp to datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

            # Set timestamp as index
            df.set_index('timestamp', inplace=True)

            # Drop unused columns
            df.drop(columns=[
                'close_time', 'quote_asset_volume', 'number_of_trades',
                'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
            ], inplace=True)

            # Optionally store as an attribute
            if save_as_attribute:
                self.coin_df = df

            print(f'Binance data retrieved for {ticker} at {interval} interval')
            return df

        except Exception as e:
            print(f"Error retrieving data: {str(e)}")
            return None

    def clean_ohlcv(self,
                        price: Optional[bool] = None,
                        df: Optional[pd.DataFrame] = None,
                        remove_last_n: int = 0) -> pd.DataFrame:
        """
        Cleans and formats cryptocurrency price data.

        Args:
            price (Optional[bool]): If True, returns only date and close price columns.
                                   If None, returns all OHLCV columns. Defaults to None.
            df (Optional[pd.DataFrame]): The DataFrame to clean. If None, tries to use self.coin_df or self.df.
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
        # Get the DataFrame to work with
        if df is None:
            if hasattr(self, 'coin_df'):
                df = self.coin_df
            elif hasattr(self, 'df'):
                df = self.df
            else:
                raise ValueError("No DataFrame provided and no DataFrame stored as attribute (coin_df or df)")

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
        self.cleaned_df = df
        print('Binance Data Cleaned')
        print(df.head(3))
        print(df.tail(3))

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
