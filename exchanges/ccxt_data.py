# WIP
import os
from datetime import datetime, timedelta
import pandas as pd
import ccxt
from typing import Optional

class CCXTETL:
    """
    A class to handle ETL (Extract, Transform, Load) operations for cryptocurrency data using CCXT.

    This class provides functionality to:
    - Connect to the Binance API via CCXT
    - Retrieve historical cryptocurrency price data
    - Clean and format the data
    - Save the data to CSV files

    Attributes:
    api_key (str): Binance API key
    api_secret (str): Binance API secret
    exchange (ccxt.Exchange): CCXT exchange instance
    coin_df (pd.DataFrame): Raw cryptocurrency price data
    cleaned_df (pd.DataFrame): Cleaned cryptocurrency price data
    """

    def __init__(self,
                 api_key: str = os.getenv('BINANCE_API_KEY'),
                 api_secret: str = os.getenv('BINANCE_SECRET_KEY'),
                 exchange_id: str = 'binanceus'):
        """
        Initialize the CCXTETL instance and establish connection to the exchange via CCXT.

        Args:
        api_key (str): The Binance API key. Defaults to BINANCE_API_KEY environment variable.
        api_secret (str): The Binance API secret. Defaults to BINANCE_SECRET_KEY environment variable.
        exchange_id (str): The ID of the exchange to connect to. Defaults to 'binance'.

        Raises:
        EnvironmentError: If API credentials are not provided and not found in environment variables.
        ConnectionError: If connection to the exchange fails.
        """
        self.build_exchange(api_key, api_secret, exchange_id)

    def build_exchange(self,
                       api_key: str = os.getenv('BINANCE_API_KEY'),
                       api_secret: str = os.getenv('BINANCE_SECRET_KEY'),
                       exchange_id: str = 'binanceus') -> None:
        """
        Initializes the CCXT exchange client using API credentials.

        Args:
        api_key (str): The Binance API key. Defaults to BINANCE_API_KEY environment variable.
        api_secret (str): The Binance API secret. Defaults to BINANCE_SECRET_KEY environment variable.
        exchange_id (str): The ID of the exchange to connect to. Defaults to 'binance'.

        Raises:
        EnvironmentError: If API credentials are not provided and not found in environment variables.
        ConnectionError: If connection to the exchange fails.
        """
        self.api_key = api_key
        self.api_secret = api_secret

        # Validate credentials
        if not api_key or not api_secret:
            raise EnvironmentError("API credentials not found. Please set Binance US API key and secret key environment variables or provide them as arguments.")

        # Initialize exchange
        self.exchange = getattr(ccxt, exchange_id)({
            'apiKey': api_key,
            'secret': api_secret,
        })

        # Test connection
        try:
            self.exchange.load_markets()
            print(f'Connected to Binance US API')
        except Exception as e:
            raise ConnectionError(f"Failed to connect to {exchange_id} API: {str(e)}")

    def get_ohlcv(self,
                  ticker: str = 'BTC/USDT',
                  interval: str = '1d',
                  start_date: str = '5 years ago UTC',
                  save_as_attribute: bool = True) -> Optional[pd.DataFrame]:
        """
        Retrieves historical price data for a cryptocurrency from the exchange.

        Args:
        ticker (str): The trading pair symbol (e.g., 'BTC/USDT'). Defaults to 'BTC/USDT'.
        interval (str): The candlestick interval (e.g., '1d', '1h', '15m'). Defaults to '1d'.
        start_date (str): The start date for historical data. Defaults to '5 years ago UTC'.
        save_as_attribute (bool): Whether to save the DataFrame as an attribute. Defaults to True.

        Returns:
        Optional[pd.DataFrame]: DataFrame containing historical price data, or None if no data is retrieved.

        Raises:
        AttributeError: If called before establishing a connection with build_exchange().
        ValueError: If the provided ticker or interval is invalid.
        """
        try:
            # Convert start_date to timestamp
            since = int(datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S').timestamp() * 1000)

            # Retrieve historical price data
            klines = self.exchange.fetch_ohlcv(ticker, interval, since=since)

            # Check if data was returned
            if not klines:
                print("No data returned. Please check the ticker, interval, and start_date.")
                return None

            # Convert to DataFrame
            df = pd.DataFrame(klines, columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume'
            ])

            # Convert numeric columns to float
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = df[col].astype(float)

            # Convert timestamp to datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

            # Set timestamp as index
            df.set_index('timestamp', inplace=True)

            # Optionally store as an attribute
            if save_as_attribute:
                self.coin_df = df

            print(f'Exchange data retrieved for {ticker} at {interval} interval')
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
        df["ticker"] = ticker
        df["exchange"] = "Binance US"
        df["trade"] = 1

        # Reset index
        df.reset_index(drop=True, inplace=True)

        # Store as attribute
        self.cleaned_df = df
        print('Exchange Data Cleaned')
        print(df.head(3))
        print(df.tail(3))

        return df

    def get_ohlcv_clean(self,
                        ticker: str = 'BTC/USDT',
                        interval: str = '1d',
                        start_date: str = '5 years ago UTC',
                        price_only: Optional[bool] = None,
                        csv: Optional[str] = None,
                        remove_last_n: int = 0) -> Optional[pd.DataFrame]:
        """
        Complete ETL process for cryptocurrency price data: retrieves and cleans data.

        Args:
        ticker (str): The trading pair symbol (e.g., 'BTC/USDT'). Defaults to 'BTC/USDT'.
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

        print(f'ETL process completed for {ticker} at {interval} interval')
        return cleaned_df