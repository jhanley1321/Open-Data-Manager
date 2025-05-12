
import os
from datetime import datetime, timedelta
import pandas as pd
from binance import Client
from typing import Optional
from dotenv import load_dotenv

class BinanceExtractor:


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
        print("Initializing BinanceExtractor")
        self.df_ohlcv = None
        
       
        # Load environment variables if not provided
        if api_key is None or api_secret is None:
            load_dotenv()
            api_key = os.getenv('BINANCE_API_KEY')
            api_secret = os.getenv('BINANCE_SECRET_KEY')

        self.load_binance(api_key, api_secret, tld)
    
    @property
    def df(self):
        """Getter for the DataFrame"""
        return self.df_ohlcv

    @df.setter
    def df(self, value):
        """Setter for the DataFrame"""
        self.df_ohlcv = value
        
    def load_binance(self,
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
                      start_date: str = '5 years ago UTC' ) -> Optional[pd.DataFrame]:
        """
        Retrieves historical price data for a cryptocurrency from Binance.

        Args:
            ticker (str): The trading pair symbol (e.g., 'BTCUSDT'). Defaults to 'BTCUSDT'.
            interval (str): The candlestick interval (e.g., '1d', '1h', '15m'). Defaults to '1d'.
            start_date (str): The start date for historical data. Defaults to '5 years ago UTC'.
       

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

           
            self.df_ohlcv = df

            print(f'Binance data retrieved for {ticker} at {interval} interval')
            print(df.head(3))
            return df

        except Exception as e:
            print(f"Error retrieving data: {str(e)}")
            return None

