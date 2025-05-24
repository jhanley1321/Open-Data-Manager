from etl.binance_extract import BinanceExtractor
from etl.binance_transform import BinanceTransform
from sql.sql_load import SQLLoader



class DataManager:
    """
    A class to manage the ETL process for cryptocurrency data.

    This class coordinates between:
    - BinanceExtractor for data extraction
    - BinanceTransform for data transformation
    - SQLLoader for data loading

    Attributes:
        df_ohlcv (pd.DataFrame): Main DataFrame for OHLCV data
        extractor (BinanceExtractor): Instance of BinanceExtractor
        transform (BinanceTransform): Instance of BinanceTransform
        loader (SQLLoader): Instance of SQLLoader
    """

    def __init__(self,
                 api_key: str = None,
                 api_secret: str = None,
                 tld: str = 'us',
                 host: str = 'localhost',
                 port: int = 5432,
                 database: str = 'postgres',
                 username: str = 'postgres',
                 password_env_var: str = 'POSTGRESQL_PASSWORD'):
        """
        Initialize DataManager with its component classes.

        Args:
            api_key (str, optional): Binance API key. Defaults to None.
            api_secret (str, optional): Binance API secret. Defaults to None.
            tld (str, optional): Top-level domain for Binance. Defaults to 'us'.
            host (str, optional): Database host. Defaults to 'localhost'.
            port (int, optional): Database port. Defaults to 5432.
            database (str, optional): Database name. Defaults to 'postgres'.
            username (str, optional): Database username. Defaults to 'postgres'.
            password_env_var (str, optional): Environment variable for DB password. Defaults to 'POSTGRESQL_PASSWORD'.
        """
        # Initialize the DataFrames as  attributes
        self.df_ohlcv = None
        self.df_sql = None
        self.df_ohlcv_wrangled = None

        # Initialize components with self as manager for extractor
        self.extractor = BinanceExtractor(
            api_key=api_key,
            api_secret=api_secret,
            tld=tld,
            manager=self
        )
        self.transform = BinanceTransform(manager=self)
        self.loader = SQLLoader(
            host=host,
            port=port,
            database=database,
            username=username,
            password_env_var=password_env_var,
            manager=self
        )

    def __getattr__(self, name):
        """
        Delegate any undefined attributes/methods to extractor, transform, or loader instance.

        Args:
            name (str): Name of the attribute/method being accessed

        Returns:
            Any: The requested attribute/method from the appropriate component

        Raises:
            AttributeError: If the attribute/method is not found in any component
        """
        if hasattr(self.extractor, name):
            return getattr(self.extractor, name)
        elif hasattr(self.transform, name):
            return getattr(self.transform, name)
        elif hasattr(self.loader, name):
            return getattr(self.loader, name)
        raise AttributeError(f"Method '{name}' not found in any component class")