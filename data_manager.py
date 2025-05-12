from extract.binance_extract import BinanceExtractor
from transform.binance_transform import BinanceTransform
from load.sql_load import SQLLoader



class DataManager:
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
        Initialize DataManager with BinanceExtractor, BinanceTransform, and SQLLoader instances.
        Uses environment variables for API credentials if none provided.
        """
        self.extractor = BinanceExtractor(api_key=api_key, api_secret=api_secret, tld=tld)
        self.transform = BinanceTransform()
        self.loader = SQLLoader(
            host=host,
            port=port,
            database=database,
            username=username,
            password_env_var=password_env_var
        )

    def __getattr__(self, name):
        """
        Delegate any undefined attributes/methods to extractor, transform, or loader instance.
        Tries each in turn until the method is found.
        """
        if hasattr(self.extractor, name):
            return getattr(self.extractor, name)
        elif hasattr(self.transform, name):
            return getattr(self.transform, name)
        elif hasattr(self.loader, name):
            return getattr(self.loader, name)
        raise AttributeError(f"Method '{name}' not found in any component class")




