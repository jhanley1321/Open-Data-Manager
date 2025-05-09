# WIP
import os
import asyncpg
from typing import Optional, List, Any
import asyncio


class PSQLManager:
    def __init__(self, host: str = 'localhost', port: int = 5432, database: str = 'postgres', username: str = 'postgres', password_env_var: str = 'POSTGRESQL_PASSWORD'):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = os.getenv(password_env_var)
        self.connection_url = f'postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}'
        self.pool = None

        asyncio.run(self.load())

    async def load(self) -> None:
        self.pool = await asyncpg.create_pool(dsn=self.connection_url)
        print("Connection successful!")

    async def query(self, query: str = 'SELECT * FROM exchanges', **kwargs: Any) -> List[asyncpg.Record]:
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                result = await connection.fetch(query)
                return result

    # Additional methods can be added here, such as read_df, insert, etc.