from pymongo import MongoClient
from typing import Dict, List, Any
import pandas as pd

class MongoDBReader:
    def __init__(self, database_name: str, collection_name: str, host: str = 'localhost', port: int = 27017):
        self.client = MongoClient(host, port)
        self.db = self.client[database_name]
        self.collection = self.db[collection_name]

    def find_one(self, query: Dict) -> Dict:
        """Find a single document (returns as dict)"""
        return self.collection.find_one(query)

    def find_many(self, query: Dict) -> List[Dict[str, Any]]:
        """Find multiple documents (returns as list of dicts)"""
        return list(self.collection.find(query))

    def find_many_df(self, query: Dict) -> pd.DataFrame:
        """Find multiple documents and return as a pandas DataFrame"""
        docs = self.find_many(query)
        if docs:
            # Optionally remove the MongoDB '_id' field if you don't want it in your DataFrame
            for doc in docs:
                doc.pop('_id', None)
            return pd.DataFrame(docs)
        else:
            return pd.DataFrame()

    def close(self):
        """Close the connection"""
        self.client.close()

# Example usage
if __name__ == "__main__":
    reader = MongoDBReader(database_name="local", collection_name="Orders")

    # Get as list of dicts
    docs = reader.find_many({})
    print("As list of dicts:\n", docs)

    # Get as DataFrame
    df = reader.find_many_df({})
    print("\nAs DataFrame:\n", df)

    reader.close()