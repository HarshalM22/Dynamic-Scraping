import pymongo
from pymongo.errors import ConnectionFailure, OperationFailure
from config.settings import SETTINGS
import sys
from typing import Optional

class MongoDBHandler:
    """Handles persistent connection and interaction with MongoDB."""
    def __init__(self):
        self.client = None
        self.db = None
        self.collection = None
        self._connect()

    def _connect(self):
        """Establishes the connection to MongoDB Atlas."""
        print("Attempting to connect to MongoDB...")
        try:
            # Attempt connection with a short timeout
            self.client = pymongo.MongoClient(
                SETTINGS.MONGO_URI, 
                serverSelectionTimeoutMS=5000 
            )
            print("url: ",SETTINGS.MONGO_URI)
            # The ismaster command verifies the connection without requiring auth
            self.client.admin.command('ismaster')
            
            self.db = self.client[SETTINGS.MONGO_DB_NAME]
            self.collection = self.db[SETTINGS.MONGO_COLLECTION]
            print("MongoDB connection **SUCCESSFUL**.")
            
        except ConnectionFailure as e:
            # This is a network/host failure
            print(f"MongoDB Connection ERROR (Network): {e}")
            self.client = None
        except Exception as e:
            # Catches auth errors, configuration errors, etc.
            print(f"MongoDB Connection ERROR (Critical): {e}")
            self.client = None
            
    def is_connected(self) -> bool:
        """Utility to check connection status."""
        return self.client is not None

    def insert_data(self, document: dict) -> Optional[str]:
        """Inserts a single document and returns the object ID string."""
        if self.collection is None:
         return None 
            
        try:
            result = self.collection.insert_one(document)
            # Returns the string representation of the inserted ID
            return str(result.inserted_id) 
            
        except OperationFailure as e:
            # Failure due to write concerns, permissions, etc.
            raise e
        except Exception as e:
            # Catch all other insertion errors
            raise e

# Instantiate the handler once for system-wide use
DB_HANDLER = MongoDBHandler()