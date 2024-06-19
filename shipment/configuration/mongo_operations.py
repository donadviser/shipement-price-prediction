import sys
from json import loads
from typing import Collection
import pandas as pd
from pymongo.database import Database
from pymongo import MongoClient
from shipment.constant import DB_URL
from shipment.exception import ShipmentException
from shipment.logger import logging


class MongoDBOperation:
    def __init__(self):
        self.DB_URL = DB_URL
        self.client = MongoClient(self.DB_URL)


    def get_database(self, db_name) -> Database:
        """
        Get the database object from the database name.

        Args:
            db_name (str): The name of the database.

        Returns:
            Database: The database object.
        """
        logging.info("Entered the get_database method of MongoDBOperation class.")
        try:
            db = self.client[db_name]
            logging.info(f"Successfully fetched the {db_name} database in MongoDB.")
            logging.info("Exited the get_database method of MongoDBOperation class.")
            return db
        except Exception as e:
            raise ShipmentException(e, sys)
        
    @staticmethod
    def get_collection(database: Database, collection_name: str) -> Collection:
        """
        Get the collection object from the database and collection name.

        Args:
            database (Database): The database object.
            collection_name (str): The name of the collection.

        Returns:
            Collection: The collection object.
        """
        logging.info("Entered the get_collection method of MongoDBOperation class.")
        try:
            collection = database[collection_name]
            logging.info(f"Successfully fetched the {collection_name} collection in MongoDB.")
            logging.info("Exited the get_collection method of MongoDBOperation class.")
            return collection
        except Exception as e:
            raise ShipmentException(e, sys)
        
    def get_collection_as_dataframe(self, db_name: str, collection_name: str) -> pd.DataFrame:
        """
        Get the collection object from the database and collection name.

        Args:
            db_name (str): The name of database object.
            collection_name (str): The name of the collection.

        Returns:
            Collection: The collection object.
        """
        logging.info("Entered the get_collection_as_dataframe method of MongoDBOperation class.")
        try:
            #Getting the database
            database = self.get_database(db_name=db_name)

            # Get the collection object.
            collection = self.get_collection(database=database, collection_name=collection_name)

            #Reading the dataframe and dropping the _id column
            df = pd.DataFrame(list(collection.find()))
            if "_id" in df.columns.to_list():
                df.drop(columns =["_id"], axis=1)

            logging.info(f"Successfully fetched the {collection_name} collection in MongoDB.")
            logging.info("Exited the get_collection_as_dataframe method of MongoDBOperation class.")
            return df
        except Exception as e:
            raise ShipmentException(e, sys)
        

    def insert_dataframe_as_record(self, data_frame: pd.DataFrame, db_name: str, collection_name: str) -> None:
        """
        Insert the dataframe as a record in the collection.

        Args:
            data_frame (pd.DataFrame): The dataframe to be inserted.
            db_name (str): The name of the database.
            collection_name (str): The name of the collection.
        """
        logging.info("Entered the insert_dataframe_as_record method of MongoDBOperation class.")
        try:
            # Converting dataframe into json
            records = loads(data_frame.T.to_json()).values()
            logging.info("Converted dataframe to json records")

            # Getting the database
            database = self.get_database(db_name=db_name)

            # Get the collection object.
            collection = self.get_collection(database=database, collection_name=collection_name)

            # Inserting the dataframe as a record in the collection.
            logging.info("Inserting recording to MongoDB")
            collection.insert_many(records)
            logging.info(f"Successfully inserted the {collection_name} collection in MongoDB.")
            logging.info("Exited the insert_dataframe_as_record method of MongoDBOperation class.")
        except Exception as e:
            raise ShipmentException(e, sys)

