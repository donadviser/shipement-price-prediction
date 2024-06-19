import sys
import os
from typing import Tuple
from shipment.logger import logging
from shipment.exception import ShipmentException

import pandas as pd
from sklearn.model_selection import train_test_split
from shipment.configuration.mongo_operations import MongoDBOperation
from shipment.entity.config_entity import DataIngestionConfig
from shipment.entity.artefacts_entity import DataIngestionArtefacts
from shipment.constant import TEST_SIZE


class DataIngestion:
    def __init__(
            self,
            data_ingestion_config: DataIngestionConfig,
            mongo_op: MongoDBOperation
            ):
        self.data_ingestion_config = data_ingestion_config
        self.mongo_op = mongo_op

    # This method will fetch data from mongoDB
    def get_data_from_mongodb(self) -> pd.DataFrame:
        """
        Get the data from mongoDB.

        Returns:
            pd.DataFrame: The data from mongoDB.
        """
        logging.info("Entered the get_data_from_mongodb method of DataIngestion class")
        try:

            db_name = self.data_ingestion_config.DB_NAME
            collection_name = self.data_ingestion_config.COLLECTION_NAME
            df = self.mongo_op.get_collection_as_dataframe(db_name, collection_name)
            logging.info("Obtaied the dataframe from mongodb")
            logging.info("Exited the get_data_from_mongodb method of DataIngestion class")
            return df
        except Exception as e:
            raise ShipmentException(e, sys)
    
    # This method will split the data into train and test
    def split_data_as_train_test(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Split the data into train and test.

        Args:
            df (pd.DataFrame): The dataframe to be split.

        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: The train and test dataframe.
        """
        logging.info("Entered the split_data_as_train_test method of DataIngestion class")
        try:
            # Creating Data Ingestion Artefacts directory inside Artefacts folder            
            os.makedirs(self.data_ingestion_config.DATA_INGESTION_ARTEFACTS_DIR, exist_ok=True)
            
            # Splitting the data into train an test
            train_set, test_set = train_test_split(df, test_size=TEST_SIZE)
            logging.info("Splitted the data into train and test")

            # Creating Train directory inside DataIngestionArtefacts directory
            os.makedirs(self.data_ingestion_config.TRAIN_DATA_ARTEFACT_FILE_DIR, exist_ok=True)
            logging.info(f"Created {os.path.basename(self.data_ingestion_config.TRAIN_DATA_ARTEFACT_FILE_DIR)} directory")

            # Creating Test directory inside DataIngestionArtefacts directory
            os.makedirs(self.data_ingestion_config.TEST_DATA_ARTEFACT_FILE_DIR, exist_ok=True)
            logging.info(f"Created {os.path.basename(self.data_ingestion_config.TEST_DATA_ARTEFACT_FILE_DIR)} directory")

            # Saving the train and test data as csv files
            train_set.to_csv(self.data_ingestion_config.TRAIN_DATA_FILE_PATH, index=False, header=True)
            test_set.to_csv(self.data_ingestion_config.TEST_DATA_FILE_PATH, index=False, header=True)
            logging.info("Saved the train and test data as csv files")
            logging.info(f"Saved {os.path.basename(self.data_ingestion_config.TRAIN_DATA_FILE_PATH)}, \
                          {os.path.basename(self.data_ingestion_config.TEST_DATA_FILE_PATH)} in \
                            {os.path.basename(self.data_ingestion_config.DATA_INGESTION_ARTEFACTS_DIR)} directory")
            logging.info("Exited the split_data_as_train_test method of DataIngestion class")
            return train_set, test_set
        except Exception as e:
            raise ShipmentException(e, sys)
        

    # This method initiates data ingestion
    def initiate_data_ingestion(self) -> DataIngestionArtefacts:
        """
        Initiate the data ingestion.

        Returns:
            DataIngestionArtefacts: The data ingestion artefacts.
        """
        logging.info("Entered the initiate_data_ingestion method of DataIngestion class")
        try:
            # Getting the data from mongoDB
            df = self.get_data_from_mongodb()

            # Dropping the unnecessary columns from the dataframe
            df1 = df.drop(columns=self.data_ingestion_config.DROP_COLS, axis=1)
            df1 = df1.dropna()
            logging.info("Obtained the data from mongodb and dropped the unnecessary columns from the dataframe")

            # Splitting the data as train set and test set
            train_set, test_set = self.split_data_as_train_test(df)
            logging.info("Initiated the data ingestion")
            logging.info("Exited the initiate_data_ingestion method of DataIngestion class")
            return DataIngestionArtefacts(train_set, test_set)
        except Exception as e:
            raise ShipmentException(e, sys)
        
    