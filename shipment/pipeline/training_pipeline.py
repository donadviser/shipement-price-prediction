import sys
from shipment.configuration.mongo_operations import MongoDBOperation
from shipment.entity.artefacts_entity import (
    DataIngestionArtefacts,
)
from shipment.entity.config_entity import (
    DataIngestionConfig,
)
from shipment.components.data_ingestion import DataIngestion
from shipment.logger import logging
from shipment.exception import ShipmentException

class TrainPipeline:
    def __init__(self):
        self.data_ingestion_config = DataIngestionConfig()
        self.mongo_op = MongoDBOperation()

    # This method is used to start the data ingestion.
    def start_data_ingestion(self) -> DataIngestionArtefacts:
        logging.info("Entered the start_data_ingestion method of TrainPipeline class.")
        try:            
            data_ingestion = DataIngestion(self.data_ingestion_config, self.mongo_op)
            data_ingestion_artefacts = data_ingestion.initiate_data_ingestion()
            logging.info("Exited the start_data_ingestion method of TrainPipeline class.")
            return data_ingestion_artefacts
        except Exception as e:
            raise ShipmentException(e, sys)
        
    # This method is used to start the training pipeline.
    def run_pipeline(self) -> None:
        logging.info("Entered the run_pipeline method of TrainPipeline class.")
        try:
            data_ingestion_artefacts = self.start_data_ingestion()
            logging.info("Exited the run_pipeline method of TrainPipeline class.")
        except Exception as e:
            raise ShipmentException(e, sys)