import sys
from shipment.logger import logging
from shipment.exception import ShipmentException

from shipment.configuration.mongo_operations import MongoDBOperation
from shipment.entity.artefacts_entity import (
    DataIngestionArtefacts,
    DataValidationArtefacts,
)
from shipment.entity.config_entity import (
    DataIngestionConfig,
    DataValidationConfig
)
from shipment.components.data_ingestion import DataIngestion
from shipment.components.data_validation import DataValidation


class TrainPipeline:
    def __init__(self):
        self.data_ingestion_config = DataIngestionConfig()
        self.data_validation_config = DataValidationConfig()
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
        

    # This method is used to start the data validation.
    def start_data_validation(self, data_ingestion_artefact: DataIngestionArtefacts) -> DataValidationArtefacts:
        logging.info("Entered the start_data_validation method of TrainPipeline class.")
        try:
            data_validation = DataValidation(
                data_ingestion_artefacts=data_ingestion_artefact,
                data_validation_config=self.data_validation_config)
            
            data_validation_artefact = data_validation.initiate_data_validation()
            logging.info("Performed the data validation operation.")
            logging.info("Exited the start_data_validation method of TrainPipeline class.")
            return data_validation_artefact
        
        except Exception as e:
            raise ShipmentException(e, sys)
        
    # This method is used to start the training pipeline.
    def run_pipeline(self) -> None:
        logging.info("Entered the run_pipeline method of TrainPipeline class.")
        try:
            data_ingestion_artefact = self.start_data_ingestion()
            data_validation_artefact = self.start_data_validation(
                data_ingestion_artefact=data_ingestion_artefact
            )
            logging.info("Exited the run_pipeline method of TrainPipeline class.")
        except Exception as e:
            raise ShipmentException(e, sys)