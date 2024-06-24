import sys
from shipment.logger import logging
from shipment.exception import ShipmentException

from shipment.configuration.mongo_operations import MongoDBOperation
from shipment.entity.artefacts_entity import (
    DataIngestionArtefacts,
    DataValidationArtefacts,
    DataTransformationArtefacts,
    ModelTrainerArtefacts,
    ModelEvaluationArtefacts,
    ModelPusherArtefacts,
    )
    
from shipment.entity.config_entity import (
    DataIngestionConfig,
    DataValidationConfig,
    DataTransformationConfig,
    ModelTrainerConfig,
    ModelEvaluationConfig,
    ModelPusherConfig,
    )
    
from shipment.components.data_ingestion import DataIngestion
from shipment.components.data_validation import DataValidation
from shipment.components.data_transformation import DataTransformation
from shipment.components.model_trainer import ModelTrainer
from shipment.components.model_evaluation import ModelEvaluation
from shipment.configuration.s3_operations import S3Operations
from shipment.components.model_pusher import ModelPusher


class TrainPipeline:
    def __init__(self):
        self.data_ingestion_config = DataIngestionConfig()
        self.data_validation_config = DataValidationConfig()
        self.data_transformation_config = DataTransformationConfig()
        self.model_trainer_config = ModelTrainerConfig()
        self.model_evaluation_config = ModelEvaluationConfig()
        self.s3_operations = S3Operations()
        self.model_pusher_config = ModelPusherConfig()
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
        

    # This method is used to start the data transformation.
    def start_data_transformation(
            self, data_ingestion_artefact: DataIngestionArtefacts
            ) -> DataTransformationArtefacts:
        logging.info("Entered the start_data_transformation method of TrainPipeline class.")
        try:
            data_transformation = DataTransformation(
                data_ingestion_artefacts=data_ingestion_artefact,
                data_transformation_config=self.data_transformation_config)
            
            data_transformation_artefact = data_transformation.initiate_data_transformation()
            logging.info("Performed the data transformation operation.")
            logging.info("Exited the start_data_transformation method of TrainPipeline class.")
            return data_transformation_artefact
        except Exception as e:
            raise ShipmentException(e, sys)
        

    # This method is used to start the model trainer.
    def start_model_trainer(
            self, data_transformation_artefact: DataTransformationArtefacts
            ) -> ModelTrainerArtefacts:
        logging.info("Entered the start_model_trainer method of TrainPipeline class.")
        try:
            model_trainer = ModelTrainer(
                data_transformation_artefact=data_transformation_artefact,
                model_trainer_config=self.model_trainer_config
                )
            
            model_trainer_artefact = model_trainer.initiate_model_trainer()
            logging.info("Performed the model training operation.")
            logging.info("Exited the start_model_trainer method of TrainPipeline class.")
            return model_trainer_artefact
        except Exception as e:
            raise ShipmentException(e, sys)
        



    # This method is used to start the model evaluation.
    def start_model_evaluation(
            self,
            data_ingestion_artefact: DataIngestionArtefacts,
            model_trainer_artefact: ModelTrainerArtefacts,
        ) -> ModelEvaluationArtefacts:
        logging.info("Entered the start_model_evaluation method of TrainPipeline class.")
        try:
            model_evaluation = ModelEvaluation(
                model_trainer_artefact=model_trainer_artefact,
                model_evaluation_config=self.model_evaluation_config,
                data_ingestion_artefact=data_ingestion_artefact,
            )
            
            model_evaluation_artefact = model_evaluation.initiate_model_evaluation()
            logging.info("Performed the model evaluation operation.")
            logging.info("Exited the start_model_evaluation method of TrainPipeline class.")
            return model_evaluation_artefact
        except Exception as e:
            raise ShipmentException(e, sys)
        

    # This method is used to start the model pusher.
    def start_model_pusher(
            self,
            model_trainer_artefacts: ModelTrainerArtefacts,
            s3: S3Operations,
            data_transformation_artefacts: DataTransformationArtefacts
        ) -> ModelPusherArtefacts:
        logging.info("Entered the start_model_pusher method of TrainPipeline class.")
        try:
            model_pusher = ModelPusher(
                model_pusher_config=self.model_pusher_config,
                model_trainer_artefacts=model_trainer_artefacts,
                s3=s3,
                data_transformation_artefacts=data_transformation_artefacts,
            )
            
            model_pusher_artefact = model_pusher.initiate_model_pusher()
            logging.info("Performed the model push operation.")
            logging.info("Exited the start_model_pusher method of TrainPipeline class.")
            return model_pusher_artefact
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

            data_transformation_artefact = self.start_data_transformation(
                data_ingestion_artefact=data_ingestion_artefact
            )

            model_trainer_artefact = self.start_model_trainer(
                data_transformation_artefact=data_transformation_artefact
            )

            model_evaluation_artefact = self.start_model_evaluation(
                data_ingestion_artefact=data_ingestion_artefact,
                model_trainer_artefact=model_trainer_artefact,
            )
            if not model_evaluation_artefact.is_model_accepted:
                logging.info("The model is not accpeted.")
                return None
            
            model_pusher_artefact = self.start_model_pusher(
                model_trainer_artefacts=model_trainer_artefact,
                s3=self.s3_operations,
                data_transformation_artefacts=data_transformation_artefact,
            )

            logging.info("Exited the run_pipeline method of TrainPipeline class.")
        except Exception as e:
            raise ShipmentException(e, sys)