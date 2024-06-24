import os
import sys
import pandas as pd
from dataclasses import dataclass
from shipment.logger import logging
from shipment.exception import ShipmentException
from shipment.constant import *
from shipment.entity.config_entity import ModelEvaluationConfig
from shipment.entity.artefacts_entity import (
    DataIngestionArtefacts,
    ModelTrainerArtefacts,
    ModelEvaluationArtefacts,
)


@dataclass
class EvaluateModelResponse:
    trained_model_r2_score: float
    s3_model_r2_score: float
    is_model_accepted: bool
    difference: float



class ModelEvaluation:
    def __init__(
            self,
            model_trainer_artefact: ModelTrainerArtefacts,
            model_evaluation_config: ModelEvaluationConfig,
            data_ingestion_artefact: DataIngestionArtefacts,
        ):
        self.model_trainer_artefact = model_trainer_artefact
        self.model_evaluation_config = model_evaluation_config
        self.data_ingestion_artefact = data_ingestion_artefact


    # This method is used to get the s3 model
    def get_s3_model(self) -> object:
        """
        Get the s3 model.

        Returns:
            object: The s3 model.
        """
        logging.info("Entered the get_s3_model method of ModelEvaluation class.")

        try:
            # Checking whether model is present in S3 bucket or not
            status = self.model_evaluation_config.S3_OPERATIONS.is_model_present(
                BUCKET_NAME, S3_MODEL_NAME
            )
            logging.info(f"Got the status - is model present? -> {status}")

            # If model is present then loading the model
            if status == True:
                model = self.model_evaluation_config.S3_OPERATIONS.load_model(
                    MODEL_FILE_NAME, BUCKET_NAME
                )
                logging.info("Exited the get_s3_model method of Model Evaluation class")
                return model
            else:
                logging.info("Exited the get_s3_model method of Model Evaluation class")
                None
                #
 
        except Exception as e:
            raise ShipmentException(e, sys)
        
    
    # This method is used to evaluate the model
    def evaluate_model(self) -> EvaluateModelResponse:
        """
        Evaluate the model.

        Returns:
            EvaluateModelResponse: The evaluate model response.
        """
        logging.info("Entered the evaluate_model method of ModelEvaluation class.")
        
        try:
            # Reading the test data and splitting it into train and test
            test_df = pd.read_csv(self.data_ingestion_artefact.test_data_file_path)
            X, y = test_df.drop(TARGET_COLUMN, axis=1), test_df[TARGET_COLUMN]
            logging.info("Loaded the test data from DataIngestionArtefacts directory and splitted the data into X and y")

            print(X.head())

            # Loading production model for prediction
            trained_model = self.model_evaluation_config.UTILS.load_object(
                self.model_trainer_artefact.trained_model_file_path
            )
            y_hat_trained_model = trained_model.predict(X)
            logging.info("Prediction done with production model")

            # Checking the r2 score of production model
            trained_model_r2_score = self.model_evaluation_config.UTILS.get_model_score(
                y, y_hat_trained_model
            )
            logging.info("Got the r2 score of production model")

            # Loading s3 model for prediction
            s3_model_r2_score = None
            s3_model = self.get_s3_model()
            if s3_model is not None:
                y_hat_s3_model = s3_model.predict(X)           

                # Checking the r2 score of s3 model
                s3_model_r2_score = self.model_evaluation_config.UTILS.get_model_score(
                    y, y_hat_s3_model
                )

            print(f"{s3_model_r2_score=}")
            print(f"{s3_model=}")
            
            # Saving the s3 model r2 score in tmp_best_model_score variable
            tmp_best_model_score = 0 if s3_model_r2_score is not None else s3_model_r2_score

            # Saving the Evaluate model response]
            result = EvaluateModelResponse(
                trained_model_r2_score=trained_model_r2_score,
                s3_model_r2_score=s3_model_r2_score,
                # is_model_accepted=trained_model_r2_score > tmpt_best_model_score,
                is_model_accepted=True,
                difference=trained_model_r2_score - tmp_best_model_score if s3_model_r2_score is not None else 0,
            )
            logging.info("Exited the evaluate_model method of ModelEvaluation class")
            return result
        except Exception as e:
            raise ShipmentException(e, sys)
        

    # This method is used to start the model evaluation
    def initiate_model_evaluation(self) -> ModelEvaluationArtefacts:
        """
        Initiate model evaluation.

        Returns:
            ModelEvaluationArtefacts: The model evaluation artefacts.
        """
        logging.info("Entered the initiate_model_evaluation method of ModelEvaluation class.")
        try:
            evaluate_model_response = self.evaluate_model()

            # Saving the model evaluation artefacts
            model_evaluation_artefacts = ModelEvaluationArtefacts(
                is_model_accepted=evaluate_model_response.is_model_accepted,
                trained_model_path=self.model_trainer_artefact.trained_model_file_path,
                changed_accuracy=evaluate_model_response.difference,
            )
            logging.info("Exited the initiate_model_evaluation method of ModelEvaluation class.")
            return model_evaluation_artefacts
        except Exception as e:
            raise ShipmentException(e, sys)