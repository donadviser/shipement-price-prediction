from dataclasses import dataclass
from from_root import from_root
import os
from shipment.configuration.s3_operations import S3Operations
from shipment.utils.main_utils import MainUtils
from shipment.constant import *
from shipment.exception import ShipmentException
from shipment.logger import logging


@dataclass
class DataIngestionConfig:
    def __init__(self):
        self.UTILS = MainUtils()
        self.SCHEMA_CONFIG = self.UTILS.read_yaml_file(filename=SCHEMA_FILE_PATH)
        self.DB_NAME = DB_NAME
        self.COLLECTION_NAME = COLLECTION_NAME
        self.TARGET_COLUMN = TARGET_COLUMN

        self.DROP_COLS = list(self.SCHEMA_CONFIG["drop_columns"])
        self.DATA_INGESTION_ARTEFACTS_DIR: str = os.path.join(
            from_root(), ARTEFACTS_DIR, DATA_INGESTION_ARTEFACTS_DIR
            )
        self.TRAIN_DATA_ARTEFACT_FILE_DIR: str = os.path.join(
            self.DATA_INGESTION_ARTEFACTS_DIR, DATA_INGESTION_TRAIN_DIR
            )
        self.TEST_DATA_ARTEFACT_FILE_DIR: str = os.path.join(
            self.DATA_INGESTION_ARTEFACTS_DIR, DATA_INGESTION_TEST_DIR
            )
        self.TRAIN_DATA_FILE_PATH: str = os.path.join(
            self.TRAIN_DATA_ARTEFACT_FILE_DIR, DATA_INGESTION_TRAIN_FILE_NAME
            )
        self.TEST_DATA_FILE_PATH: str = os.path.join(
            self.TEST_DATA_ARTEFACT_FILE_DIR, DATA_INGESTION_TEST_FILE_NAME
            )
        

@dataclass
class DataValidationConfig:
    def __init__(self):
        self.UTILS = MainUtils()
        self.SCHEMA_CONFIG = self.UTILS.read_yaml_file(filename=SCHEMA_FILE_PATH)

        self.DATA_INGESTION_ARTEFACTS_DIR: str = os.path.join(
            from_root(), ARTEFACTS_DIR, DATA_INGESTION_ARTEFACTS_DIR
        )
        self.DATA_VALIDATION_ARTEFACTS_DIR: str = os.path.join(
            from_root(), ARTEFACTS_DIR, DATA_VALIDATION_ARTEFACT_DIR
            )
        self.DATA_DRIFT_FILE_PATH: str = os.path.join(
            self.DATA_VALIDATION_ARTEFACTS_DIR, DATA_DRIFT_FILE_NAME
            )
        


@dataclass
class DataTransformationConfig:
    def __init__(self):
        self.UTILS = MainUtils()
        self.SCHEMA_CONFIG = self.UTILS.read_yaml_file(filename=SCHEMA_FILE_PATH)

        self.DATA_INGESTION_ARTEFACTS_DIR: str = os.path.join(
            from_root(), ARTEFACTS_DIR, DATA_INGESTION_ARTEFACTS_DIR
        )
        self.DATA_TRANSFORMATION_ARTEFACTS_DIR: str = os.path.join(
            from_root(), ARTEFACTS_DIR, DATA_TRANSFORMATION_ARTEFACTS_DIR
            )
        self.TRANSFORMED_TRAIN_DATA_DIR: str = os.path.join(
            self.DATA_TRANSFORMATION_ARTEFACTS_DIR, TRANSFORMED_TRAIN_DATA_DIR
            )
        self.TRANSFORMED_TEST_DATA_DIR: str = os.path.join(
            self.DATA_TRANSFORMATION_ARTEFACTS_DIR, TRANSFORMED_TEST_DATA_DIR
            )
        self.TRANSFORMED_TRAIN_FILE_PATH: str = os.path.join(
            self.TRANSFORMED_TRAIN_DATA_DIR, TRANSFORMED_TRAIN_DATA_FILE_NAME
            )
        self.TRANSFORMED_TEST_FILE_PATH: str = os.path.join(
            self.TRANSFORMED_TEST_DATA_DIR, TRANSFORMED_TEST_DATA_FILE_NAME
            )
        self.PREPROCESSOR_FILE_PATH: str = os.path.join(
            from_root(), ARTEFACTS_DIR, DATA_TRANSFORMATION_ARTEFACTS_DIR, PREPROCESSOR_OBJECT_FILE_NAME
            )

# Model Evaluation Configurations
@dataclass
class ModelTrainerConfig:
    def __init__(self):
        self.UTILS = MainUtils()
        
        self.DATA_TRANSFORMATION_ARTEFACTS_DIR: str = os.path.join(
            from_root(), ARTEFACTS_DIR, DATA_TRANSFORMATION_ARTEFACTS_DIR
        )
        self.MODEL_TRAINER_ARTEFACTS_DIR: str = os.path.join(
            from_root(), ARTEFACTS_DIR, MODEL_TRAINER_ARTEFACTS_DIR
            )
        self.PREPROCESSOR_OBJECT_FILE_PATH: str = os.path.join(
            self.DATA_TRANSFORMATION_ARTEFACTS_DIR, PREPROCESSOR_OBJECT_FILE_NAME
    
        )
        self.TRAINED_MODEL_FILE_PATH: str = os.path.join(
            from_root(), ARTEFACTS_DIR, MODEL_TRAINER_ARTEFACTS_DIR, MODEL_FILE_NAME
            )
        

@dataclass
class ModelEvaluationConfig:
    def __init__(self):
        self.S3_OPERATIONS =S3Operations()
        self.UTILS = MainUtils()
        self.BUCKET_NAME: str = BUCKET_NAME
        self.BEST_MODEL_PATH: str = os.path.join(
            from_root(), ARTEFACTS_DIR, MODEL_TRAINER_ARTEFACTS_DIR, MODEL_FILE_NAME
        )


# Model Pusher Configureations
@dataclass
class ModelPusherConfig:
    def __init__(self):
        self.BEST_MODEL_PATH: str = os.path.join(
            from_root(), ARTEFACTS_DIR, MODEL_TRAINER_ARTEFACTS_DIR, MODEL_FILE_NAME
        )
        self.BUCKET_NAME: str = BUCKET_NAME
        self.S3_MODEL_KEY_PATH: str = os.path.join(S3_MODEL_NAME)