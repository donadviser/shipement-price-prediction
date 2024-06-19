import os
from os import environ
from datetime import datetime
from from_root import from_root

TIMESTAMP: str = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

MODEL_CONFIG_FILE = "config/model.yaml"
# CONFIG_FILE_PATH = "config/config.yaml"
SCHEMA_FILE_PATH = "config/schema.yaml"

DB_URL = environ["MONGO_DB_URL"]
DB_NAME = "shipmentdata"
COLLECTION_NAME = "ship"

TARGET_COLUMN = "Cost"

TEST_SIZE = 0.2

ARTEFACTS_DIR = os.path.join(from_root(), "artefacts", TIMESTAMP)

DATA_INGESTION_ARTEFACTS_DIR = "DataIngestionArtefacts"
DATA_INGESTION_TRAIN_DIR = "Train"
DATA_INGESTION_TEST_DIR = "Test"
DATA_INGESTION_TRAIN_FILE_NAME = "train.csv"
DATA_INGESTION_TEST_FILE_NAME = "test.csv"

