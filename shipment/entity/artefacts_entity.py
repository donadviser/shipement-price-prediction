from dataclasses import dataclass

# Data Ingestion Artefacts

@dataclass
class DataIngestionArtefacts:
    train_data_file_path: str
    test_data_file_path: str


@dataclass
class DataValidationArtefacts:
    data_drift_file_path: str
    validation_status: bool