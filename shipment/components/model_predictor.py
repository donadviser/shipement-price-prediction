
import sys
from typing import Dict
import pandas as pd
from shipment.logger import logging
from shipment.exception import ShipmentException
from shipment.constant import *
from shipment.configuration.s3_operations import S3Operations



class ShippingData:
    def __init__(
            self,
            artist,
            height,
            width,
            weight,
            material,
            priceOfSculpture,
            baseShippingPrice,
            international,
            expressShipment,
            installationIncluded,
            transport,
            fragile,
            customerInformation,
            remoteLocation,
    ):
        self.artist = artist
        self.height = height
        self.width = width
        self.weight = weight
        self.material = material
        self.priceOfSculpture = priceOfSculpture
        self.baseShippingPrice = baseShippingPrice
        self.international = international
        self.expressShipment = expressShipment
        self.installationIncluded = installationIncluded
        self.transport = transport
        self.fragile = fragile
        self.customerInformation = customerInformation
        self.remoteLocation = remoteLocation


    def get_data(self) -> Dict:
        """
        Get the data.

        Returns:
            Dict: The data.
        """
        logging.info("Entered the get_data method of ShippingData class")
        try:
            # Saving the features as dictionary
            input_data = {
                    "Artist Reputation": [self.artist],
                    "Height": [self.height],
                    "Width": [self.width],
                    "Weight": [self.weight],
                    "Material": [self.material],
                    "Price Of Sculpture": [self.priceOfSculpture],
                    "Base Shipping Price": [self.baseShippingPrice],
                    "International": [self.international],
                    "Express Shipment": [self.expressShipment],
                    "Installation Included": [self.installationIncluded],
                    "Transport": [self.transport],
                    "Fragile": [self.fragile],
                    "Customer Information": [self.customerInformation],
                    "Remote Location": [self.remoteLocation],
            }
            logging.info("Exited the get_data method of ShippingData class")
            return input_data
        except Exception as e:
            raise ShipmentException(e, sys)
        
    
    def get_input_data_frame(self) -> pd.DataFrame:
        """
        Get the input data frame.

        Returns:
            pd.DataFrame: The input data frame.
        """
        logging.info("Entered the get_input_data_frame method of ShippingData class")
        try:
            # Getting the input data in dictionary format
            input_dict = self.get_data()
            logging.info("Obtained the input data in dictionary format")
            logging.info("Exited the get_input_data_frame method of ShippingData class")
            return pd.DataFrame(input_dict)
        
        except Exception as e:
            raise ShipmentException(e, sys)


class CostPredictor:
    def __init__(self):
        self.s3 = S3Operations()
        self.bucket_name = BUCKET_NAME

    def predict(self, X) -> float:
        """
        This method predicts the data

        Args:
            X (pd.DataFrame): The data to be predicted.

        Returns:
            float: The predicted data.

        """
        logging.info("Entered the predict method of ModelPredictor class")
        try:
            # Loading the best model from s3 bucket
            best_model = self.s3.load_model(MODEL_FILE_NAME, self.bucket_name)
            logging.info("Loaded best model from s3 bucket")

            # Predicting the data with the best model
            result = best_model.predict(X)
            logging.info("Exited the predict method of ModelPredictor class")
            return result
        
        except Exception as e:
            raise ShipmentException(e, sys)