from shipment.constant import *
import boto3
from shipment.logger import logging
from shipment.exception import ShipmentException
from botocore.exceptions import ClientError
from mypy_boto3_s3.service_resource import Bucket
import pandas as pd
from typing import Union, List
from io import StringIO
import sys
import pickle
import os


class S3Operations:
    def __init__(self):
        self.s3_resource = boto3.resource("s3")
        self.s3_client = boto3.client("s3")

    @staticmethod
    def read_object(
        object_name: str, decode: bool = True, make_readable: bool = True
    ) -> Union[StringIO, str]:
        """
        Read the object from the S3 bucket.

        Args:
            object_name (str): The name of the object.
            decode (bool, optional): Whether to decode the object. Defaults to True.
            make_readable (bool, optional): Whether to make the object readable. Defaults to True.

        Returns:
            Union[StringIO, str]: The object.
        """
        logging.info("Entered the read_object method of S3Operations class.")
        try:
            s3_object = boto3.resource("s3").Object(object_name)
            content = s3_object.get()["Body"].read()

            if decode:
                content = content.decode()  # Decode bytes to str

            if make_readable:
                content = StringIO(content)  # Convert str to StringIO

            logging.info("Exited the read_object method of S3Operations class.")
            return content

        except Exception as e:
            raise ShipmentException(e, sys)

        
    def get_bucket(self, bucket_name: str) -> Bucket:
        """
        Get the bucket object.

        Args:
            bucket_name (str): The name of the bucket.

        Returns:
            Bucket: The bucket object.
        """
        logging.info("Entered the get_bucket method of S3Operations class.")
        try:
            bucket = self.s3_resource.Bucket(bucket_name)
            logging.info("Exited the get_bucket method of S3Operations class.")
            return bucket
        except Exception as e:
            raise ShipmentException(e, sys)
        
    def is_model_present(self, bucket_name: str, s3_model_key: str) -> bool:
        """
        Check whether the model is present in the S3 bucket.

        Args:
            bucket_name (str): The name of the bucket.
            s3_model_key (str): The key of the model.

        Returns:
            bool: Whether the model is present.
        """
        logging.info("Entered the is_model_present method of S3Operations class.")
        try:
            bucket = self.get_bucket(bucket_name)
            file_objects = list(bucket.objects.filter(Prefix=s3_model_key))
            return len(file_objects) > 0
        except Exception as e:
            raise ShipmentException(e, sys)
        
    def get_file_object(
        self, filename: str, bucket_name: str
    ) -> Union[List[object], object]:
        """
        Get the file object.

        Args:
            filename (str): The name of the file.
            bucket_name (str): The name of the bucket.

        Returns:
            Union[List[object], object]: The file object.
        """
        logging.info("Entered the get_file_object method of S3Operations class.")
        try:
            bucket = self.get_bucket(bucket_name)
            list_objs = list(bucket.objects.filter(Prefix=filename))
            file_objs = list_objs[0] if len(list_objs) == 1 else list_objs
            logging.info("Exited the get_file_object method of S3Operations class.")
            return file_objs
        except Exception as e:
            raise ShipmentException(e, sys)

    def load_model(
        self, model_name: str, bucket_name: str, model_dir: str = None
    ) -> object:
        """
        Load the model from the S3 bucket.

        Args:
            model_name (str): The name of the model.
            bucket_name (str): The name of the bucket.
            model_dir (str, optional): The directory of the model. Defaults to None.

        Returns:
            object: The model object.
        """
        logging.info("Entered the load_model method of S3Operations class.")
        
        try:
            model_file = model_name if model_dir is None else f"{model_dir}/{model_name}"
            f_obj = self.get_file_object(model_file, bucket_name)
            model_obj = self.read_object(f_obj, decode=False)
            model = pickle.loads(model_obj)
            logging.info("Exited the load_model method of S3Operations class.")
            return model
        except Exception as e:
            raise ShipmentException(e, sys)
        
    def create_folder(self, folder_name: str, bucket_name: str) -> None:
        """
        Create the folder in the S3 bucket.

        Args:
            folder_name (str): The name of the folder.
            bucket_name (str): The name of the bucket.
        """
        logging.info("Entered the create_folder method of S3Operations class.")
        try:
            self.s3_resource.Object(bucket_name, folder_name).load()
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                folder_obj = folder_name + "/"
                self.s3_client.put_object(Bucket=bucket_name, Key=folder_obj)
            else:
                raise ShipmentException(e, sys)
        logging.info("Exited the create_folder method of S3Operations class.")

    def upload_file(
        self,
        from_filename: str,
        to_filename: str,
        bucket_name: str,
        remove: bool = True
    ) -> None:
        """
        Upload the file to the S3 bucket.

        Args:
            from_filename (str): The name of the file to be uploaded.
            to_filename (str): The name of the file in the S3 bucket.
            bucket_name (str): The name of the bucket.
            remove (bool, optional): Whether to remove the file after uploading. Defaults to True.
        """
        logging.info("Entered the upload_file method of S3Operations class.")
        try:
            logging.info(
                f"Uploading {from_filename} file to {to_filename} file in {bucket_name} bucket"
            )
            self.s3_resource.meta.client.upload_file(
                from_filename, bucket_name, to_filename
            )

            if remove:
                os.remove(from_filename)
                logging.info(
                    f"Deleted the {from_filename} file because Remove is set to {remove}"
                )
            else:
                logging.info(
                    f"Not deleted the {from_filename} file because Remove is set to {remove}"
                )
            logging.info("Exiting upload_file method of S3Operations class")
        except Exception as e:
            raise ShipmentException(e, sys)
        
    
    def upload_folder(self, folder_name: str, bucket_name: str) -> None:
        """
        Upload the folder to the S3 bucket.

        Args:
            folder_name (str): The name of the folder.
            bucket_name (str): The name of the bucket.
        """
        logging.info("Entered the upload_folder method of S3Operations class.")
        try:
            logging.info(
                f"Uploading {folder_name} folder to {bucket_name} bucket"
            )
            lst = os.listdir(folder_name)
            for f in lst:
                local_f = os.path.join(folder_name, f)
                dest_f  =  f
                self.upload_file(local_f, dest_f, bucket_name, remove=False)
            logging.info("Exited the upload_folder method of S3Operations class.")
        except Exception as e:
            raise ShipmentException(e, sys)
        
    
    def upload_df_as_csv(
        self,
        data_frame: pd.DataFrame,
        local_filename: str,
        bucket_filename: str,
        bucket_name: str,
    ) -> None:
        
        """
        Upload the data frame as csv to the S3 bucket.

        Args:
            data_frame (pd.DataFrame): The data frame.
            local_filename (str): The local filename.
            bucket_filename (str): The bucket filename.
            bucket_name (str): The name of the bucket.

        Returns:
            None
        """
        logging.info("Entered the upload_df_as_csv method of S3Operations class.")
        try:
            data_frame.to_csv(local_filename, index=False, header=True)
            self.upload_file(local_filename, bucket_filename, bucket_name)
            logging.info("Exited the upload_df_as_csv method of S3Operations class.")
        except Exception as e:
            raise ShipmentException(e, sys)
