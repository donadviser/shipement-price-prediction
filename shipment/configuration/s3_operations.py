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


class S3Operations:
    def __init__(self):
        self.s3_resource = boto3.resource("s3")
        self.s3_client = boto3.client("s3")

    @staticmethod
    def read_object(
        object_name: str, decode: bool=True, make_readable: bool=True

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
            func = (
                lambda: object_name.get()["Body"].read().decode()
                if decode is True
                else object_name.get()["Body"].read()
            )

            conv_func = lambda: StringIO(func()) if make_readable is True else func()
            logging.info("Existed the read_object method of S3Operations class")
            return conv_func()
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
            file_objects = [
                file_object
                for file_object in bucket.objects.filter(Prefix=s3_model_key)
            ]
            if len(file_objects) > 0:
                return True
            else:
                return False
        
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
            list_objs = [object for object in bucket.objects.filter(Prefix=filename)]
            func = lambda x: x[0] if len(x) == 1 else x
            file_objs = func(list_objs)
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
            func = (
                lambda: model_name
                if model_dir is None
                else model_dir + "/" + model_name
            )
            model_file = func()
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
                self.s3.client.put_object(Bucket=bucket_name, key=folder_obj)
            else:
                pass
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

            if remove is True:
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
        

    def get_df_from_object(self, object_: object) -> pd.DataFrame:
        """
        Get the data frame from the object.

        Args:
            object_ (object): The object.

        Returns:
            pd.DataFrame: The data frame.
        """
        logging.info("Entered the get_df_from_object method of S3Operations class.")
        try:
            content = self.read_object(object_, make_readable=True)
            df = pd.read_csv(content, na_values="na")
            logging.info("Exited the get_df_from_object method of S3Operations class.")
            return df
        except Exception as e:
            raise ShipmentException(e, sys)
        


    def read_csv(self, filename: str, bucket_name: str) -> pd.DataFrame:
        """
        Read the csv file from the S3 bucket.

        Args:
            filename (str): The name of the file.
            bucket_name (str): The name of the bucket.

        Returns:
            pd.DataFrame: The data frame.
        """
        logging.info("Entered the read_csv method of S3Operations class.")
        try:
            csv_obj = self.get_file_object(filename, bucket_name)
            df = self.get_df_from_object(csv_obj)
            logging.info("Exited the read_csv method of S3Operations class.")
            return df
        except Exception as e:
            raise ShipmentException(e, sys)