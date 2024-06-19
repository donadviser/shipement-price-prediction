from shipment.logger import logging
from shipment.exception import ShipmentException
import sys


logging.info("Starting")
try:
    a = 1/0
    print(a)
except Exception as e:
    raise ShipmentException(e, sys)