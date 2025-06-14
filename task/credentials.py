from trempy.Shared.Types import *
from dotenv import load_dotenv
import os

load_dotenv()

credentials = {
    "source_endpoint": {
        "database_type": os.getenv("DB_SOURCE_TYPE"),
        "endpoint_type": "source",
        "endpoint_name": os.getenv("DB_SOURCE_ENDPOINT_NAME"),
        "batch_cdc_size": os.getenv("DB_SOURCE_BATCH_SIZE"),
        "credentials": {
            "dbname": os.getenv("DB_SOURCE_NAME"),
            "user": os.getenv("DB_SOURCE_USER"),
            "password": os.getenv("DB_SOURCE_PASSWORD"),
            "host": os.getenv("DB_SOURCE_HOST"),
            "port": os.getenv("DB_SOURCE_PORT"),
        },
    },
    "target_endpoint": {
        "database_type": os.getenv("DB_TARGET_TYPE"),
        "endpoint_type": "target",
        "endpoint_name": os.getenv("DB_TARGET_ENDPOINT_NAME"),
        "credentials": {
            "dbname": os.getenv("DB_TARGET_NAME"),
            "user": os.getenv("DB_TARGET_USER"),
            "password": os.getenv("DB_TARGET_PASSWORD"),
            "host": os.getenv("DB_TARGET_HOST"),
            "port": os.getenv("DB_TARGET_PORT"),
        },
    },
}
