from trempy.Shared.Types import *
from dotenv import load_dotenv
import os

load_dotenv()

credentials = {
    "source_endpoint": {
        "database_type": "postgresql",
        "endpoint_type": "source",
        "endpoint_name": "Source_PostgreSQL",
        "batch_cdc_size": 1,
        "credentials": {
            "dbname": os.getenv("DB_NAME_POSTGRESQL_SOURCE"),
            "user": os.getenv("DB_USER_POSTGRESQL"),
            "password": os.getenv("DB_PASSWORD_POSTGRESQL"),
            "host": os.getenv("DB_HOST_POSTGRESQL"),
            "port": os.getenv("DB_PORT_POSTGRESQL"),
        },
    },
    "target_endpoint": {
        "database_type": "postgresql",
        "endpoint_type": "target",
        "endpoint_name": "Target_PostgreSQL",
        "credentials": {
            "dbname": os.getenv("DB_NAME_POSTGRESQL_TARGET"),
            "user": os.getenv("DB_USER_POSTGRESQL"),
            "password": os.getenv("DB_PASSWORD_POSTGRESQL"),
            "host": os.getenv("DB_HOST_POSTGRESQL"),
            "port": os.getenv("DB_PORT_POSTGRESQL"),
        },
    },
}
