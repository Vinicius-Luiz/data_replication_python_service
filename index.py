from Entities.Endpoints.Factory.EndpointFactory import EndpointFactory
from Entities.Shared.Types import EndpointType, DatabaseType
from dotenv import load_dotenv
import logging
import os

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

if __name__ == "__main__":
    logging.info("Obtendo credenciais")
    dbname = os.getenv('DB_NAME_POSTGRESQL')
    user = os.getenv('DB_USER_POSTGRESQL')
    password = os.getenv('DB_PASSWORD_POSTGRESQL')
    host = os.getenv('DB_HOST_POSTGRESQL')
    port = os.getenv('DB_PORT_POSTGRESQL')

    credentials = {
        'dbname': dbname,
        'user': user,
        'password': password,
        'host': host,
        'port': port
    }

    logging.info(f"Conectando ao banco de dados {dbname} como Source")
    endpoint_source = EndpointFactory.create_endpoint(
        database_type=DatabaseType.POSTGRESQL,
        endpoint_type=EndpointType.SOURCE,
        endpoint_name='Source_PostgreSQL',
        credentials=credentials
    )
    logging.info("Conexão criada com sucesso: {}".format(endpoint_source.__dict__))

    schema = 'employees'
    table = 'salary'

    schema_target = 'employees'
    table_target = 'salary_target'

    logging.info(f"Obtendo detalhes da tabela {schema}.{table}")
    table_detail = endpoint_source.get_table_details(schema=schema, table=table)
    logging.debug(table_detail)

    logging.info(f"Obtendo dados da tabela {schema}.{table}")
    table_data   = endpoint_source.get_full_load_from_table(schema=schema, table=table)
    logging.debug(table_data)

    logging.info(f"Conectando ao banco de dados {dbname} como Target")
    endpoint_target = EndpointFactory.create_endpoint(
        database_type=DatabaseType.POSTGRESQL,
        endpoint_type=EndpointType.TARGET,
        endpoint_name='Target_PostgreSQL',
        credentials=credentials
    )
    logging.info("Conexão criada com sucesso: {}".format(endpoint_target.__dict__))

    logging.info(f"Realizando carga completa da tabela {schema}.{table}")
    table_full_load = endpoint_target.insert_full_load_into_table(
        target_schema=schema_target,
        target_table=table_target,
        source_table=table_detail,
        create_table_if_not_exists=True,
        recreate_table_if_exists=True,
        truncate_before_insert=True)
    logging.info(table_full_load)