from Entities.Endpoints.Factory.EndpointFactory import EndpointFactory
from Entities.Shared.Types import EndpointType, DatabaseType
from dotenv import load_dotenv
import os

load_dotenv()

if __name__ == "__main__":
    # Obter credenciais do arquivo .env
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

    # Criar o endpoint usando a fábrica
    endpoint = EndpointFactory.create_endpoint(
        database_type=DatabaseType.POSTGRESQL,
        endpoint_type=EndpointType.SOURCE,
        endpoint_name='Source_PostgreSQL',
        credentials=credentials
    )

    # Testar a conexão
    print(f"Conexão criada com sucesso: {endpoint.__dict__}\n")

    employees_employee_detail = endpoint.get_table_details(schema='employees', table='salary')

    employees_employee_data   = endpoint.get_full_load_from_table(schema='employees', table='salary')
    print(employees_employee_data)


    # for key, value in employees_employee_detail.__dict__.items():
    #     if key == 'columns':
    #         for column in value:
    #             for key, value in column.__dict__.items():
    #                 print(f"\t\t{key}: {value}")
    #             print("")
    #     else:
    #         print(f"{key}: {value}")

    # print(employees_employee_data)

    employees_employee_full_load = endpoint.insert_full_load_into_table(
        target_schema_name='employees',
        target_table_name='salary_target',
        path_data='data/full_load_data/employees_salary.csv',
        source_table=employees_employee_detail,
        create_table_if_not_exists=True,
        recreate_table_if_exists=True,
        truncate_before_insert=True)
    print(employees_employee_full_load)