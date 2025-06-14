from trempy.Shared.Types import DatabaseType
import streamlit as st
import os


class DisplayConnections:

    def __save_connection_settings(self, **kwargs):
        """Salva todas as configurações no arquivo .env"""

        env_vars = {
            "DB_SOURCE_TYPE": kwargs["source_db_type"],
            "DB_SOURCE_NAME": kwargs["source_db_name"],
            "DB_SOURCE_USER": kwargs["source_db_user"],
            "DB_SOURCE_PASSWORD": kwargs["source_db_password"],
            "DB_SOURCE_HOST": kwargs["source_db_host"],
            "DB_SOURCE_PORT": kwargs["source_db_port"],
            "DB_SOURCE_ENDPOINT_NAME": kwargs["source_endpoint_name"],
            "DB_SOURCE_BATCH_SIZE": str(kwargs["source_batch_size"]),
            "DB_TARGET_TYPE": kwargs["target_db_type"],
            "DB_TARGET_NAME": kwargs["target_db_name"],
            "DB_TARGET_USER": kwargs["target_db_user"],
            "DB_TARGET_PASSWORD": kwargs["target_db_password"],
            "DB_TARGET_HOST": kwargs["target_db_host"],
            "DB_TARGET_PORT": kwargs["target_db_port"],
            "DB_TARGET_ENDPOINT_NAME": kwargs["target_endpoint_name"],
        }

        try:
            with open(".env", "w", encoding="utf-8") as f:
                for key, value in env_vars.items():
                    f.write(f"{key}={value}\n")

            st.success("Configurações salvas!")

        except Exception as e:
            st.error(f"Erro ao salvar configurações: {str(e)}")

    def display_connections(self):
        """Exibe configurações de conexão"""
        st.header("Configurações de Conexão")

        # Opções para selects
        database_types = list(map(lambda task: task.value, DatabaseType))

        with st.form("connection_settings_form"):
            # Dividir em duas colunas para origem e destino
            col1, col2 = st.columns(2, gap="large")

            with col1:
                st.subheader("Origem :computer::arrow_right:")
                source_db_type = st.selectbox(
                    "Tipo de Banco de Dados",
                    options=database_types,
                    key="source_db_type",
                    help="Selecione o tipo de banco de dados de origem",
                )
                source_endpoint_name = st.text_input(
                    "Nome do Endpoint",
                    value="Source_PostgreSQL",
                    key="source_endpoint_name",
                    help="Insira o nome do endpoint de origem. Escolha livremente",
                )
                source_db_name = st.text_input(
                    "Nome do Banco",
                    value=os.getenv("DB_NAME_SOURCE", ""),
                    key="source_db_name",
                    help="Insira o nome do banco de dados de origem. Ex: db_employees",
                )
                subcol11, subcol12 = st.columns(2, gap="medium")
                with subcol11:
                    source_db_user = st.text_input(
                        "Usuário",
                        value=os.getenv("DB_USER_SOURCE", ""),
                        key="source_db_user",
                        help="Insira o usuário de acesso ao banco de dados de origem",
                    )
                with subcol12:
                    source_db_password = st.text_input(
                        "Senha",
                        type="password",
                        value=os.getenv("DB_PASSWORD_SOURCE", ""),
                        key="source_db_password",
                        help="Insira a senha de acesso ao banco de dados de origem",
                    )

                subcol21, subcol22 = st.columns(2, gap="medium")
                with subcol21:
                    source_db_host = st.text_input(
                        "Host",
                        value=os.getenv("DB_HOST_SOURCE", "localhost"),
                        key="source_db_host",
                        help="Insira o host do banco de dados de origem",
                    )
                with subcol22:
                    source_db_port = st.text_input(
                        "Porta",
                        value=os.getenv("DB_PORT_SOURCE", "5432"),
                        key="source_db_port",
                        help="Insira a porta do banco de dados de origem",
                    )
                source_batch_size = st.number_input(
                    "Tamanho do Lote CDC",
                    min_value=1,
                    max_value=80000,
                    value=20000,
                    step=1000,
                    key="source_batch_size",
                    help="Quantidade de registros a serem processados por vez",
                )

            with col2:
                st.subheader("Destino :arrow_right::computer:")
                target_db_type = st.selectbox(
                    "Tipo de Banco de Dados",
                    options=database_types,
                    key="target_db_type",
                    help="Selecione o tipo de banco de dados de destino",
                )
                target_endpoint_name = st.text_input(
                    "Nome do Endpoint",
                    value="Target_PostgreSQL",
                    key="target_endpoint_name",
                    help="Insira o nome do endpoint de destino. Escolha livremente",
                )
                target_db_name = st.text_input(
                    "Nome do Banco",
                    value=os.getenv("DB_NAME_TARGET", ""),
                    key="target_db_name",
                    help="Insira o nome do banco de dados de destino. Ex: db_employees",
                )
                subcol11, subcol12 = st.columns(2, gap="medium")
                with subcol11:
                    target_db_user = st.text_input(
                        "Usuário",
                        value=os.getenv("DB_USER_TARGET", ""),
                        key="target_db_user",
                        help="Insira o usuário de acesso ao banco de dados de destino",
                    )
                with subcol12:
                    target_db_password = st.text_input(
                        "Senha",
                        type="password",
                        value=os.getenv("DB_PASSWORD_TARGET", ""),
                        key="target_db_password",
                        help="Insira a senha de acesso ao banco de dados de destino",
                    )

                subcol21, subcol22 = st.columns(2, gap="medium")
                with subcol21:
                    target_db_host = st.text_input(
                        "Host",
                        value=os.getenv("DB_HOST_TARGET", "localhost"),
                        key="target_db_host",
                        help="Insira o host do banco de dados de destino",
                    )
                with subcol22:
                    target_db_port = st.text_input(
                        "Porta",
                        value=os.getenv("DB_PORT_TARGET", "5432"),
                        key="target_db_port",
                    )

            # Botão de submit
            submitted = st.form_submit_button("Salvar Configurações", type="primary")

            if submitted:
                self.__save_connection_settings(
                    source_db_type=source_db_type,
                    source_endpoint_name=source_endpoint_name,
                    source_batch_size=source_batch_size,
                    source_db_name=source_db_name,
                    source_db_user=source_db_user,
                    source_db_password=source_db_password,
                    source_db_host=source_db_host,
                    source_db_port=source_db_port,
                    target_db_type=target_db_type,
                    target_endpoint_name=target_endpoint_name,
                    target_db_name=target_db_name,
                    target_db_user=target_db_user,
                    target_db_password=target_db_password,
                    target_db_host=target_db_host,
                    target_db_port=target_db_port,
                )
