from trempy.Shared.Types import DatabaseType
from trempy.Shared.Utils import Utils
from typing import Dict, Any, Union
from pathlib import Path
import streamlit as st
import json
import os


class DisplayConnections:
    """
    Classe responsável por gerenciar e exibir a interface de configuração de conexões com bancos de dados.
    
    Esta classe fornece uma interface gráfica para configurar e salvar as configurações
    de conexão com bancos de dados de origem e destino.
    """
    
    CREDENTIALS_FILE = Path("task/credentials.json")
    DEFAULT_BATCH_SIZE = 1000
    
    def __init__(self):
        """Inicializa a classe carregando as credenciais."""
        self.credentials = self.__load_credentials()
        
    def __load_credentials(self) -> dict:
        """
        Carrega as credenciais do arquivo JSON.
        
        Returns:
            dict: Dicionário com as credenciais dos endpoints
        """
        try:
            if self.CREDENTIALS_FILE.exists():
                return Utils.read_credentials()
            return {
                "source_endpoint": {
                    "database_type": "",
                    "endpoint_type": "source",
                    "endpoint_name": "",
                    "batch_cdc_size": self.DEFAULT_BATCH_SIZE,
                    "credentials": {
                        "dbname": "",
                        "user": "",
                        "password": "",
                        "host": "172.26.64.1",
                        "port": "5432"
                    }
                },
                "target_endpoint": {
                    "database_type": "",
                    "endpoint_type": "target",
                    "endpoint_name": "",
                    "credentials": {
                        "dbname": "",
                                                  "user": "",
                          "password": "",
                          "host": "172.26.64.1",
                          "port": "5432"
                    }
                }
            }
        except Exception as e:
            st.error(f"Erro ao carregar credenciais: {str(e)}")
            return {}
        
    def __save_connection_settings(self, **kwargs: Any) -> bool:
        """
        Salva todas as configurações no arquivo credentials.json.
        
        Args:
            **kwargs: Dicionário com as configurações de conexão.
                Deve conter as chaves para configurações de origem e destino.
                
        Returns:
            bool: True se as configurações foram salvas com sucesso, False caso contrário.
        """
        required_fields = [
            "source_db_type", "source_db_name", "source_db_user",
            "source_db_password", "source_db_host", "source_db_port",
            "source_endpoint_name", "source_batch_size",
            "target_db_type", "target_db_name", "target_db_user",
            "target_db_password", "target_db_host", "target_db_port",
            "target_endpoint_name"
        ]
        
        # Validação dos campos obrigatórios
        missing_fields = [field for field in required_fields if not kwargs.get(field)]
        if missing_fields:
            st.error(f"Campos obrigatórios não preenchidos: {', '.join(missing_fields)}")
            return False

        credentials = {
            "source_endpoint": {
                "database_type": kwargs["source_db_type"],
                "endpoint_type": "source",
                "endpoint_name": kwargs["source_endpoint_name"],
                "batch_cdc_size": kwargs["source_batch_size"],
                "credentials": {
                    "dbname": kwargs["source_db_name"],
                    "user": kwargs["source_db_user"],
                    "password": kwargs["source_db_password"],
                    "host": kwargs["source_db_host"],
                    "port": kwargs["source_db_port"]
                }
            },
            "target_endpoint": {
                "database_type": kwargs["target_db_type"],
                "endpoint_type": "target",
                "endpoint_name": kwargs["target_endpoint_name"],
                "credentials": {
                    "dbname": kwargs["target_db_name"],
                    "user": kwargs["target_db_user"],
                    "password": kwargs["target_db_password"],
                    "host": kwargs["target_db_host"],
                    "port": kwargs["target_db_port"]
                }
            }
        }

        try:
            Utils.save_credentials(credentials)
            st.success("Configurações salvas com sucesso!")
            return True
        except Exception as e:
            st.error(f"Erro ao salvar configurações: {str(e)}")
            return False

    def __get_endpoint_value(self, endpoint_type: str, field: str, default: Any = "") -> Any:
        """
        Obtém um valor específico das credenciais do endpoint.
        
        Args:
            endpoint_type: 'source' ou 'target'
            field: Campo a ser obtido
            default: Valor padrão caso não exista
            
        Returns:
            Any: Valor do campo
        """
        endpoint = self.credentials.get(f"{endpoint_type}_endpoint", {})
        if field in ["dbname", "user", "password", "host", "port"]:
            return endpoint.get("credentials", {}).get(field, default)
        return endpoint.get(field, default)

    def __render_database_config(
        self, 
        prefix: str, 
        title: str, 
        icon: str,
        database_types: list
    ) -> Dict[str, Union[str, int]]:
        """
        Renderiza o formulário de configuração para um banco de dados.
        
        Args:
            prefix: Prefixo para as chaves de ambiente ('source' ou 'target')
            title: Título da seção
            icon: Ícone da seção
            database_types: Lista de tipos de banco de dados disponíveis
            
        Returns:
            Dict[str, Union[str, int]]: Configurações do banco de dados
        """
        st.subheader(f"{title} {icon}")
        
        config = {
            f"{prefix}_db_type": st.selectbox(
                "Tipo de Banco de Dados",
                options=database_types,
                key=f"{prefix}_db_type",
                help=f"Selecione o tipo de banco de dados de {title.lower()}"
            ),
            f"{prefix}_endpoint_name": st.text_input(
                "Nome do Endpoint",
                value=self.__get_endpoint_value(prefix, "endpoint_name"),
                key=f"{prefix}_endpoint_name",
                help=f"Insira o nome do endpoint de {title.lower()}. Escolha livremente",
            ),
            f"{prefix}_db_name": st.text_input(
                "Nome do Banco",
                value=self.__get_endpoint_value(prefix, "dbname"),
                key=f"{prefix}_db_name",
                help=f"Insira o nome do banco de dados de {title.lower()}. Ex: db_employees",
            )
        }
        
        # Campos de usuário e senha
        subcol11, subcol12 = st.columns(2, gap="medium")
        with subcol11:
            config[f"{prefix}_db_user"] = st.text_input(
                "Usuário",
                value=self.__get_endpoint_value(prefix, "user"),
                key=f"{prefix}_db_user",
                help=f"Insira o usuário de acesso ao banco de dados de {title.lower()}",
            )
        with subcol12:
            config[f"{prefix}_db_password"] = st.text_input(
                "Senha",
                type="password",
                value=self.__get_endpoint_value(prefix, "password"),
                key=f"{prefix}_db_password",
                help=f"Insira a senha de acesso ao banco de dados de {title.lower()}",
            )

        # Campos de host e porta
        subcol21, subcol22 = st.columns(2, gap="medium")
        with subcol21:
            config[f"{prefix}_db_host"] = st.text_input(
                "Host",
                value=self.__get_endpoint_value(prefix, "host", 'localhost'),
                key=f"{prefix}_db_host",
                help=f"Insira o host do banco de dados de {title.lower()}",
            )
        with subcol22:
            config[f"{prefix}_db_port"] = st.text_input(
                "Porta",
                value=self.__get_endpoint_value(prefix, "port", 5432),
                key=f"{prefix}_db_port",
                help=f"Insira a porta do banco de dados de {title.lower()}",
            )
            
        # Campo de batch size apenas para origem
        if prefix == "source":
            config["source_batch_size"] = st.number_input(
                "Tamanho do Lote CDC",
                min_value=1,
                max_value=80000,
                value=int(self.__get_endpoint_value(prefix, "batch_cdc_size", self.DEFAULT_BATCH_SIZE)),
                step=1000,
                key="source_batch_size",
                help="Quantidade de registros a serem processados por vez",
            )
            
        return config

    def render(self) -> None:
        """Renderiza a interface de configuração de conexões."""
        st.header("Configuração de Conexões")
        st.markdown(
            """
            Configure as conexões com os bancos de dados de origem e destino.
            Defina os parâmetros de conexão como host, porta, usuário e senha.
            Para o banco de origem, você também pode configurar o tamanho do lote
            para operações CDC.
            """
        )

        # Opções para selects
        database_types = [db_type.value for db_type in DatabaseType]

        with st.form("connection_settings_form"):
            # Dividir em duas colunas para origem e destino
            col1, col2 = st.columns(2, gap="large")

            # Renderiza configurações de origem
            with col1:
                source_config = self.__render_database_config(
                    prefix="source",
                    title="Origem",
                    icon=":computer::arrow_right:",
                    database_types=database_types
                )

            # Renderiza configurações de destino
            with col2:
                target_config = self.__render_database_config(
                    prefix="target",
                    title="Destino",
                    icon=":arrow_right::computer:",
                    database_types=database_types
                )

            # Botão de submit
            if st.form_submit_button("Salvar Configurações", type="primary"):
                # Combina as configurações
                all_config = {**source_config, **target_config}
                self.__save_connection_settings(**all_config)
