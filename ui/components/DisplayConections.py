from trempy.Shared.Types import DatabaseType
from typing import Dict, Any, Union
from dotenv import load_dotenv
from pathlib import Path
import streamlit as st
import os


class DisplayConnections:
    """
    Classe responsável por gerenciar e exibir a interface de configuração de conexões com bancos de dados.
    
    Esta classe fornece uma interface gráfica para configurar e salvar as configurações
    de conexão com bancos de dados de origem e destino.
    """
    
    ENV_FILE = Path(".env")
    DEFAULT_HOST = "localhost"
    DEFAULT_PORT = "5432"
    DEFAULT_BATCH_SIZE = 1000
    
    def __init__(self):
        """Inicializa a classe carregando as variáveis de ambiente."""
        load_dotenv()
        
    def __save_connection_settings(self, **kwargs: Any) -> bool:
        """
        Salva todas as configurações no arquivo .env.
        
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
            # Garante que o diretório pai existe
            self.ENV_FILE.parent.mkdir(parents=True, exist_ok=True)
            
            # Salva as configurações
            with open(self.ENV_FILE, "w", encoding="utf-8") as f:
                for key, value in env_vars.items():
                    f.write(f"{key}={value}\n")

            st.success("Configurações salvas com sucesso!")
            return True

        except PermissionError:
            st.error("Erro de permissão ao salvar o arquivo .env. Verifique as permissões da pasta.")
            return False
        except Exception as e:
            st.error(f"Erro inesperado ao salvar configurações: {str(e)}")
            return False

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
                help=f"Selecione o tipo de banco de dados de {title.lower()}",
            ),
            f"{prefix}_endpoint_name": st.text_input(
                "Nome do Endpoint",
                value=os.getenv(f"DB_{prefix.upper()}_ENDPOINT_NAME", ""),
                key=f"{prefix}_endpoint_name",
                help=f"Insira o nome do endpoint de {title.lower()}. Escolha livremente",
            ),
            f"{prefix}_db_name": st.text_input(
                "Nome do Banco",
                value=os.getenv(f"DB_{prefix.upper()}_NAME", ""),
                key=f"{prefix}_db_name",
                help=f"Insira o nome do banco de dados de {title.lower()}. Ex: db_employees",
            )
        }
        
        # Campos de usuário e senha
        subcol11, subcol12 = st.columns(2, gap="medium")
        with subcol11:
            config[f"{prefix}_db_user"] = st.text_input(
                "Usuário",
                value=os.getenv(f"DB_{prefix.upper()}_USER", ""),
                key=f"{prefix}_db_user",
                help=f"Insira o usuário de acesso ao banco de dados de {title.lower()}",
            )
        with subcol12:
            config[f"{prefix}_db_password"] = st.text_input(
                "Senha",
                type="password",
                value=os.getenv(f"DB_{prefix.upper()}_PASSWORD", ""),
                key=f"{prefix}_db_password",
                help=f"Insira a senha de acesso ao banco de dados de {title.lower()}",
            )

        # Campos de host e porta
        subcol21, subcol22 = st.columns(2, gap="medium")
        with subcol21:
            config[f"{prefix}_db_host"] = st.text_input(
                "Host",
                value=os.getenv(f"DB_{prefix.upper()}_HOST", self.DEFAULT_HOST),
                key=f"{prefix}_db_host",
                help=f"Insira o host do banco de dados de {title.lower()}",
            )
        with subcol22:
            config[f"{prefix}_db_port"] = st.text_input(
                "Porta",
                value=os.getenv(f"DB_{prefix.upper()}_PORT", self.DEFAULT_PORT),
                key=f"{prefix}_db_port",
                help=f"Insira a porta do banco de dados de {title.lower()}",
            )
            
        # Campo de batch size apenas para origem
        if prefix == "source":
            config["source_batch_size"] = st.number_input(
                "Tamanho do Lote CDC",
                min_value=1,
                max_value=80000,
                value=int(os.getenv("DB_SOURCE_BATCH_SIZE", self.DEFAULT_BATCH_SIZE)),
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
        
        load_dotenv()

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
