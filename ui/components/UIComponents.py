from ui.components.DisplayErrorHandling import DisplayErrorHandling
from ui.components.DisplayTaskSettings import DisplayTaskSettings
from ui.components.DisplayConections import DisplayConnections
from ui.components.DisplayHomePage import DisplayHomePage
from ui.components.DisplayTables import DisplayTables
from ui.components.DisplayFilter import DisplayFilter
import streamlit as st


class UIComponents:

    def __init__(self):
        self.home_page = DisplayHomePage()
        self.connections = DisplayConnections()
        self.task_settings = DisplayTaskSettings()
        self.tables = DisplayTables()
        self.display_filter = DisplayFilter()
        self.error_settings = DisplayErrorHandling()


    def __display_default(self, session_name: str):
        st.header(session_name)

        # Conteúdo da terceira aba
        st.markdown(
            """
        ## Aplicação de Exemplo
        
        Este é um exemplo simples de como usar abas no Streamlit.
        
        - **Tab1**: Configurações do usuário
        - **Tab2**: Visualização de dados
        - **Tab3**: Informações sobre o app
        
        Desenvolvido com ❤️ usando Streamlit
        """
        )

    def display_home_page(self):
        """Exibe a página inicial"""
        self.home_page.display_home_page()

    def display_connections(self):
        """Exibe configurações de conexão"""
        self.connections.display_connections()

    def display_task_settings(self):
        """Exibe configurações da tarefa"""
        self.task_settings.display_task_settings()

    def display_tables(self):
        """Exibe configurações de tabelas"""
        self.tables.display_tables()

    def display_filters(self):
        """Exibe configurações de filtros"""
        self.display_filter.display_filters()

    def display_transformations(self):
        """Exibe configurações de transformações"""
        self.__display_default("Transformações")

    def display_error_handling(self):
        """Exibe configurações de tratamento de erros"""
        self.error_settings.display_error_settings()
