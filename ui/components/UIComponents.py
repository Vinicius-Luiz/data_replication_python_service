from ui.components.DisplayErrorHandling import DisplayErrorHandling
from ui.components.DisplayTaskSettings import DisplayTaskSettings
from ui.components.DisplayConections import DisplayConnections
from ui.components.DisplayHomePage import DisplayHomePage
from ui.components.DisplayTables import DisplayTables
from ui.components.DisplayFilter import DisplayFilter
import streamlit as st


class UIComponents:
    """Gerencia os componentes da interface do usuário."""

    def __init__(self):
        """Inicializa os componentes da interface."""
        self.home_page = DisplayHomePage()
        self.connections = DisplayConnections()
        self.task_settings = DisplayTaskSettings()
        self.tables = DisplayTables()
        self.display_filter = DisplayFilter()
        self.error_settings = DisplayErrorHandling()

        # Define o layout da página
        st.set_page_config(
            page_title="TREMpy - Replicação de Dados",
            page_icon="🚂",
            layout="wide",
            initial_sidebar_state="expanded"
        )

    def display_sidebar(self):
        """Exibe a barra lateral de navegação."""
        with st.sidebar:
            st.title("🚂 TREMpy")
            
            # Grupo: Configurações Básicas
            st.sidebar.markdown("### ⚙️ Configurações Básicas")
            if st.sidebar.button("DASHBOARD", use_container_width=True):
                st.session_state.current_page = "home"
            if st.sidebar.button("CONEXÕES", use_container_width=True):
                st.session_state.current_page = "connections"
            if st.sidebar.button("TAREFA", use_container_width=True):
                st.session_state.current_page = "task"
                
            # Grupo: Configurações de Dados
            st.sidebar.markdown("### 📚 Configurações de Dados")
            if st.sidebar.button("TABELAS", use_container_width=True):
                st.session_state.current_page = "tables"
            if st.sidebar.button("FILTROS", use_container_width=True):
                st.session_state.current_page = "filters"
            if st.sidebar.button("TRANSFORMAÇÕES", use_container_width=True):
                st.session_state.current_page = "transformations"
                
            # Grupo: Configurações Avançadas
            st.sidebar.markdown("### 🛠️ Configurações Avançadas")
            if st.sidebar.button("TRATAMENTO DE ERROS", use_container_width=True):
                st.session_state.current_page = "error_handling"
            
            # Rodapé da sidebar com ícones de redes sociais
            st.sidebar.markdown("---")
            col1, col2 = st.sidebar.columns(2)
            with col1:
                st.markdown(
                    """<a href="https://www.linkedin.com/in/vlsf2/" target="_blank">
                        <img src="https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white" />
                    </a>""",
                    unsafe_allow_html=True
                )
            with col2:
                st.markdown(
                    """<a href="https://github.com/Vinicius-Luiz/data_replication_python_service" target="_blank">
                        <img src="https://img.shields.io/badge/GitHub-100000?style=for-the-badge&logo=github&logoColor=white" />
                    </a>""",
                    unsafe_allow_html=True
                )

    def display_content(self):
        """Exibe o conteúdo principal baseado na página selecionada."""
        # Inicializa a página atual se necessário
        if "current_page" not in st.session_state:
            st.session_state.current_page = "home"
            
        # Exibe o conteúdo apropriado
        if st.session_state.current_page == "home":
            self.home_page.display_home_page()
        elif st.session_state.current_page == "connections":
            self.connections.display_connections()
        elif st.session_state.current_page == "task":
            self.task_settings.display_task_settings()
        elif st.session_state.current_page == "tables":
            self.tables.display_tables()
        elif st.session_state.current_page == "filters":
            self.display_filter.display_filters()
        elif st.session_state.current_page == "transformations":
            self.__display_transformations()
        elif st.session_state.current_page == "error_handling":
            self.error_settings.display_error_settings()

    def __display_transformations(self):
        """Exibe a página de transformações (placeholder)."""
        st.header("🔄 Transformações")
        st.info(
            """
            Esta seção permitirá configurar transformações nos dados durante a replicação.
            Funcionalidade em desenvolvimento.
            """
        )

    def display_ui(self):
        """Método principal para exibir a interface completa."""
        self.display_sidebar()
        self.display_content()
