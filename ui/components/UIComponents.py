from ui.components.DisplayTransformation import DisplayTransformation
from ui.components.DisplayErrorHandling import DisplayErrorHandling
from ui.components.DisplayTaskSettings import DisplayTaskSettings
from ui.components.DisplayConections import DisplayConnections
from ui.components.DisplayHomePage import DisplayHomePage
from ui.components.DisplayTables import DisplayTables
from ui.components.DisplayFilter import DisplayFilter
from time import sleep
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
        self.display_transformation = DisplayTransformation()
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
            
            # Grupo: Importar/Exportar Configurações
            st.sidebar.markdown("### 💾 Backup")
            action = st.sidebar.selectbox(
                "Configurações",
                options=["📤 Exportar", "📥 Importar", "🔄 Redefinir"],
                key="settings_action",
                help="Selecione uma ação para importar, exportar ou redefinir configurações",
                label_visibility="collapsed"
            )
            
            if action == "📤 Exportar":
                st.sidebar.download_button(
                    "Baixar settings.json",
                    data=open("task/settings.json", "r", encoding="utf-8").read(),
                    file_name="settings.json",
                    mime="application/json",
                    help="Exporta o arquivo de configurações",
                    use_container_width=True,
                )
            elif action == "📥 Importar":
                uploaded_file = st.sidebar.file_uploader(
                    "Selecione o arquivo settings.json",
                    type=["json"],
                    help="Importa um arquivo de configurações",
                    label_visibility="collapsed"
                )
                if uploaded_file is not None:
                    try:
                        with open("task/settings.json", "w", encoding="utf-8") as f:
                            f.write(uploaded_file.getvalue().decode())
                        st.sidebar.success("Configurações importadas com sucesso!")
                        st.rerun()
                    except Exception as e:
                        st.sidebar.error(f"Erro ao importar configurações: {str(e)}")
            elif action == "🔄 Redefinir":
                if st.sidebar.button(
                    "Confirmar Redefinição",
                    help="Redefine todas as configurações para o padrão",
                    type="primary",
                    use_container_width=True
                ):
                    try:
                        with open("task/settings.template.json", "r", encoding="utf-8") as template_file:
                            template_content = template_file.read()
                        with open("task/settings.json", "w", encoding="utf-8") as settings_file:
                            settings_file.write(template_content)
                        st.sidebar.success("Configurações redefinidas com sucesso! Atualize a página")
                        sleep(5)
                        st.rerun()
                    except FileNotFoundError:
                        st.sidebar.error("Arquivo template não encontrado. Verifique se task/settings.template.json existe.")
                    except Exception as e:
                        st.sidebar.error(f"Erro ao redefinir configurações: {str(e)}")
            
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
            self.home_page.render()
        elif st.session_state.current_page == "connections":
            self.connections.render()
        elif st.session_state.current_page == "task":
            self.task_settings.render()
        elif st.session_state.current_page == "tables":
            self.tables.render()
        elif st.session_state.current_page == "filters":
            self.display_filter.render()
        elif st.session_state.current_page == "transformations":
            self.display_transformation.render()
        elif st.session_state.current_page == "error_handling":
            self.error_settings.render()

    def display_ui(self):
        """Método principal para exibir a interface completa."""
        self.display_sidebar()
        self.display_content()
