from ui.components.ReplicationEngine import ReplicationEngine
from ui.components.GraphGenerator import GraphGenerator
import streamlit as st


class LogViewer:
    def __init__(self, log_file="app.log"):
        self.log_file = log_file
        self.last_position = 0

    def get_new_log_entries(self):
        """Retorna as novas entradas do arquivo de log"""
        try:
            with open(self.log_file, "r", encoding="utf-8") as f:
                # Vai para a última posição conhecida
                f.seek(self.last_position)
                new_content = f.read()
                # Atualiza a última posição
                self.last_position = f.tell()
                return new_content
        except FileNotFoundError:
            return "Arquivo de log não encontrado. A replicação pode não ter gerado logs ainda."
        except Exception as e:
            return f"Erro ao ler o arquivo de log: {str(e)}"


class UIComponents:

    def __init__(self):
        self.replication_engine = ReplicationEngine()
        self.graph_generator = GraphGenerator()
        self.log_viewer = LogViewer()

    def __display_status(self):
        # Container principal com largura total
        main_container = st.container()

        with main_container:
            cols = st.columns([6, 12])

            with cols[0]:
                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    if st.button("Iniciar", key="start"):
                        self.replication_engine.start()

                with col2:
                    if st.button("Parar", key="stop"):
                        self.replication_engine.stop()

                with col3:
                    if st.button("Atualizar", key="unique_refresh_button"):
                        st.session_state.log_refresh = not st.session_state.log_refresh

                with col4:
                    if st.session_state.process is None:
                        st.error("**PARADO**")
                    else:
                        st.success("**EXECUTANDO**")

    def __display_logs(self):
        """Exibe o conteúdo do arquivo de log em tempo real"""
        st.subheader("Visualização do Log em Tempo Real")

        if "log_refresh" not in st.session_state:
            st.session_state.log_refresh = False

        with st.expander("Visualizar Logs", expanded=True):
            current_logs = self.log_viewer.get_new_log_entries()
            st.code(
                current_logs,
                language="log",
                line_numbers=False,
                height=400,
            )

    def __display_cdc_stats(self):
        self.graph_generator.generate_cdc_graph1()

        col1, col2 = st.columns(2)

        with col1:
            self.graph_generator.generate_cdc_graph2()

        with col2:
            self.graph_generator.generate_cdc_graph3()

    def __display_errors_stats(self):
        self.graph_generator.generate_errors_graph1()

    def __display_full_load_stats(self):
        self.graph_generator.generate_fl_graph1()

    def display_home_page(self):
        """Exibe a página inicial"""
        self.__display_status()

        subtab1, subtab2, subtab3, subtab4 = st.tabs(
            ["Logs", "Full Load Stats", "CDC Stats", "Errors"]
        )

        with subtab1:
            st.session_state.running = True
            self.__display_logs()

        with subtab2:
            self.__display_full_load_stats()

        with subtab3:
            self.__display_cdc_stats()

        with subtab4:
            self.__display_errors_stats()

    def display_connections(self):
        """Exibe configurações de conexão"""
        self.__display_default("Conexões")

    def display_task_settings(self):
        """Exibe configurações da tarefa"""
        self.__display_default("Configurações da Tarefa")

    def display_tables(self):
        """Exibe configurações de tabelas"""
        self.__display_default("Tabelas")

    def display_filters(self):
        """Exibe configurações de filtros"""
        self.__display_default("Filtros")

    def display_transformations(self):
        """Exibe configurações de transformações"""
        self.__display_default("Transformações")

    def display_error_handling(self):
        """Exibe configurações de tratamento de erros"""
        self.__display_default("Error Handling")

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
