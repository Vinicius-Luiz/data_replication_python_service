from trempy.Loggings.Logging import ReplicationLogger
import streamlit as st
import subprocess
import os
import signal
import psutil
import platform
import sys


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


class ReplicationController:
    def __init__(self):
        self.logger = ReplicationLogger()
        self.python_path = sys.executable
        self.log_viewer = LogViewer()
        self._initialize_session_state()

    def _initialize_session_state(self):
        if "process" not in st.session_state:
            st.session_state.process = None

    def __start_replication(self):
        """Inicia o processo de replicação"""
        if st.session_state.process is None:
            try:
                st.session_state.process = subprocess.Popen(
                    [self.python_path, "manager.py"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    creationflags=(
                        subprocess.CREATE_NEW_PROCESS_GROUP
                        if platform.system() == "Windows"
                        else 0
                    ),
                )
                st.success("Replicação de dados iniciada com sucesso!")
                self.logger.info("UI - Replicação de dados iniciada")
            except Exception as e:
                st.error(f"Erro ao iniciar a replicação: {e}")
                self.logger.error(f"UI - Erro ao iniciar replicação: {e}")

    def __stop_replication(self):
        """Para o processo de replicação (manager.py) sem afetar o app.py"""
        if st.session_state.process is not None:
            try:
                parent = psutil.Process(st.session_state.process.pid)

                if platform.system() == "Windows":
                    os.system(f"taskkill /pid {parent.pid} /f /t")
                else:
                    os.killpg(os.getpgid(parent.pid), signal.SIGINT)

                try:
                    st.session_state.process.wait(timeout=5)
                    st.success("Replicação de dados parada com sucesso!")
                    self.logger.info("UI - Replicação de dados finalizada")
                except subprocess.TimeoutExpired:
                    st.error(
                        "Não foi possível parar a replicação graciosamente. Processo foi terminado."
                    )
                    self.logger.warning("UI - Replicação finalizada forçadamente")
                finally:
                    st.session_state.process = None

            except Exception as e:
                st.error(f"Erro ao parar a replicação: {e}")
                self.logger.error(f"UI - Erro ao parar replicação: {e}")
                st.session_state.process = None

    def __display_status(self):
        """Exibe o status atual da replicação"""
        if st.session_state.process is None:
            st.warning("Replicação de dados não está em execução")
        else:
            st.success("Replicação de dados está em execução")

    def __display_logs(self):
        """Exibe o conteúdo do arquivo de log em tempo real"""
        st.subheader("Visualização do Log em Tempo Real")

        # Variável de estado para forçar atualização
        if "log_refresh" not in st.session_state:
            st.session_state.log_refresh = False

        with st.expander("Visualizar Logs", expanded=True):
            # Botão que altera o estado (não a key)
            if st.button("Atualizar Logs", key="unique_refresh_button"):
                st.session_state.log_refresh = not st.session_state.log_refresh

            # Exibe os logs com syntax highlighting
            current_logs = self.log_viewer.get_new_log_entries()
            st.code(
                current_logs,
                language="log",  # Formatação específica para logs
                line_numbers=False,  # Opcional: mostra numeração de linhas
                height=300,
            )

    def __display_instructions(self):
        """Exibe as instruções de uso"""
        st.markdown(
            """
        ### Instruções:
        1. Clique em **Iniciar Replicação** para começar o processo
        2. Clique em **Parar Replicação** para interromper o processo
        3. Os logs aparecerão automaticamente na seção abaixo
        """
        )

    def display_execute(self):
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Iniciar Replicação", key="start"):
                self.__start_replication()

        with col2:
            if st.button("Parar Replicação", key="stop"):
                self.__stop_replication()

        self.__display_status()
        self.__display_instructions()

        # Seção de logs
        st.session_state.running = True
        self.__display_logs()

    def display_connections(self):
        st.header("Conexões")

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

    def display_task_settings(self):
        st.header("Configurações da Tarefa")

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

    def display_tables(self):
        st.header("Tabelas")

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

    def display_transformations(self):
        st.header("Transformações")

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

    def display_error_handling(self):
        st.header("Error Handling")

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


class ReplicationApp:
    def __init__(self):
        st.set_page_config(layout="wide", initial_sidebar_state="expanded")
        self.controller = ReplicationController()
        self._setup_ui()

    def _setup_ui(self):
        """Configura a interface do usuário"""
        st.title("Controle de Replicação de Dados")

        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(
            [
                "Executar",
                "Conexões",
                "Configurações da Tarefa",
                "Tabelas",
                "Transformações",
                "Error Handling",
            ]
        )

        with tab1:
            self.controller.display_execute()

        with tab2:
            self.controller.display_connections()

        with tab3:
            self.controller.display_task_settings()

        with tab4:
            self.controller.display_tables()

        with tab5:
            self.controller.display_transformations()

        with tab6:
            self.controller.display_error_handling()


# Configuração inicial do logging
ReplicationLogger.configure_logging()

# Inicializa e executa a aplicação
if __name__ == "__main__":
    app = ReplicationApp()
