from trempy.Loggings.Logging import ReplicationLogger
import streamlit as st
import subprocess
import os
import signal
import psutil
import platform
import sys
import time
from pathlib import Path


class LogViewer:
    def __init__(self, log_file='app.log'):
        self.log_file = log_file
        self.last_position = 0
        
    def get_new_log_entries(self):
        """Retorna as novas entradas do arquivo de log"""
        try:
            with open(self.log_file, 'r', encoding='utf-8') as f:
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

    def start_replication(self):
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

    def stop_replication(self):
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
                    st.error("Não foi possível parar a replicação graciosamente. Processo foi terminado.")
                    self.logger.warning("UI - Replicação finalizada forçadamente")
                finally:
                    st.session_state.process = None

            except Exception as e:
                st.error(f"Erro ao parar a replicação: {e}")
                self.logger.error(f"UI - Erro ao parar replicação: {e}")
                st.session_state.process = None

    def display_status(self):
        """Exibe o status atual da replicação"""
        if st.session_state.process is None:
            st.warning("Replicação de dados não está em execução")
        else:
            st.success("Replicação de dados está em execução")

    def display_logs(self):
        """Exibe o conteúdo do arquivo de log em tempo real"""
        st.subheader("Visualização do Log em Tempo Real")
        
        # Cria um expander para os logs
        with st.expander("Visualizar Logs", expanded=True):
            # Cria o text_area uma única vez fora do loop
            log_display = st.empty()
            
            # Atualiza os logs a cada 2 segundos
            while True:
                new_logs = self.log_viewer.get_new_log_entries()
                if new_logs:
                    # Atualiza o conteúdo sem recriar o elemento
                    log_display.text_area(
                        "",
                        value=new_logs,
                        height=300,
                        key=f"log_content_{time.time()}"  # Chave única usando timestamp
                    )
                time.sleep(2)
                
                # Interrompe o loop se o usuário sair da página
                if not st.session_state.get('running', True):
                    break

    def display_instructions(self):
        """Exibe as instruções de uso"""
        st.markdown("""
        ### Instruções:
        1. Clique em **Iniciar Replicação** para começar o processo
        2. Clique em **Parar Replicação** para interromper o processo
        3. Os logs aparecerão automaticamente na seção abaixo
        """)


class ReplicationApp:
    def __init__(self):
        st.set_page_config(layout="wide", initial_sidebar_state="expanded")
        self.controller = ReplicationController()
        self._setup_ui()

    def _setup_ui(self):
        """Configura a interface do usuário"""
        st.title("Controle de Replicação de Dados")
        
        # Seção de controle
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Iniciar Replicação", key="start"):
                self.controller.start_replication()
        
        with col2:
            if st.button("Parar Replicação", key="stop"):
                self.controller.stop_replication()
        
        self.controller.display_status()
        self.controller.display_instructions()
        
        # Seção de logs
        st.session_state.running = True
        self.controller.display_logs()


# Configuração inicial do logging
ReplicationLogger.configure_logging()

# Inicializa e executa a aplicação
if __name__ == "__main__":
    app = ReplicationApp()