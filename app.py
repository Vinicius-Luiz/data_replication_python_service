from trempy.Metadata.MetadataConnectionManager import MetadataConnectionManager
from trempy.Loggings.Logging import ReplicationLogger
import matplotlib.pyplot as plt
import plotly.express as px
import streamlit as st
import pandas as pd
import polars as pl
import subprocess
import platform
import signal
import psutil
import sys
import os


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
        # Container principal com largura total
        main_container = st.container()

        with main_container:
            cols = st.columns([6, 12])

            with cols[0]:
                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    if st.button("Iniciar", key="start"):
                        self.__start_replication()

                with col2:
                    if st.button("Parar", key="stop"):
                        self.__stop_replication()

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

        # Variável de estado para forçar atualização
        if "log_refresh" not in st.session_state:
            st.session_state.log_refresh = False

        with st.expander("Visualizar Logs", expanded=True):
            # Exibe os logs com syntax highlighting
            current_logs = self.log_viewer.get_new_log_entries()
            st.code(
                current_logs,
                language="log",  # Formatação específica para logs
                line_numbers=False,  # Opcional: mostra numeração de linhas
                height=300,
            )

    def __graph_cdc_01(self):
        with MetadataConnectionManager() as metadata_manager:
            df = metadata_manager.get_metadata_tables("stats_cdc")
            df = (
                df.group_by(["task_name", "schema_name", "table_name"])
                .agg(
                    [
                        pl.sum("inserts"),
                        pl.sum("updates"),
                        pl.sum("deletes"),
                        pl.sum("errors"),
                        pl.sum("total"),
                    ]
                )
                .sort(["task_name", "schema_name", "table_name"])
            )

        st.table(df.to_pandas())

    def __graph_cdc_02(self):
        try:
            data = {}
            with MetadataConnectionManager() as metadata_manager:
                df = metadata_manager.get_messages_stats().to_pandas()
                data = df.iloc[0].to_dict()
        except:
            return
        finally:
            if not data:
                return

        # Cálculo das diferenças
        bar_A = data["quantity_operations"] - data["published"]
        bar_B = data["published"] - data["received"]
        bar_C = max(data["received"] - data["processed"], 0)  # Evita valores negativos
        bar_D = data["processed"] if data["processed"] < data["quantity_operations"] else 0

        # Criar DataFrame para o Plotly
        df_plotly = pd.DataFrame(
            {
                "Estágio": [
                    "Não publicados",
                    "Publicados na fila",
                    "Em processamento",
                    "Processado",
                ],
                "Quantidade": [bar_A, bar_B, bar_C, bar_D],
                "Cor": [
                    "#FF6B6B",
                    "#FFD166",
                    "#06D6A0",
                    "#118AB2",
                ],  # Cores personalizadas
            }
        )

        # Gráfico de barras interativo
        fig = px.bar(
            df_plotly,
            x="Estágio",
            y="Quantidade",
            color="Cor",  # Cores baseadas na coluna 'Cor'
            title="Estatísticas de Transações em processamento",
            labels={"Quantidade": "", "Estágio": ""},
            text="Quantidade",
            color_discrete_map="identity",  # Usa cores diretamente da coluna
        )

        # Ajustar formatação
        fig.update_traces(
            texttemplate="%{text:,}",  # Formato com separador de milhar
            textposition="outside",
            marker_line_color="black",
            marker_line_width=1,
        )
        fig.update_layout(
            yaxis_range=[
                0,
                data["quantity_operations"] * 1.05,
            ],  # Margem de 5% no eixo Y
            showlegend=False,  # Remove a legenda (as cores já são autoexplicativas)
        )

        # Exibir no Streamlit
        st.plotly_chart(fig, use_container_width=True)

    def __display_cdc_stats(self):
        self.__graph_cdc_01()

        # Linha 2 - Gráficos lado a lado
        col1, col2 = st.columns(2)

        with col1:
            self.__graph_cdc_02()

        with col2:
            st.subheader("Distribuição de Eventos (Pizza)")

    def display_home_page(self):

        self.__display_status()

        subtab1, subtab2, subtab3, subtab4 = st.tabs(
            ["Logs", "Full Load Stats", "CDC Stats", "Errors"]
        )

        with subtab1:
            st.session_state.running = True
            self.__display_logs()

        with subtab2:
            st.write("Full Load Stats")

        with subtab3:
            self.__display_cdc_stats()

        with subtab4:
            st.write("Errors")

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
                "Página Inicial",
                "Conexões",
                "Configurações da Tarefa",
                "Tabelas",
                "Transformações",
                "Error Handling",
            ]
        )

        with tab1:
            self.controller.display_home_page()

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
