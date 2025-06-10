from trempy.Metadata.MetadataConnectionManager import MetadataConnectionManager
from trempy.Loggings.Logging import ReplicationLogger
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

    def __graph_fl_01(self):
        try:
            with MetadataConnectionManager() as metadata_manager:
                df = metadata_manager.get_metadata_tables("stats_full_load")
        except:
            df = pl.DataFrame()

        st.table(df.to_pandas())
        
    def __graph_errors_01(self):
        try:
            with MetadataConnectionManager() as metadata_manager:
                df = metadata_manager.get_metadata_tables("apply_exceptions")
        except:
            df = pl.DataFrame()

        if df.is_empty():
            st.warning("Nenhum dado de erro encontrado")
            return
        
        # Convertendo para pandas para facilitar a manipulação no Streamlit
        df_pd = df.to_pandas()
        
        # Adicionando métricas resumidas
        st.subheader("Resumo de Erros")
        col1, col2, col3 = st.columns(3)
        col1.metric("Total de Erros", len(df_pd))
        col2.metric("Tipos de Erros Únicos", df_pd['type'].nunique())
        col3.metric("Códigos Únicos", df_pd['code'].nunique())
        
        # Criando colunas para os filtros
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Filtro por tipo de erro
            types = ['Todos'] + sorted(df_pd['type'].unique().tolist())
            selected_type = st.selectbox('Tipo de erro:', types)
        
        with col2:
            # Filtro por código de erro
            codes = ['Todos'] + sorted(df_pd['code'].unique().tolist())
            selected_code = st.selectbox('Código de erro:', codes)
        
        with col3:
            # Filtro por tabela
            tables = ['Todos'] + sorted(df_pd['table_name'].unique().tolist())
            selected_table = st.selectbox('Tabela:', tables)
        
        # Aplicando filtros
        if selected_type != 'Todos':
            df_pd = df_pd[df_pd['type'] == selected_type]
        
        if selected_code != 'Todos':
            df_pd = df_pd[df_pd['code'] == selected_code]
        
        if selected_table != 'Todos':
            df_pd = df_pd[df_pd['table_name'] == selected_table]
        
        # Configurações de estilo para a tabela
        st.markdown("""
        <style>
            .stDataFrame {
                width: 100%;
                height: 500px;
                overflow-y: auto;
            }
        </style>
        """, unsafe_allow_html=True)
        
        # Exibindo a tabela com rolagem
        st.dataframe(
            df_pd,
            column_config={
                "created_at": st.column_config.DatetimeColumn(
                    "Data/Hora",
                    format="YYYY-MM-DD HH:mm:ss",
                ),
                "message": st.column_config.TextColumn(
                    "Mensagem",
                    width="large",
                ),
                "query": st.column_config.TextColumn(
                    "Query",
                    width="large",
                )
            },
            hide_index=True,
            use_container_width=True
        )

    def __graph_cdc_01(self):
        
        try:
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
        except:
            df = pl.DataFrame()

        st.table(df.to_pandas())

    def __graph_cdc_02(self):
        try:
            data = {}
            with MetadataConnectionManager() as metadata_manager:
                df = metadata_manager.get_messages_stats().to_pandas()
                data = df.iloc[0].to_dict()
        except:
            pass
        finally:
            if not data:
                data = {
                    "quantity_operations": 0,
                    "published": 0,
                    "received": 0,
                    "processed": 0,
                }

        # Cálculo das diferenças
        bar_A = data["quantity_operations"] - data["published"]
        bar_B = data["published"] - data["received"]
        bar_C = max(data["received"] - data["processed"], 0)  # Evita valores negativos
        bar_D = (
            data["processed"] if data["processed"] < data["quantity_operations"] else 0
        )

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

    def __graph_cdc_03(self):
        try:
            with MetadataConnectionManager() as metadata_manager:
                df = metadata_manager.get_metadata_tables("stats_cdc")
                df = df.group_by(["task_name", "schema_name", "table_name"]).agg(
                    [
                        pl.sum("inserts"),
                        pl.sum("updates"),
                        pl.sum("deletes"),
                        pl.sum("errors"),
                        pl.sum("total"),
                    ]
                )

                # Calcular totais de cada operação
                total_inserts = df["inserts"].sum()
                total_updates = df["updates"].sum()
                total_deletes = df["deletes"].sum()

        except:
            total_inserts = 0
            total_updates = 0
            total_deletes = 0

        fig = px.pie(
            names=["Inserts", "Updates", "Deletes"],
            values=[total_inserts, total_updates, total_deletes],
            title="Operações CDC",
            color=["Inserts", "Updates", "Deletes"],
            color_discrete_map={
                "Inserts": "#118AB2",
                "Updates": "#FFD166",
                "Deletes": "#FF6B6B",
            },
        )

        st.plotly_chart(fig, use_container_width=True)

    def __display_cdc_stats(self):
        self.__graph_cdc_01()

        # Linha 2 - Gráficos lado a lado
        col1, col2 = st.columns(2)

        with col1:
            self.__graph_cdc_02()

        with col2:
            self.__graph_cdc_03()

    def __display_full_load_stats(self):
        self.__graph_fl_01()
        
    def __display_errors_stats(self):
        self.__graph_errors_01()

    def display_home_page(self):

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

    def display_filters(self):
        st.header("Filtros")

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
        st.title("TREMpy - Transactional Replication Engine for Multi-databases")

        tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(
            [
                "Página Inicial",
                "Conexões",
                "Configurações da Tarefa",
                "Tabelas",
                "Filtros",
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
            self.controller.display_filters()

        with tab6:
            self.controller.display_transformations()

        with tab7:
            self.controller.display_error_handling()


# Configuração inicial do logging
ReplicationLogger.configure_logging()

# Inicializa e executa a aplicação
if __name__ == "__main__":
    app = ReplicationApp()
