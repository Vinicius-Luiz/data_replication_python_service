from ui.components.tools.ReplicationEngine import ReplicationEngine
from ui.components.tools.GraphGenerator import GraphGenerator
from trempy.Metadata.MetadataConnectionManager import MetadataConnectionManager
from typing import Optional
from pathlib import Path
import streamlit as st


class LogViewer:
    """
    Classe responsável por gerenciar a visualização de logs em tempo real.

    Esta classe fornece funcionalidades para ler e exibir novas entradas
    de um arquivo de log, mantendo o controle da última posição lida.
    """

    def __init__(self, log_file: str = "app.log"):
        """
        Inicializa o visualizador de logs.

        Args:
            log_file (str): Caminho para o arquivo de log. Padrão é 'app.log'.
        """
        self.log_file = Path(log_file)
        self.last_position = 0
        self.page_size = 100  # Número de linhas por página
        self.log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

    def parse_log_line(self, line: str) -> dict | None:
        """
        Parseia uma linha de log para extrair informações estruturadas.

        Args:
            line (str): Linha do log para parsear

        Returns:
            dict | None: Dicionário com as informações parseadas ou None se a linha for inválida
        """
        try:
            # Formato esperado: "2025-06-28 02:48:03,647 - INFO - UI - Replicação de dados iniciada"
            parts = line.split(" - ", 2)
            if len(parts) >= 3:
                timestamp = parts[0]
                level = parts[1].strip()
                message = parts[2].strip()
                return {
                    "timestamp": timestamp,
                    "level": level,
                    "message": message,
                    "raw": line,
                }
            return None
        except:
            return None

    def get_filtered_logs(
        self, selected_levels=None, search_text="", page=1
    ) -> tuple[list, int]:
        """
        Retorna as entradas do arquivo de log filtradas e paginadas.

        Args:
            selected_levels (list): Lista de níveis de log para filtrar
            search_text (str): Texto para filtrar nas mensagens
            page (int): Número da página atual

        Returns:
            tuple[list, int]: Lista de logs filtrados e total de páginas
        """
        try:
            all_logs = []
            with open(self.log_file, "r", encoding="utf-8") as f:
                for line in f:
                    parsed = self.parse_log_line(line.strip())
                    if parsed:
                        # Aplica filtros
                        if selected_levels and parsed["level"] not in selected_levels:
                            continue
                        if (
                            search_text
                            and search_text.lower() not in parsed["raw"].lower()
                        ):
                            continue
                        all_logs.append(parsed)

            # Calcula paginação
            total_logs = len(all_logs)
            total_pages = (total_logs + self.page_size - 1) // self.page_size
            start_idx = (page - 1) * self.page_size
            end_idx = start_idx + self.page_size

            return all_logs[start_idx:end_idx], total_pages

        except FileNotFoundError:
            return [], 0
        except Exception as e:
            st.error(f"Erro ao ler o arquivo de log: {str(e)}")
            return [], 0


class DisplayHomePage:
    """
    Classe responsável por gerenciar e exibir a página inicial da aplicação.

    Esta classe fornece uma interface gráfica para controlar a replicação,
    visualizar logs em tempo real e exibir estatísticas através de gráficos.
    """

    def __init__(self):
        """Inicializa a página inicial com seus componentes."""
        self.replication_engine = ReplicationEngine()
        self.graph_generator = GraphGenerator()
        self.log_viewer = LogViewer()
        self.metadata_manager = MetadataConnectionManager()

    def __get_metrics(self):
        """Obtém as métricas do sistema de replicação."""
        try:
            with self.metadata_manager as metadata_manager:
                # Obtém estatísticas de mensagens
                messages_stats = metadata_manager.get_messages_stats()
                if not messages_stats.is_empty():
                    total_ops = messages_stats["quantity_operations"][0]
                else:
                    total_ops = 0

                # Obtém estatísticas de CDC
                stats_cdc = metadata_manager.get_metadata_tables("stats_cdc")
                if not stats_cdc.is_empty():
                    total_errors = stats_cdc["errors"].sum()
                    total_operations = stats_cdc["total"].sum()
                    error_rate = (
                        (total_errors / total_operations * 100)
                        if total_operations > 0
                        else 0
                    )
                else:
                    error_rate = 0

                # Obtém total de registros do full load
                stats_fl = metadata_manager.get_metadata_tables("stats_full_load")
                if not stats_fl.is_empty():
                    total_records = stats_fl["records"].sum()
                else:
                    total_records = 0

                return {
                    "total_ops": f"{total_ops:,}".replace(",", "."),
                    "total_records": f"{total_records:,}".replace(",", "."),
                    "error_rate": f"{100 - error_rate:.1f}%",
                }
        except Exception as e:
            return {"total_ops": "N/A", "total_records": "N/A", "error_rate": "N/A"}

    def __display_header(self) -> None:
        """
        Exibe o cabeçalho da página com informações importantes.
        """
        metrics = self.__get_metrics()

        st.markdown(
            """
            <style>
                .main-header {
                    font-size: 2.5em;
                    font-weight: bold;
                    margin-bottom: 1em;
                    color: #1E88E5;
                }
                .metric-card {
                    background-color: #1E1E1E;
                    border-radius: 10px;
                    padding: 1.5em;
                    box-shadow: 0 2px 8px rgba(0,0,0,0.3);
                    text-align: center;
                    border: 1px solid #333;
                }
                .metric-value {
                    font-size: 2em;
                    font-weight: bold;
                    color: #1E88E5;
                }
                .metric-label {
                    font-size: 0.9em;
                    color: #BBB;
                    margin-top: 0.5em;
                }
                .status-running {
                    color: #4CAF50;
                    font-weight: bold;
                }
                .status-stopped {
                    color: #f44336;
                    font-weight: bold;
                }
            </style>
            """,
            unsafe_allow_html=True,
        )

        # Métricas em cards
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.markdown(
                f"""
                <div class="metric-card">
                    <div class="metric-value">{metrics['total_records']}</div>
                    <div class="metric-label">Registros Full Load</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with col2:
            st.markdown(
                f"""
                <div class="metric-card">
                    <div class="metric-value">{metrics['total_ops']}</div>
                    <div class="metric-label">Operações CDC</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with col3:
            st.markdown(
                f"""
                <div class="metric-card">
                    <div class="metric-value">{metrics['error_rate']}</div>
                    <div class="metric-label">Taxa de Sucesso</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with col4:
            status_class = (
                "status-running"
                if self.replication_engine.is_running()
                else "status-stopped"
            )
            status_text = (
                "Em Execução" if self.replication_engine.is_running() else "Parado"
            )
            st.markdown(
                f"""
                <div class="metric-card">
                    <div class="metric-value {status_class}">{status_text}</div>
                    <div class="metric-label">Status do Sistema</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    def __display_control_panel(self) -> None:
        """
        Exibe o painel de controle da replicação com botões de ação.
        """
        st.markdown(
            """
            <style>
                div.stButton > button {
                    width: 100%;
                    padding: 12px;
                    border-radius: 8px;
                    font-weight: bold;
                    transition: all 0.3s ease;
                }
                div.stButton > button:hover {
                    transform: translateY(-2px);
                    box-shadow: 0 4px 8px rgba(0,0,0,0.1);
                }
                .control-panel {
                    background-color: #1E1E1E;
                    border-radius: 10px;
                    padding: 20px;
                    margin: 20px 0;
                    box-shadow: 0 2px 8px rgba(0,0,0,0.3);
                    border: 1px solid #333;
                }
            </style>
            """,
            unsafe_allow_html=True,
        )

        with st.container():
            st.markdown('<div class="control-panel">', unsafe_allow_html=True)

            # Layout dos controles em grid
            col1, col2, col3 = st.columns([1, 1, 1])

            with col1:
                start_btn = st.button(
                    "▶️ Iniciar Replicação",
                    key="start",
                    type="primary",
                    help="Inicia o processo de replicação",
                    use_container_width=True,
                )
                if start_btn:
                    self.replication_engine.start()

            with col2:
                stop_btn = st.button(
                    "⏹️ Parar Replicação",
                    key="stop",
                    type="primary",
                    help="Para o processo de replicação",
                    use_container_width=True,
                )
                if stop_btn:
                    self.replication_engine.stop()

            with col3:
                refresh_btn = st.button(
                    "🔄 Atualizar Dados",
                    key="unique_refresh_button",
                    help="Atualiza a visualização dos logs e estatísticas",
                    use_container_width=True,
                )
                if refresh_btn:
                    st.session_state.log_refresh = not st.session_state.get(
                        "log_refresh", False
                    )

            st.markdown("</div>", unsafe_allow_html=True)

    def __display_logs(self) -> None:
        """
        Exibe o conteúdo do arquivo de log em tempo real com opção de limpeza.
        """
        if "log_refresh" not in st.session_state:
            st.session_state.log_refresh = False
        if "log_page" not in st.session_state:
            st.session_state.log_page = 1
        if "log_levels" not in st.session_state:
            st.session_state.log_levels = self.log_viewer.log_levels.copy()
        if "log_search" not in st.session_state:
            st.session_state.log_search = ""

        # Container para logs com estilo moderno
        st.markdown(
            """
            <style>
                .log-container {
                    background-color: #1E1E1E;
                    border-radius: 10px;
                    padding: 20px;
                    margin: 20px 0;
                    border: 1px solid #333;
                }
                .log-header {
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    margin-bottom: 15px;
                }
                .stSelectbox {
                    min-width: 200px;
                }
            </style>
            """,
            unsafe_allow_html=True,
        )

        with st.container():
            # Cabeçalho com título e botão de limpar
            col1, col2, col3 = st.columns([4, 1, 1])
            with col1:
                st.subheader("📋 Logs do Sistema")
            with col2:
                try:
                    with open(self.log_viewer.log_file, "r", encoding="utf-8") as f:
                        log_content = f.read()
                    st.download_button(
                        label="📥 Baixar Log",
                        data=log_content.encode("utf-8"),
                        file_name="app.log",
                        mime="text/plain",
                        help="Baixa o arquivo de log completo",
                        type="secondary",
                    )
                except Exception as e:
                    st.error(f"Erro ao baixar logs: {str(e)}")
            with col3:
                if st.button(
                    "🗑️ Limpar Logs",
                    help="Limpa todo o conteúdo do arquivo de log",
                    type="secondary",
                ):
                    try:
                        with open(self.log_viewer.log_file, "w") as f:
                            f.truncate(0)
                        st.success("Logs limpos com sucesso!")
                        st.session_state.log_refresh = not st.session_state.log_refresh
                        st.session_state.log_page = 1
                    except Exception as e:
                        st.error(f"Erro ao limpar logs: {str(e)}")

            # Filtros
            col1, col2 = st.columns([2, 1])
            with col1:
                st.session_state.log_search = st.text_input(
                    "🔍 Buscar nos logs",
                    value=st.session_state.log_search,
                    placeholder="Digite para filtrar mensagens...",
                )
            with col2:
                st.session_state.log_levels = st.multiselect(
                    "Níveis de Log",
                    options=self.log_viewer.log_levels,
                    default=st.session_state.log_levels,
                    help="Selecione os níveis de log que deseja visualizar",
                )

            # Busca logs filtrados e paginados
            logs, total_pages = self.log_viewer.get_filtered_logs(
                selected_levels=st.session_state.log_levels,
                search_text=st.session_state.log_search,
                page=st.session_state.log_page,
            )

            # Exibe os logs
            if logs:
                # Formata os logs para exibição
                formatted_logs = []
                for log in logs:
                    level_color = {
                        "DEBUG": "🔵",
                        "INFO": "⚪",
                        "WARNING": "🟡",
                        "ERROR": "🔴",
                        "CRITICAL": "⛔",
                    }.get(log["level"], "⚪")

                    formatted_logs.append(
                        f"{log['timestamp']} {level_color} {log['level']} - {log['message']}"
                    )

                st.code(
                    "\n".join(formatted_logs),
                    language="log",
                    line_numbers=True,
                )
            else:
                st.info("Nenhum log encontrado com os filtros selecionados.")

            # Controles de paginação
            if total_pages > 1:
                col1, col2, col3 = st.columns([1, 3, 1])
                with col1:
                    if st.button("⬅️ Anterior", disabled=st.session_state.log_page <= 1):
                        st.session_state.log_page -= 1
                with col2:
                    st.write(f"Página {st.session_state.log_page} de {total_pages}")
                with col3:
                    if st.button(
                        "Próxima ➡️", disabled=st.session_state.log_page >= total_pages
                    ):
                        st.session_state.log_page += 1

    def __display_statistics(self) -> None:
        """
        Exibe as estatísticas em um layout moderno com tabs.
        """
        st.markdown(
            """
            <style>
                .stTabs {
                    background-color: #1E1E1E;
                    border-radius: 10px;
                    padding: 20px;
                    margin: 20px 0;
                    box-shadow: 0 2px 8px rgba(0,0,0,0.3);
                    border: 1px solid #333;
                }
                .stTab {
                    background-color: #2D2D2D;
                    border-radius: 8px;
                    margin: 10px 0;
                    padding: 15px;
                }
            </style>
            """,
            unsafe_allow_html=True,
        )

        st.subheader("📊 Estatísticas de Replicação")

        tab1, tab2, tab3 = st.tabs(["📈 Full Load", "🔄 CDC", "⚠️ Erros"])

        with tab1:
            self.__display_full_load_stats()

        with tab2:
            self.__display_cdc_stats()

        with tab3:
            self.__display_errors_stats()

    def __display_cdc_stats(self) -> None:
        """Exibe estatísticas relacionadas ao CDC."""
        self.graph_generator.generate_cdc_graph1()
        col1, col2 = st.columns(2)
        with col1:
            self.graph_generator.generate_cdc_graph2()
        with col2:
            self.graph_generator.generate_cdc_graph3()

    def __display_errors_stats(self) -> None:
        """Exibe estatísticas relacionadas a erros."""
        self.graph_generator.generate_errors_graph1()

    def __display_full_load_stats(self) -> None:
        """Exibe estatísticas relacionadas ao Full Load."""
        self.graph_generator.generate_fl_graph1()

    def render(self) -> None:
        """
        Exibe a página inicial com todas as suas seções.

        Esta é a função principal que organiza e exibe todos os componentes
        da página inicial, incluindo status, logs e estatísticas.
        """
        # Exibe o cabeçalho com métricas
        self.__display_header()

        # Exibe o painel de controle
        self.__display_control_panel()

        # Exibe logs e estatísticas
        col1, col2 = st.columns([1, 1])

        with col1:
            self.__display_logs()

        with col2:
            self.__display_statistics()
