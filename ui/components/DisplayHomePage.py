from ui.components.tools.ReplicationEngine import ReplicationEngine
from ui.components.tools.GraphGenerator import GraphGenerator
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

    def get_new_log_entries(self) -> str:
        """
        Retorna as novas entradas do arquivo de log desde a última leitura.
        
        Returns:
            str: Novas entradas do log ou mensagem de erro em caso de falha.
        """
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
        
    def __display_status(self) -> None:
        """
        Exibe o painel de controle da replicação com botões de ação e status.
        """
        # Container principal com largura total
        main_container = st.container()

        with main_container:
            cols = st.columns([6, 12])

            with cols[0]:
                # Botões de controle
                control_cols = st.columns(4)
                
                # Botão Iniciar
                with control_cols[0]:
                    if st.button(
                        "Iniciar",
                        key="start",
                        type="primary",
                        help="Inicia o processo de replicação",
                    ):
                        self.replication_engine.start()

                # Botão Parar
                with control_cols[1]:
                    if st.button(
                        "Parar",
                        key="stop",
                        type="primary",
                        help="Para o processo de replicação",
                    ):
                        self.replication_engine.stop()

                # Botão Atualizar
                with control_cols[2]:
                    if st.button(
                        "Atualizar",
                        key="unique_refresh_button",
                        help="Atualiza a visualização dos logs",
                    ):
                        st.session_state.log_refresh = not st.session_state.get("log_refresh", False)

                # Indicador de Status
                with control_cols[3]:
                    if st.session_state.get("process") is None:
                        st.error("**PARADO**")
                    else:
                        st.success("**EXECUTANDO**")

    def __display_logs(self) -> None:
        """
        Exibe o conteúdo do arquivo de log em tempo real com opção de limpeza.
        """
        # Inicializa o estado de atualização dos logs se necessário
        if "log_refresh" not in st.session_state:
            st.session_state.log_refresh = False

        # Botão para limpar logs
        if st.button(
            "Limpar",
            help="Limpa todo o conteúdo do arquivo de log",
        ):
            try:
                with open(self.log_viewer.log_file, "w") as f:
                    f.truncate(0)
                st.success("Logs limpos com sucesso!")
                st.session_state.log_refresh = not st.session_state.log_refresh
            except Exception as e:
                st.error(f"Erro ao limpar logs: {str(e)}")

        # Exibe os logs
        with st.expander("Visualizar Logs", expanded=True):
            current_logs = self.log_viewer.get_new_log_entries()
            st.code(
                current_logs,
                language="log",
                line_numbers=False,
                height=500,
            )

    def __display_cdc_stats(self) -> None:
        """
        Exibe estatísticas relacionadas ao CDC (Change Data Capture).
        """
        # Gráfico principal
        self.graph_generator.generate_cdc_graph1()

        # Gráficos secundários em colunas
        col1, col2 = st.columns(2)
        with col1:
            self.graph_generator.generate_cdc_graph2()
        with col2:
            self.graph_generator.generate_cdc_graph3()

    def __display_errors_stats(self) -> None:
        """
        Exibe estatísticas relacionadas a erros ocorridos durante a replicação.
        """
        self.graph_generator.generate_errors_graph1()

    def __display_full_load_stats(self) -> None:
        """
        Exibe estatísticas relacionadas ao carregamento inicial (Full Load).
        """
        self.graph_generator.generate_fl_graph1()

    def display_home_page(self) -> None:
        """
        Exibe a página inicial com todas as suas seções.
        
        Esta é a função principal que organiza e exibe todos os componentes
        da página inicial, incluindo status, logs e estatísticas.
        """
        # Exibe o painel de controle
        self.__display_status()

        # Cria as abas para diferentes visualizações
        subtab1, subtab2, subtab3, subtab4 = st.tabs([
            "Logs",
            "Full Load Stats",
            "CDC Stats",
            "Errors"
        ])

        # Configura e exibe cada aba
        with subtab1:
            st.session_state.running = True
            self.__display_logs()

        with subtab2:
            self.__display_full_load_stats()

        with subtab3:
            self.__display_cdc_stats()

        with subtab4:
            self.__display_errors_stats()