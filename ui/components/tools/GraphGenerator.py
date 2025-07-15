from trempy.Metadata.MetadataConnectionManager import MetadataConnectionManager
import plotly.express as px
import streamlit as st
import pandas as pd
import polars as pl


class GraphGenerator:
    def generate_fl_graph1(self):
        """Gera gráfico para Full Load"""
        try:
            with MetadataConnectionManager() as metadata_manager:
                df = metadata_manager.get_metadata_tables("stats_full_load")
        except:
            df = pl.DataFrame()

        st.table(df.to_pandas())

    def generate_errors_graph1(self):
        """Gera gráfico de erros"""
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
        col1, col2, col3 = st.columns(3)
        col1.metric("Total de Erros", len(df_pd))
        col2.metric("Tipos de Erros Únicos", df_pd["type"].nunique())
        col3.metric("Códigos Únicos", df_pd["code"].nunique())

        # Criando colunas para os filtros
        col1, col2, col3 = st.columns(3)

        with col1:
            # Filtro por tipo de erro
            types = ["Todos"] + sorted(df_pd["type"].unique().tolist())
            selected_type = st.selectbox("Tipo de erro:", types)

        with col2:
            # Filtro por código de erro
            codes = ["Todos"] + sorted(df_pd["code"].unique().tolist())
            selected_code = st.selectbox("Código de erro:", codes)

        with col3:
            # Filtro por tabela
            tables = ["Todos"] + sorted(df_pd["table_name"].unique().tolist())
            selected_table = st.selectbox("Tabela:", tables)

        # Aplicando filtros
        if selected_type != "Todos":
            df_pd = df_pd[df_pd["type"] == selected_type]

        if selected_code != "Todos":
            df_pd = df_pd[df_pd["code"] == selected_code]

        if selected_table != "Todos":
            df_pd = df_pd[df_pd["table_name"] == selected_table]

        # Configurações de estilo para a tabela
        st.markdown(
            """
        <style>
            .stDataFrame {
                width: 100%;
                height: 500px;
                overflow-y: auto;
            }
        </style>
        """,
            unsafe_allow_html=True,
        )

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
                ),
            },
            hide_index=True,
            use_container_width=True,
        )

    def generate_cdc_graph1(self):
        """Gera primeiro gráfico de CDC"""
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

    def generate_cdc_graph2(self):
        """Gera segundo gráfico de CDC"""
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

    def generate_cdc_graph3(self):
        """Gera terceiro gráfico de CDC"""
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
