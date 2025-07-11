from trempy.IA.TaskCreator import TaskCreator
from pathlib import Path
from typing import Dict
import streamlit as st
import json


class DisplayTaskCreatorAI:
    """
    Classe responsável por gerenciar e exibir a interface de uso do Chatbot.

    Esta classe fornece uma interface gráfica para enviar prompts e receber da API do DeepSeek uma configuração
    de tarefa personalizada
    """

    SYSTEM_CONTENT_PATH = Path("trempy/Shared/README.md")

    def __load_system_content(self) -> str:
        with open(
            "trempy/IA/task_creator_tutorial_for_ia.md", "r", encoding="utf-8"
        ) as f:
            system_content = f.read()

        return system_content

    def __create_task_by_ia(self, system_content: str, user_content: str) -> Dict:
        chatbot = TaskCreator()
        response = chatbot.request(system_content, user_content)

        return response

    def render(self) -> None:
        """Exibe a interface para a utilização do TaskCreator AI"""
        st.header("🤖 TaskCreator AI")

        if "taskcreator_ai_state" not in st.session_state:
            st.session_state.taskcreator_ai_state = "prompt"
        if "taskcreator_ai_json" not in st.session_state:
            st.session_state.taskcreator_ai_json = None

        if "taskcreator_ai_prompt_value" not in st.session_state:
            st.session_state.taskcreator_ai_prompt_value = ""

        if st.session_state.taskcreator_ai_state == "prompt":
            # CSS customizado para aumentar o tamanho da fonte do text_area
            st.markdown(
                """
                <style>
                textarea {
                    font-size: 1.2em !important;
                }
                </style>
                """,
                unsafe_allow_html=True,
            )
            st.markdown(
                "<span style='font-size:1.3em; font-weight:600;'>Descreva em linguagem natural como você quer criar a tarefa:</span>",
                unsafe_allow_html=True,
            )
            prompt = st.text_area(
                label="",
                height=500,
                key="taskcreator_ai_prompt",
                value=st.session_state.taskcreator_ai_prompt_value,
                label_visibility="collapsed",
            )
            if st.button("Gerar Tarefa com IA", type="primary"):
                st.session_state.taskcreator_ai_prompt_value = prompt
                st.session_state.taskcreator_ai_state = "loading"
                st.rerun()
        elif st.session_state.taskcreator_ai_state == "loading":
            with st.spinner("A IA está pensando... Aguarde alguns segundos..."):
                system_content = self.__load_system_content()
                response = self.__create_task_by_ia(
                    system_content,
                    user_content=st.session_state.taskcreator_ai_prompt_value,
                )
                # Salva o JSON e as métricas no session_state
                st.session_state.taskcreator_ai_json = response["message_content"]
                st.session_state.taskcreator_ai_stats = {
                    "usage_prompt_tokens": response.get("usage_prompt_tokens", "-"),
                    "usage_completion_tokens": response.get("usage_completion_tokens", "-"),
                    "usage_total_tokens": response.get("usage_total_tokens", "-"),
                    "usage_cached_tokens": response.get("usage_cached_tokens", "-"),
                    "len_system_content": response.get("len_system_content", "-"),
                    "len_user_content": response.get("len_user_content", "-"),
                    "len_message_content": response.get("len_message_content", "-")
                }
                st.session_state.taskcreator_ai_state = "result"
                st.rerun()
        elif st.session_state.taskcreator_ai_state == "result":
            if st.session_state.taskcreator_ai_json:
                st.subheader("Configuração sugerida pela IA:")
                st.json(st.session_state.taskcreator_ai_json, expanded=True)

                # Exibir métricas da IA de forma destacada e bonita
                response_stats = st.session_state.get("taskcreator_ai_stats", {})
                if response_stats:
                    st.markdown("""
                    <style>
                    .metric-card {
                        background: #23272f;
                        border-radius: 12px;
                        padding: 18px 10px 10px 10px;
                        margin-bottom: 10px;
                        box-shadow: 0 2px 8px rgba(0,0,0,0.18);
                        text-align: center;
                        border: 1.5px solid #3b82f6;
                    }
                    .metric-title {
                        font-size: 1.1em;
                        font-weight: 600;
                        margin-bottom: 0.2em;
                        color: #e0e6ef;
                    }
                    .metric-value {
                        font-size: 1.7em;
                        font-weight: bold;
                        color: #60a5fa;
                    }
                    .metric-icon {
                        font-size: 1.5em;
                        margin-bottom: 0.2em;
                    }
                    </style>
                    """, unsafe_allow_html=True)
                    st.markdown("---")
                    st.markdown("#### Estatísticas da Geração da Tarefa")
                    metric_cols = st.columns(4)
                    metric_data = [
                        ("<span class='metric-icon'>📝</span><div class='metric-title'>Prompt tokens</div>", response_stats.get("usage_prompt_tokens", "-")),
                        ("<span class='metric-icon'>🤖</span><div class='metric-title'>Completion tokens</div>", response_stats.get("usage_completion_tokens", "-")),
                        ("<span class='metric-icon'>🔢</span><div class='metric-title'>Total tokens</div>", response_stats.get("usage_total_tokens", "-")),
                        ("<span class='metric-icon'>💾</span><div class='metric-title'>Tokens em cache</div>", response_stats.get("usage_cached_tokens", "-")),
                    ]
                    for i, (label, value) in enumerate(metric_data):
                        metric_cols[i].markdown(f"<div class='metric-card'>{label}<div class='metric-value'>{value}</div></div>", unsafe_allow_html=True)
                    st.markdown("---")

                cols = st.columns(4)
                with cols[0]:
                    if st.button("Aplicar", type="primary"):
                        with open("task/settings.json", "w", encoding="utf-8") as f:
                            json.dump(
                                st.session_state.taskcreator_ai_json,
                                f,
                                ensure_ascii=False,
                                indent=4,
                            )
                        st.success("Configuração salva!")
                        st.session_state.taskcreator_ai_applied = True
                with cols[1]:
                    if st.button("Recusar", type="secondary"):
                        st.session_state.taskcreator_ai_state = "prompt"
                        st.session_state.taskcreator_ai_json = None
                        st.rerun()
                with cols[2]:
                    pass
                with cols[3]:
                    pass

                # Se a configuração foi aplicada, mostrar botão para continuar
                if st.session_state.get("taskcreator_ai_applied", False):
                    if st.button("Continuar"):
                        st.session_state.taskcreator_ai_state = "prompt"
                        st.session_state.taskcreator_ai_json = None
                        st.session_state.taskcreator_ai_applied = False
                        st.rerun()
            
            else:
                st.error("Erro ao executar a API da IA. Verifique se DEEPSEEK_API_KEY está configurado na variável de ambiente corretamente.")
