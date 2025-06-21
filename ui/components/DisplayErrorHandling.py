from typing import Dict, Tuple, Any
from pathlib import Path
import streamlit as st
import json


class DisplayErrorHandling:
    """
    Classe responsável por gerenciar e exibir a interface de configuração de tratamento de erros.
    
    Esta classe fornece uma interface gráfica para configurar como o sistema deve se comportar
    quando encontrar diferentes tipos de erros durante as operações de banco de dados.
    """
    
    SETTINGS_PATH = Path("task/settings.json")
    
    # Configurações padrão para tratamento de erros
    DEFAULT_ERROR_SETTINGS = {
        "stop_if_insert_error": False,
        "stop_if_update_error": False,
        "stop_if_delete_error": False,
        "stop_if_upsert_error": False,
        "stop_if_scd2_error": False,
    }
    
    def __configure_default_settings(self) -> Tuple[Dict[str, bool], Dict[str, Any]]:
        """
        Carrega as configurações padrão e existentes para tratamento de erros.
        
        Returns:
            Tuple[Dict[str, bool], Dict[str, Any]]: Uma tupla contendo:
                - Dicionário com configurações de erro
                - Dicionário com todas as configurações
        """
        # Load existing settings (if they exist)
        all_settings = {}
        if self.SETTINGS_PATH.exists():
            try:
                with open(self.SETTINGS_PATH, "r", encoding="utf-8") as f:
                    all_settings = json.load(f)
            except Exception as e:
                st.warning(
                    f"Não foi possível carregar as configurações existentes: {e}"
                )

        # Merge defaults with existing settings (only for 'error_handling')
        current_error_settings = all_settings.get("error_handling", {})
        error_settings = {**self.DEFAULT_ERROR_SETTINGS, **current_error_settings}

        return error_settings, all_settings

    def __save_settings(self, all_settings: Dict[str, Any], error_settings: Dict[str, bool]) -> bool:
        """
        Salva as configurações de tratamento de erro no arquivo de configurações.
        
        Args:
            all_settings (Dict[str, Any]): Todas as configurações do sistema
            error_settings (Dict[str, bool]): Configurações de tratamento de erro
            
        Returns:
            bool: True se as configurações foram salvas com sucesso, False caso contrário
        """
        try:
            # Validação dos campos obrigatórios
            missing_fields = [
                field for field in self.DEFAULT_ERROR_SETTINGS.keys()
                if field not in error_settings
            ]
            if missing_fields:
                st.error(f"Campos obrigatórios não encontrados: {', '.join(missing_fields)}")
                return False

            # Garante que todos os valores são booleanos
            new_error_settings = {
                key: bool(error_settings.get(key, False))
                for key in self.DEFAULT_ERROR_SETTINGS.keys()
            }

            # Atualiza apenas a chave 'error_handling' mantendo outras intactas
            updated_settings = {
                **all_settings,
                "error_handling": new_error_settings,
            }

            # Garante que o diretório pai existe
            self.SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
            
            # Salva no arquivo
            with open(self.SETTINGS_PATH, "w", encoding="utf-8") as f:
                json.dump(updated_settings, f, indent=4, ensure_ascii=False)
                
            st.success("Configurações de tratamento de erro salvas com sucesso!")
            st.json(updated_settings)
            return True
            
        except PermissionError:
            st.error("Erro de permissão ao salvar o arquivo de configurações. Verifique as permissões da pasta.")
            return False
        except json.JSONDecodeError:
            st.error("Erro ao codificar as configurações em JSON. Verifique se não há caracteres inválidos.")
            return False
        except Exception as e:
            st.error(f"Erro inesperado ao salvar configurações: {str(e)}")
            return False

    def __render_dml_settings(self, error_settings: Dict[str, bool]) -> Dict[str, bool]:
        """
        Renderiza as configurações de tratamento de erro para operações DML.
        
        Args:
            error_settings (Dict[str, bool]): Configurações atuais
            
        Returns:
            Dict[str, bool]: Novas configurações de DML
        """
        st.subheader("Operações DML")
        return {
            "stop_if_insert_error": st.checkbox(
                "Parar se ocorrer erro no INSERT",
                value=error_settings.get("stop_if_insert_error", False),
                help="Se marcado, a execução será interrompida quando ocorrer um erro de INSERT",
            ),
            "stop_if_update_error": st.checkbox(
                "Parar se ocorrer erro no UPDATE",
                value=error_settings.get("stop_if_update_error", False),
                help="Se marcado, a execução será interrompida quando ocorrer um erro de UPDATE",
            ),
            "stop_if_delete_error": st.checkbox(
                "Parar se ocorrer erro no DELETE",
                value=error_settings.get("stop_if_delete_error", False),
                help="Se marcado, a execução será interrompida quando ocorrer um erro de DELETE",
            ),
        }
        
    def __render_cdc_settings(self, error_settings: Dict[str, bool]) -> Dict[str, bool]:
        """
        Renderiza as configurações de tratamento de erro para modos CDC.
        
        Args:
            error_settings (Dict[str, bool]): Configurações atuais
            
        Returns:
            Dict[str, bool]: Novas configurações de CDC
        """
        st.subheader("Modos de CDC")
        return {
            "stop_if_upsert_error": st.checkbox(
                "Parar se ocorrer erro no UPSERT",
                value=error_settings.get("stop_if_upsert_error", False),
                help="Se marcado, a execução será interrompida quando ocorrer um erro de UPSERT",
            ),
            "stop_if_scd2_error": st.checkbox(
                "Parar se ocorrer erro no SCD2",
                value=error_settings.get("stop_if_scd2_error", False),
                help="Se marcado, a execução será interrompida quando ocorrer um erro de SCD2",
            ),
        }

    def render(self) -> None:
        """
        Exibe a interface para configuração do tratamento de erros.
        
        Esta interface permite configurar como o sistema deve se comportar quando
        encontrar diferentes tipos de erros durante as operações de banco de dados.
        """
        st.header("Configurações de Tratamento de Erro")
        st.markdown(
            """
            Configure como o sistema deve se comportar quando encontrar erros durante
            as operações de banco de dados. Para cada tipo de operação, você pode escolher
            se o sistema deve parar a execução quando encontrar um erro ou continuar mesmo
            com erros.
            """
        )

        # Carrega configurações existentes
        error_settings, all_settings = self.__configure_default_settings()

        # Formulário de configurações
        col1, _ = st.columns(2)
        with col1:
            with st.form("error_settings_form"):
                col1, col2, _ = st.columns(3, gap="medium")
                
                # Configurações de DML
                with col1:
                    dml_settings = self.__render_dml_settings(error_settings)
                
                # Configurações de CDC
                with col2:
                    cdc_settings = self.__render_cdc_settings(error_settings)

                # Botão de salvar
                if st.form_submit_button("Salvar Configurações", type="primary"):
                    # Combina todas as configurações
                    new_error_settings = {**dml_settings, **cdc_settings}
                    self.__save_settings(all_settings, new_error_settings)