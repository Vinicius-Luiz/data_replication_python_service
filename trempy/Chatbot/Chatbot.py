from trempy.Loggings.Logging import ReplicationLogger
from typing import Any, Dict, Optional
from dotenv import load_dotenv
from openai import OpenAI
import csv
import os

ReplicationLogger.configure_logging()

class Chatbot:
    def __init__(
        self, api_key: Optional[str] = None, base_url: str = "https://api.deepseek.com"
    ):
        load_dotenv()
        self.api_key = api_key or os.getenv("DEEPSEEK_API_KEY")
        self.base_url = base_url
        self.client = OpenAI(api_key=self.api_key, base_url=self.base_url)
        self.log_file = os.path.join(os.getcwd(), "chatbot_stats.csv")
        self.logger = ReplicationLogger()
        self.logger.info("Chatbot inicializado com sucesso.")

    def request(
        self, system_content: str, user_content: str, **kwargs
    ) -> Dict[str, Any]:
        params = {
            "model": "deepseek-chat",
            "messages": [
                {"role": "system", "content": system_content},
                {"role": "user", "content": user_content},
            ],
            "stream": False,
            "response_format": {"type": "json_object"},
            "temperature": 0,  # Coding / Math
        }
        params.update(kwargs)
        self.logger.debug({"params": params})
        try:
            response = self.client.chat.completions.create(**params)
            message_content = response.choices[0].message.content
            usage_prompt_tokens = response.usage.prompt_tokens
            usage_completion_tokens = response.usage.completion_tokens
            usage_total_tokens = response.usage.total_tokens
            usage_cached_tokens = response.usage.prompt_tokens_details.cached_tokens
            system_fingerprint = response.system_fingerprint
            result = {
                "message_content": message_content,
                "usage_prompt_tokens": usage_prompt_tokens,
                "usage_completion_tokens": usage_completion_tokens,
                "usage_total_tokens": usage_total_tokens,
                "usage_cached_tokens": usage_cached_tokens,
                "system_fingerprint": system_fingerprint,
            }
            self.logger.info(
                f"Requisição realizada com sucesso. Tokens usados: {usage_total_tokens}"
            )
            self.__save_stats(
                usage_prompt_tokens,
                usage_completion_tokens,
                usage_total_tokens,
                usage_cached_tokens,
                system_fingerprint,
                len(system_content),
                len(user_content),
                len(message_content),
            )
            return result
        except Exception as e:
            self.logger.error(e)
            return {
                "message_content": None,
                "usage_prompt_tokens": None,
                "usage_completion_tokens": None,
                "usage_total_tokens": None,
                "usage_cached_tokens": None,
                "system_fingerprint": None,
                "error": str(e),
            }

    def __save_stats(
        self,
        usage_prompt_tokens,
        usage_completion_tokens,
        usage_total_tokens,
        usage_cached_tokens,
        system_fingerprint,
        len_system_content,
        len_user_content,
        len_message_content,
    ):
        file_exists = os.path.isfile(self.log_file)
        try:
            with open(self.log_file, mode="a", encoding="utf-8", newline="") as csvfile:
                writer = csv.writer(csvfile)
                if not file_exists:
                    writer.writerow(
                        [
                            "system_fingerprint",
                            "usage_prompt_tokens",
                            "usage_completion_tokens",
                            "usage_total_tokens",
                            "usage_cached_tokens",
                            "len_system_content",
                            "len_user_content",
                            "len_message_content",
                        ]
                    )
                writer.writerow(
                    [
                        system_fingerprint,
                        usage_prompt_tokens,
                        usage_completion_tokens,
                        usage_total_tokens,
                        usage_cached_tokens,
                        len_system_content,
                        len_user_content,
                        len_message_content,
                    ]
                )
            self.logger.debug(
                {
                    "log_file": self.log_file,
                    "usage_prompt_tokens": usage_prompt_tokens,
                    "usage_completion_tokens": usage_completion_tokens,
                    "usage_total_tokens": usage_total_tokens,
                    "usage_cached_tokens": usage_cached_tokens,
                    "len_system_content": len_system_content,
                    "len_user_content": len_user_content,
                    "len_message_content": len_message_content,
                }
            )
        except Exception as e:
            self.logger.error(e)
