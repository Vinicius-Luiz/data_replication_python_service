from trempy.Loggings.Logging import ReplicationLogger
from typing import Any, Dict, Optional
from dotenv import load_dotenv
from openai import OpenAI
import json
import csv
import os

ReplicationLogger.configure_logging()


class TaskCreator:
    def __init__(self, base_url: str = "https://api.deepseek.com"):
        load_dotenv()
        self.api_key = os.getenv("DEEPSEEK_API_KEY")
        self.base_url = base_url
        self.logger = ReplicationLogger()
        try:
            self.client = OpenAI(api_key=self.api_key, base_url=self.base_url)
        except Exception as e:
            self.logger.error(e)
        self.logger.info("DeepSeek API inicializado com sucesso.")

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
        self.logger.debug(
            {
                "model": params.get("model"),
                "response_format": params.get("response_format"),
                "temperature": params.get("temperature"),
            }
        )
        try:
            response = self.client.chat.completions.create(**params)
            message_content = response.choices[0].message.content
            usage_prompt_tokens = response.usage.prompt_tokens
            usage_completion_tokens = response.usage.completion_tokens
            usage_total_tokens = response.usage.total_tokens
            usage_cached_tokens = response.usage.prompt_tokens_details.cached_tokens
            system_fingerprint = response.system_fingerprint
            result = {
                "message_content": json.loads(message_content),
                "usage_prompt_tokens": usage_prompt_tokens,
                "usage_completion_tokens": usage_completion_tokens,
                "usage_total_tokens": usage_total_tokens,
                "usage_cached_tokens": usage_cached_tokens,
                "system_fingerprint": system_fingerprint,
                "success": True,
            }
            self.logger.info(
                f"Requisição realizada com sucesso. Tokens usados: {usage_total_tokens}"
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
                "success": False,
            }
