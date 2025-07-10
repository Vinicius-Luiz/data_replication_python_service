from openai.types.shared import ResponseFormatJSONSchema
from trempy.Chatbot.Chatbot import Chatbot
import json

# Carregar conteúdos do prompt
with open(r'_create_task_by_prompt\README.md', 'r', encoding='utf-8') as arquivo:
    system_content = arquivo.read()
    
with open(r'_create_task_by_prompt\prompt_scd2.txt', 'r', encoding='utf-8') as arquivo:
    user_content = arquivo.read()

chatbot = Chatbot()
resposta = chatbot.request(system_content, user_content)

# Salvar a resposta como JSON
message_json = json.loads(resposta["message_content"])

json_filename = f"_create_task_by_prompt/criado_por_deepseek_{resposta['system_fingerprint']}.json"
with open(json_filename, "w", encoding="utf-8") as f:
    json.dump(message_json, f, ensure_ascii=False, indent=2)

print('CONCLUIDO')
# print(resposta["message_content"])
# print(type(resposta["message_content"]))
# print(f"Prompt tokens: {resposta['usage_prompt_tokens']}")
# print(f"Completion tokens: {resposta['usage_completion_tokens']}")
# print(f"Total tokens: {resposta['usage_total_tokens']}")
# print(f"Cached tokens: {resposta['usage_cached_tokens']}")
# print(f"System fingerprint: {resposta['system_fingerprint']}")
