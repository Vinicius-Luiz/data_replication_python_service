import psycopg2

conn = psycopg2.connect(
    dbname="seu_banco",
    user="seu_usuario",
    password="sua_senha",
    host="localhost",
    port="5432"
)
conn.autocommit = True  # Necessário para replicação

cur = conn.cursor()
cur.execute("SELECT * FROM pg_logical_slot_get_changes('audit_slot', NULL, NULL);")

for row in cur.fetchall():
    print(row)  # Exibe as mudanças no banco de dados

cur.close()
conn.close()


# Contratos de criação de coluna

## Criar coluna com valor fixo
contract = {
    "new_column_name": "status",
    "operation": "literal",
    "value": "ativo"
}

## Criar coluna calculada
contract = {
    "new_column_name": "preco_com_imposto",
    "operation": "derived",
    "depends_on": ["preco"],
    "expression": "preco * 1.1"  # Aumento de 10%
}

## Criar coluna concatenada
contract = {
    "new_column_name": "nome_completo",
    "operation": "concat",
    "depends_on": ["primeiro_nome", "sobrenome"],
    "separator": " "
}

## Obte diferença em anos entre duas datas
contract = {
    "new_column_name": "diferenca_anos",
    "operation": "date_diff_years",
    "depends_on": ["data_inicio", "data_fim"],
    "round_result": True  # Opcional - arredonda o resultado
}