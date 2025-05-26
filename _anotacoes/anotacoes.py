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