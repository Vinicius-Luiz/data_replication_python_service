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
