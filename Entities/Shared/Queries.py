class PostgreSQLQueries:
    GET_SCHEMAS = "SELECT schema_name FROM information_schema.schemata"

    GET_TABLES = """SELECT table_name 
                      FROM information_schema.tables 
                     WHERE table_type   = 'BASE TABLE'
                       AND table_schema = %s"""