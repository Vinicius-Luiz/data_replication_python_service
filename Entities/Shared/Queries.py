class PostgreSQLQueries:
    GET_SCHEMAS = "SELECT schema_name FROM information_schema.schemata"

    GET_TABLES = """
    SELECT table_name 
      FROM information_schema.tables 
     WHERE table_type   = 'BASE TABLE'
       AND table_schema = %s"""
    
    GET_TABLE_DETAILS = """
    SELECT schemaname                           AS schema_name,
           relname                              AS table_name,
           n_live_tup                           AS estimated_row_count,
           pg_size_pretty(pg_table_size(relid)) AS table_size
      FROM  pg_stat_user_tables
     WHERE schemaname = %s
       AND relname = %s
     ORDER BY schemaname, relname
      """
    
    GET_TABLE_COLUMNS = """
    SELECT table_schema AS schema_name,
           table_name,
           column_name,
           data_type,
           udt_name,
           character_maximum_length,
           ordinal_position
      FROM information_schema.columns
     WHERE table_schema = %s
       AND table_name = %s
     ORDER BY table_schema,
              table_name,
              ordinal_position
      """
    
    GET_TABLE_PRIMARY_KEY = """
    SELECT kcu.column_name AS primary_key_column
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu 
        ON tc.constraint_name = kcu.constraint_name AND
           tc.table_schema = kcu.table_schema
      WHERE tc.constraint_type = 'PRIMARY KEY' AND
            tc.table_schema = %s      AND
            tc.table_name   = %s
    """
