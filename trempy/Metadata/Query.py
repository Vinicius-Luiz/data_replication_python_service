class Query:
    SQL_CREATE_STATS_CDC = """
        CREATE TABLE IF NOT EXISTS stats_cdc (
            task_name   TEXT,
            schema_name TEXT,
            table_name  TEXT,
            inserts     INTEGER,
            updates     INTEGER,
            deletes     INTEGER,
            errors      INTEGER,
            total       INTEGER,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """

    SQL_CREATE_STATS_FULL_LOAD = """
        CREATE TABLE IF NOT EXISTS stats_full_load (
            task_name    TEXT,
            schema_name  TEXT,
            table_name   TEXT,
            records      INTEGER,
            success      BOOLEAN,
            time_elapsed TEXT,
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    
    # {'rowcount': 300027, 'statusmessage': 'SELECT 300027', 'time_elapsed': '0.54s'}
    SQL_CREATE_STATS_SOURCE_TABLES = """
        CREATE TABLE IF NOT EXISTS stats_source_tables (
            task_name    TEXT,
            schema_name  TEXT,
            table_name   TEXT,
            rowcount     INTEGER,
            statusmessage TEXT,
            time_elapsed TEXT,
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """

    SQL_INSERT_STATS_CDC = """
        INSERT INTO stats_cdc 
        (task_name, schema_name, table_name, inserts, updates, deletes, errors, total)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """

    SQL_INSERT_STATS_FULL_LOAD = """
        INSERT INTO stats_full_load 
        (task_name, schema_name, table_name, records, success, time_elapsed)
        VALUES (?, ?, ?, ?, ?, ?)
        """
    
    SQL_INSERT_STATS_SOURCE_TABLES = """
        INSERT INTO stats_source_tables 
        (task_name, schema_name, table_name, rowcount, statusmessage, time_elapsed)
        VALUES (?, ?, ?, ?, ?, ?)
        """
