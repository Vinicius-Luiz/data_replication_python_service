class Query:
    SQL_METADATA_TABLE = """
        CREATE TABLE IF NOT EXISTS metadata_table (
            key    TEXT PRIMARY KEY,
            value  TEXT
        )
    """

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

    SQL_CREATE_STATS_MESSAGE = """
        CREATE TABLE IF NOT EXISTS stats_message (
            task_name    TEXT,
            transaction_id TEXT,
            quantity_operations INTEGER,
            published INTEGER,
            received INTEGER,
            processed INTEGER,
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

    SQL_INSERT_STATS_MESSAGE = """
        INSERT INTO stats_message 
        (task_name, transaction_id, quantity_operations, published, received, processed)
        VALUES (?, ?, ?, ?, ?, ?)
        """

    SQL_UPDATE_STATS_MESSSAGE = """
        UPDATE stats_message
           SET {column_set} = {value_set}
         WHERE transaction_id = ?
    """

    SQL_UPSERT_METADATA_TABLE = """
        INSERT OR REPLACE INTO metadata_table (key, value)
        VALUES (?, ?);
        """

    SQL_GET_STATS_MESSAGE = """
        SELECT COALESCE({column}, 0)
        FROM stats_message
        WHERE transaction_id = ?
    """
    
    SQL_GET_METADATA_CONFIG = """
        SELECT MAX(value)
        FROM metadata_table
        WHERE key = ?
    """

    SQL_TRUNCATE_TABLE = """
        DELETE FROM {table}
    """
