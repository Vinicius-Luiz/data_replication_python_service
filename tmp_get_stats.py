from trempy.Metadata.MetadataConnectionManager import MetadataConnectionManager
import polars as pl

# with MetadataConnectionManager() as metadata_manager:
#     metadata_manager.truncate_tables()

with MetadataConnectionManager() as metadata_manager:
    print("stats_cdc")
    df = metadata_manager.get_metadata_tables("stats_cdc")
    df = (
        df.group_by(["task_name", "schema_name", "table_name"])
        .agg(
            [
                pl.sum("inserts"),
                pl.sum("updates"),
                pl.sum("deletes"),
                pl.sum("errors"),
                pl.sum("total"),
            ]
        )
        .sort(["task_name", "schema_name", "table_name"])
    )
    print(df.head(100000))

with MetadataConnectionManager() as metadata_manager:
    print("stats_full_load")
    df = metadata_manager.get_metadata_tables("stats_full_load")
    print(df.head(100000))


with MetadataConnectionManager() as metadata_manager:
    print("stats_source_tables")
    df = metadata_manager.get_metadata_tables("stats_source_tables")
    print(df.head(100000))

with MetadataConnectionManager() as metadata_manager:
    print("metadata_table")
    df = metadata_manager.get_metadata_tables("metadata_table")
    print(df.head(100000))

with MetadataConnectionManager() as metadata_manager:
    print("stats_message")
    df = metadata_manager.get_metadata_tables("stats_message")
    print(df.head(100000))

with MetadataConnectionManager() as metadata_manager:
    print("apply_exceptions")
    df = metadata_manager.get_metadata_tables("apply_exceptions")
    print(df.head(100000))

with MetadataConnectionManager() as metadata_manager:
    print("messages_stats")
    df = metadata_manager.get_messages_stats()
    print(df.head(100000))