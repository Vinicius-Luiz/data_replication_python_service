import polars as pl

class DataTypes:
    TYPE_POLARS_TO_DATABASE = {
        pl.Int8: "smallint",
        pl.Int16: "smallint",
        pl.Int32: "integer",
        pl.Int64: "bigint",
        pl.Float32: "real",
        pl.Float64: "double precision",
        pl.Utf8: "character varying",
        pl.Boolean: "boolean",
        pl.Date: "date",
        pl.Datetime: "timestamp",
        pl.Decimal: "numeric",
        pl.Null: "text",
    }

    TYPE_DATABASE_TO_POLARS = {
        "smallint": pl.Int16,
        "integer": pl.Int32,
        "bigint": pl.Int64,
        "real": pl.Float32,
        "double precision": pl.Float64,
        "character varying": pl.Utf8,
        "boolean": pl.Boolean,
        "date": pl.Date,
        "timestamp": pl.Datetime,
        "numeric": pl.Decimal,
        "text": pl.Null,
    }