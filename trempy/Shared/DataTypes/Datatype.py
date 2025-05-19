import polars as pl


class Datatype:
    TYPE_POLARS_TO_DATABASE = {}

    TYPE_DATABASE_TO_POLARS = {}

    TYPE_CONVERSION_METHODS = {
        pl.Date: lambda x: pl.Series([x]).str.strptime(pl.Date, "%Y-%m-%d")[0],
        pl.Datetime: lambda x: pl.Series([x]).str.strptime(
            pl.Datetime, "%Y-%m-%d %H:%M:%S"
        )[0],
        pl.Boolean: lambda x: pl.Series([x]).cast(pl.Boolean)[0],
        pl.Int8: lambda x: pl.Series([x]).cast(pl.Int8)[0],
        pl.Int16: lambda x: pl.Series([x]).cast(pl.Int16)[0],
        pl.Int32: lambda x: pl.Series([x]).cast(pl.Int32)[0],
        pl.Int64: lambda x: pl.Series([x]).cast(pl.Int64)[0],
        pl.Float32: lambda x: pl.Series([x]).cast(pl.Float32)[0],
        pl.Float64: lambda x: pl.Series([x]).cast(pl.Float64)[0],
        pl.Decimal: lambda x: pl.Series([x]).cast(pl.Decimal)[0],
    }

    @classmethod
    def convert_value(cls, col_value, col_type):
        """Converte um valor para o tipo correspondente no Polars."""
        if col_type not in cls.TYPE_DATABASE_TO_POLARS:
            return col_value

        polars_type = cls.TYPE_DATABASE_TO_POLARS[col_type]

        if polars_type in cls.TYPE_CONVERSION_METHODS:
            return cls.TYPE_CONVERSION_METHODS[polars_type](col_value)

        return col_value


class DatatypePostgreSQL(Datatype):
    TYPE_POLARS_TO_DATABASE = {
        pl.Int8: "smallint",
        pl.Int16: "smallint",
        pl.Int32: "integer",
        pl.Int64: "bigint",
        pl.Float32: "real",
        pl.Float64: "double precision",
        pl.Utf8: "character varying",
        pl.Utf8: "varchar",
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
        "varchar": pl.Utf8,
        "boolean": pl.Boolean,
        "date": pl.Date,
        "timestamp": pl.Datetime,
        "numeric": pl.Decimal,
        "text": pl.Null,
    }
