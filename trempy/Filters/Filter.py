from __future__ import annotations
from typing import Union, List, Type, get_args, get_origin
from trempy.Loggings.Logging import ReplicationLogger
from trempy.Filters.Exceptions.Exception import *
from trempy.Shared.Types import FilterType
from typing import TYPE_CHECKING
from datetime import datetime
import polars as pl

if TYPE_CHECKING:
    from trempy.Tables.Table import Table

logger = ReplicationLogger()


class Filter:
    """Classe que representa um filtro para uma coluna de uma tabela.

    Attributes:
        column_name (str): O nome da coluna a ser filtrada.
        filter_type (str): O operador de comparação para o filtro.
        description (str): A descrição do filtro.
        value (str): O valor a ser comparado com a coluna.
        values (List[str]): Uma lista de valores a serem comparados com a coluna.
        lower (float): O limite inferior para o filtro.
        upper (float): O limite superior para o filtro.
    """

    def __init__(
        self,
        column_name: str,
        filter_type: str,
        description: str,
        value: Union[str, int, float, List] = None,
        values: List[Union[str, int, float]] = None,
        lower: Union[int, float] = None,
        upper: Union[int, float] = None,
    ) -> None:
        self.column_name = column_name
        self.filter_type = FilterType(filter_type)
        self.description = description
        self.value = value
        self.values = values
        self.lower = lower
        self.upper = upper

        self.col_type = None

        self.__validate()

    def __validate(self) -> None:
        """
        Valida se o tipo do filtro é válido.

        Raises:
            InvalidFilterTypeError: Se o tipo do filtro não for válido.
        """

        if self.filter_type not in FilterType:
            e = InvalidFilterTypeError("Tipo de filtro inválido", self.filter_type)
            logger.critical(e)

    def __validate_column_exists(self, table: Table) -> None:
        """Valida se a coluna existe no DataFrame.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Raises:
            ColumnNotFoundError: Se a coluna não existir no DataFrame.
        """
        if self.column_name not in table.data.columns:
            available = list(table.data.columns)
            e = ColumnNotFoundError(
                f"Coluna '{self.column_name}' não encontrada"
                f"Colunas disponíveis: {available}",
                self.column_name,
            )
            logger.critical(e)

    def __validate_type(self, type_required: Type, value_param: str = None) -> None:
        """Valida se o tipo do valor do filtro corresponde ao tipo requerido.

        Args:
            type_required (Type): Tipo ou tipos aceitos para a operação de filtro.

        Raises:
            InvalidTypeValueError: Se o tipo do valor não for compatível com o tipo requerido.
        """

        value_params = {
            "value": self.value,
            "values": self.values,
            "lower": self.lower,
            "upper": self.upper,
        }

        value_type = type(value_params[value_param])

        # Se for Union, pega todos os tipos aceitos
        if get_origin(type_required) is Union:
            allowed_types = get_args(type_required)
        else:
            allowed_types = (type_required,)

        # Verifica compatibilidade de tipos
        if not any(
            issubclass(value_type, t) for t in allowed_types if isinstance(t, type)
        ):
            allowed_names = [t.__name__ for t in allowed_types]
            e = InvalidTypeValueError(
                f"Tipo inválido para o valor do filtro. Esperado: {allowed_names}",
                value_type.__name__,
            )
            logger.critical(e)

    def __validate_filter_date(self, table: Table, value_param: str) -> None:
        """Valida se o tipo do valor do filtro corresponde ao tipo requerido.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Raises:
            InvalidTypeDateError or InvalidTypeValueError: Se o tipo do valor não for compatível com o tipo requerido.
        """

        value_params = {
            "value": self.value,
            "values": self.values,
            "lower": self.lower,
            "upper": self.upper,
        }

        value_param = value_params[value_param]

        self.col_type = table.data.schema[self.column_name]

        if not isinstance(self.col_type, (pl.Date, pl.Datetime)):
            e = InvalidTypeDateError(
                f"Coluna {self.column_name} deve ser do tipo Date ou Datetime",
                self.col_type.__class__.__name__,
            )
            logger.critical(e)

        if not isinstance(value_param, str):
            e = InvalidTypeValueError(
                f"Valor para comparação de datas deve ser string, ",
                type(value_param).__name__,
            )
            logger.critical(e)

    def __convert_str_to_date(self, value: str):
        """Converte string para Date ou Datetime conforme o tipo da coluna.

        Args:
            value: String no formato yyyy-mm-dd (Date) ou yyyy-mm-dd hh:mm:ss (Datetime)

        Returns:
            Valor convertido para o tipo temporal adequado

        Raises:
            ValueError: Se o formato da string não corresponder ao esperado
        """

        try:
            if isinstance(self.col_type, pl.Date):
                dt = datetime.strptime(value, "%Y-%m-%d")
                date_value = pl.date(dt.year, dt.month, dt.day)
            else:
                dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                date_value = pl.datetime(
                    dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second
                )

        except Exception as e:
            e = ValueError(
                f"Falha ao converter valor '{value}'. "
                f"Formato esperado: {'YYYY-MM-DD' if isinstance(self.col_type, pl.Date) else 'YYYY-MM-DD HH:MM:SS'}. "
                f"Erro: {str(e)}"
            )
            logger.critical(e)

        return date_value

    def __execute_equals(self, table: Table) -> Table:
        """Filtra linhas onde a coluna é igual ao valor especificado.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        type_required = Union[str, int, float]
        self.__validate_column_exists(table)
        self.__validate_type(type_required, "value")

        table.data = table.data.filter(pl.col(self.column_name) == self.value)
        return table

    def __execute_not_equals(self, table: Table) -> Table:
        """Filtra linhas onde a coluna é diferente do valor especificado.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        type_required = Union[str, int, float]
        self.__validate_column_exists(self.column_name, table)
        self.__validate_type(type_required, "value")
        table.data = table.data.filter(pl.col(self.column_name) != self.value)
        return table

    def __execute_greater_than(self, table: Table) -> Table:
        """Filtra linhas onde a coluna é maior que o valor especificado.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        type_required = Union[int, float]
        self.__validate_column_exists(self.column_name, table)
        self.__validate_type(type_required, "value")
        table.data = table.data.filter(pl.col(self.column_name) > self.value)
        return table

    def __execute_greater_than_or_equal(self, table: Table) -> Table:
        """Filtra linhas onde a coluna é maior ou igual que o valor especificado.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        type_required = Union[int, float]
        self.__validate_column_exists(self.column_name, table)
        self.__validate_type(type_required, "value")
        table.data = table.data.filter(pl.col(self.column_name) >= self.value)
        return table

    def __execute_less_than(self, table: Table) -> Table:
        """Filtra linhas onde a coluna é menor que o valor especificado.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        type_required = Union[int, float]
        self.__validate_column_exists(self.column_name, table)
        self.__validate_type(type_required, "value")
        table.data = table.data.filter(pl.col(self.column_name) < self.value)
        return table

    def __execute_less_than_or_equal(self, table: Table) -> Table:
        """Filtra linhas onde a coluna é menor ou igual que o valor especificado.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        type_required = Union[int, float]
        self.__validate_column_exists(self.column_name, table)
        self.__validate_type(type_required, "value")
        table.data = table.data.filter(pl.col(self.column_name) <= self.value)
        return table

    def __execute_in(self, table: Table) -> Table:
        """Filtra linhas onde a coluna está na lista de valores especificada.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        type_required = List[Union[str, int, float]]
        self.__validate_column_exists(self.column_name, table)
        self.__validate_type(type_required, "values")
        table.data = table.data.filter(pl.col(self.column_name).is_in(self.values))
        return table

    def __execute_not_in(self, table: Table) -> Table:
        """Filtra linhas onde a coluna não está na lista de valores especificada.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        type_required = List[Union[str, int, float]]
        self.__validate_column_exists(self.column_name, table)
        self.__validate_type(type_required, "values")
        table.data = table.data.filter(~pl.col(self.column_name).is_in(self.values))
        return table

    def __execute_is_null(self, table: Table) -> Table:
        """Filtra linhas onde a coluna é nula.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        self.__validate_column_exists(self.column_name, table)
        table.data = table.data.filter(pl.col(self.column_name).is_null())
        return table

    def __execute_is_not_null(self, table: Table) -> Table:
        """Filtra linhas onde a coluna não é nula.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        self.__validate_column_exists(self.column_name, table)
        table.data = table.data.filter(pl.col(self.column_name).is_not_null())
        return table

    def __execute_starts_with(self, table: Table) -> Table:
        """Filtra linhas onde a coluna começa com o prefixo especificado.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        type_required = str
        self.__validate_column_exists(self.column_name, table)
        self.__validate_type(type_required, "value")
        table.data = table.data.filter(
            pl.col(self.column_name).str.starts_with(self.value)
        )
        return table

    def __execute_ends_with(self, table: Table) -> Table:
        """Filtra linhas onde a coluna termina com o sufixo especificado.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        type_required = str
        self.__validate_column_exists(self.column_name, table)
        self.__validate_type(type_required, "value")
        table.data = table.data.filter(
            pl.col(self.column_name).str.ends_with(self.value)
        )
        return table

    def __execute_contains(self, table: Table) -> Table:
        """Filtra linhas onde a coluna contém a substring especificada.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        type_required = str
        self.__validate_column_exists(self.column_name, table)
        self.__validate_type(type_required, "value")
        table.data = table.data.filter(
            pl.col(self.column_name).str.contains(self.value)
        )
        return table

    def __execute_not_contains(self, table: Table) -> Table:
        """Filtra linhas onde a coluna não contém a substring especificada.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        type_required = str
        self.__validate_column_exists(self.column_name, table)
        self.__validate_type(type_required, "value")
        table.data = table.data.filter(
            ~pl.col(self.column_name).str.contains(self.value)
        )
        return table

    def __execute_between(self, table: Table) -> Table:
        """Filtra linhas onde a coluna está entre os valores especificados.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        type_required = Union[int, float]
        self.__validate_column_exists(self.column_name, table)
        self.__validate_type(type_required, "lower")
        self.__validate_type(type_required, "upper")
        table.data = table.data.filter(
            pl.col(self.column_name).is_between(self.lower, self.upper)
        )
        return table

    def __execute_not_between(self, table: Table) -> Table:
        """Filtra linhas onde a coluna não está entre os valores especificados.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        type_required = Union[int, float]
        self.__validate_column_exists(self.column_name, table)
        self.__validate_type(type_required, "lower")
        self.__validate_type(type_required, "upper")
        table.data = table.data.filter(
            ~pl.col(self.column_name).is_between(self.lower, self.upper)
        )
        return table

    def __execute_date_equals(self, table: Table) -> Table:
        """Filtra linhas onde a coluna de data é igual ao valor especificado.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        self.__validate_column_exists(table)
        self.__validate_filter_date(table, "value")

        date_value = self.__convert_str_to_date(self.value)

        table.data = table.data.filter(pl.col(self.column_name) == date_value)
        return table

    def __execute_date_not_equals(self, table: Table) -> Table:
        """Filtra linhas onde a coluna de data é diferente do valor especificado.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        self.__validate_column_exists(table)
        self.__validate_filter_date(table, "value")

        date_value = self.__convert_str_to_date(self.value)

        table.data = table.data.filter(pl.col(self.column_name) != date_value)
        return table

    def __execute_date_greater_than(self, table: Table) -> Table:
        """Filtra linhas onde a coluna de data é maior que o valor especificado.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        self.__validate_column_exists(table)
        self.__validate_filter_date(table, "value")

        date_value = self.__convert_str_to_date(self.value)

        table.data = table.data.filter(pl.col(self.column_name) > date_value)
        return table

    def __execute_date_greater_than_or_equal(self, table: Table) -> Table:
        """Filtra linhas onde a coluna de data é maior ou igual ao valor especificado.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        self.__validate_column_exists(table)
        self.__validate_filter_date(table, "value")

        date_value = self.__convert_str_to_date(self.value)

        table.data = table.data.filter(pl.col(self.column_name) >= date_value)
        return table

    def __execute_date_less_than(self, table: Table) -> Table:
        """Filtra linhas onde a coluna de data é menor que o valor especificado.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        self.__validate_column_exists(table)
        self.__validate_filter_date(table, "value")

        date_value = self.__convert_str_to_date(self.value)

        table.data = table.data.filter(pl.col(self.column_name) < date_value)
        return table

    def __execute_date_less_than_or_equal(self, table: Table) -> Table:
        """Filtra linhas onde a coluna de data é menor ou igual ao valor especificado.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        self.__validate_column_exists(table)
        self.__validate_filter_date(table, "value")

        date_value = self.__convert_str_to_date(self.value)

        table.data = table.data.filter(pl.col(self.column_name) <= date_value)
        return table

    def __execute_date_between(self, table: Table) -> Table:
        """Filtra linhas onde a coluna de data é entre os valores especificados.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        self.__validate_column_exists(table)
        self.__validate_filter_date(table, "lower")
        self.__validate_filter_date(table, "upper")

        date_lower = self.__convert_str_to_date(self.lower)
        date_upper = self.__convert_str_to_date(self.upper)

        table.data = table.data.filter(
            pl.col(self.column_name).is_between(date_lower, date_upper)
        )
        return table

    def __execute_date_not_between(self, table: Table) -> Table:
        """Filtra linhas onde a coluna de data é fora dos valores especificados.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        self.__validate_column_exists(table)
        self.__validate_filter_date(table, "lower")
        self.__validate_filter_date(table, "upper")

        date_lower = self.__convert_str_to_date(self.lower)
        date_upper = self.__convert_str_to_date(self.upper)

        table.data = table.data.filter(
            ~pl.col(self.column_name).is_between(date_lower, date_upper)
        )
        return table

    def execute(self, table: Table) -> Table:
        """Aplica o filtro no DataFrame da tabela conforme o tipo especificado.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        logger.info(
            f"FILTER - Aplicando filtro em {table.schema_name}.{table.table_name} {self.column_name}: {self.description}",
            required_types=["full_load"],
        )

        try:

            filter_functions = {
                FilterType.EQUALS: self.__execute_equals,
                FilterType.NOT_EQUALS: self.__execute_not_equals,
                FilterType.GREATER_THAN: self.__execute_greater_than,
                FilterType.GREATER_THAN_OR_EQUAL: self.__execute_greater_than_or_equal,
                FilterType.LESS_THAN: self.__execute_less_than,
                FilterType.LESS_THAN_OR_EQUAL: self.__execute_less_than_or_equal,
                FilterType.IN: self.__execute_in,
                FilterType.NOT_IN: self.__execute_not_in,
                FilterType.IS_NULL: self.__execute_is_null,
                FilterType.IS_NOT_NULL: self.__execute_is_not_null,
                FilterType.STARTS_WITH: self.__execute_starts_with,
                FilterType.ENDS_WITH: self.__execute_ends_with,
                FilterType.CONTAINS: self.__execute_contains,
                FilterType.NOT_CONTAINS: self.__execute_not_contains,
                FilterType.BETWEEN: self.__execute_between,
                FilterType.NOT_BETWEEN: self.__execute_not_between,
                FilterType.DATE_EQUALS: self.__execute_date_equals,
                FilterType.DATE_NOT_EQUALS: self.__execute_date_not_equals,
                FilterType.DATE_GREATER_THAN: self.__execute_date_greater_than,
                FilterType.DATE_GREATER_THAN_OR_EQUAL: self.__execute_date_greater_than_or_equal,
                FilterType.DATE_LESS_THAN: self.__execute_date_less_than,
                FilterType.DATE_LESS_THAN_OR_EQUAL: self.__execute_date_less_than_or_equal,
                FilterType.DATE_BETWEEN: self.__execute_date_between,
                FilterType.DATE_NOT_BETWEEN: self.__execute_date_not_between,
            }

            if self.filter_type in filter_functions:
                return filter_functions[self.filter_type](table)
            else:
                e = InvalidFilterTypeError("Tipo de filtro inválido", self.filter_type)
                logger.critical(e)

        except Exception as e:
            e = FilterError(str(e))
            logger.critical(e)
