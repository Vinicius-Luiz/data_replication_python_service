from __future__ import annotations
from typing import TYPE_CHECKING
from Entities.Shared.Types import FilterType
from typing import Union, List, Type, get_args, get_origin
from datetime import datetime
import polars as pl

import logging

if TYPE_CHECKING:
    from Entities.Tables.Table import Table


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

        self.validate()

    def validate(self) -> None:
        """
        Valida se o tipo do filtro é válido.

        Raises:
            ValueError: Se o tipo do filtro não for válido.
        """

        if self.filter_type not in FilterType:
            raise_msg = f"FILTER - Tipo de filtro inválido: {self.filter_type}"
            logging.critical(raise_msg)
            raise ValueError(raise_msg)

    def _validate_column_exists(self, table: Table) -> None:
        """Valida se a coluna existe no DataFrame.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Raises:
            ValueError: Se a coluna não existir no DataFrame.
        """
        if self.column_name not in table.data.columns:
            available = list(table.data.columns)
            raise ValueError(
                f"Coluna '{self.column_name}' não encontrada. "
                f"Colunas disponíveis: {available}"
            )

    def _validate_type(self, type_required: Type, value_param: str = None) -> None:
        """Valida se o tipo do valor do filtro corresponde ao tipo requerido.

        Args:
            type_required (Type): Tipo ou tipos aceitos para a operação de filtro.

        Raises:
            TypeError: Se o tipo do valor não for compatível com o tipo requerido.
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
            raise TypeError(
                f"Tipo inválido para o valor do filtro. "
                f"Esperado: {allowed_names}, Recebido: {value_type.__name__}"
            )

    def _validate_filter_date(self, table: Table, value_param: str) -> None:
        """Valida se o tipo do valor do filtro corresponde ao tipo requerido.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Raises:
            TypeError: Se o tipo do valor não for compatível com o tipo requerido.
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
            raise TypeError(
                f"Coluna '{self.column_name}' deve ser do tipo Date ou Datetime, "
                f"mas é {self.col_type.__class__.__name__}"
            )

        if not isinstance(value_param, str):
            raise TypeError(
                f"Valor para comparação de datas deve ser string, "
                f"mas recebeu {type(value_param).__name__}"
            )

    def execute(self, table: Table) -> Table:
        """Aplica o filtro no DataFrame da tabela conforme o tipo especificado.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        logging.info(
            f"FILTER - Aplicando filtro em {table.schema_name}.{table.table_name} {self.column_name}: {self.description}"
        )

        match self.filter_type:
            case FilterType.EQUALS:
                return self._execute_equals(table)
            case FilterType.NOT_EQUALS:
                return self._execute_not_equals(table)
            case FilterType.GREATER_THAN:
                return self._execute_greater_than(table)
            case FilterType.GREATER_THAN_OR_EQUAL:
                return self._execute_less_than_or_equal(table)
            case FilterType.LESS_THAN:
                return self._execute_less_than(table)
            case FilterType.LESS_THAN_OR_EQUAL:
                return self._execute_less_than_or_equal(table)
            case FilterType.IN:
                return self._execute_in(table)
            case FilterType.NOT_IN:
                return self._execute_not_in(table)
            case FilterType.IS_NULL:
                return self._execute_is_null(table)
            case FilterType.IS_NOT_NULL:
                return self._execute_is_not_null(table)
            case FilterType.STARTS_WITH:
                return self._execute_starts_with(table)
            case FilterType.ENDS_WITH:
                return self._execute_ends_with(table)
            case FilterType.CONTAINS:
                return self._execute_contains(table)
            case FilterType.NOT_CONTAINS:
                return self._execute_not_contains(table)
            case FilterType.BETWEEN:
                return self._execute_between(table)
            case FilterType.NOT_BETWEEN:
                return self._execute_not_between(table)
            case FilterType.DATE_EQUALS:
                return self._execute_date_equals(table)
            case FilterType.DATE_NOT_EQUALS:
                return self._execute_date_not_equals(table)
            case FilterType.DATE_GREATER_THAN:
                return self._execute_date_greater_than(table)
            case FilterType.DATE_GREATER_THAN_OR_EQUAL:
                return self._execute_date_greater_than_or_equal(table)
            case FilterType.DATE_LESS_THAN:
                return self._execute_date_less_than(table)
            case FilterType.DATE_LESS_THAN_OR_EQUAL:
                return self._execute_date_less_than_or_equal(table)
            case FilterType.DATE_BETWEEN:
                return self._execute_date_between(table)
            case FilterType.DATE_NOT_BETWEEN:
                return self._execute_date_not_between(table)

    def _execute_equals(self, table: Table) -> Table:
        """Filtra linhas onde a coluna é igual ao valor especificado.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.

        Raises:
            TypeError: Se o tipo do valor não for compatível com a operação.
        """

        type_required = Union[str, int, float]
        self._validate_column_exists(table)
        self._validate_type(type_required, "value")

        table.data = table.data.filter(pl.col(self.column_name) == self.value)
        return table

    def _execute_not_equals(self, table: Table) -> Table:
        """Filtra linhas onde a coluna é diferente do valor especificado.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        type_required = Union[str, int, float]
        self._validate_column_exists(self.column_name, table)
        self._validate_type(type_required, "value")
        table.data = table.data.filter(pl.col(self.column_name) != self.value)
        return table

    def _execute_greater_than(self, table: Table) -> Table:
        """Filtra linhas onde a coluna é maior que o valor especificado.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        type_required = Union[int, float]
        self._validate_column_exists(self.column_name, table)
        self._validate_type(type_required, "value")
        table.data = table.data.filter(pl.col(self.column_name) > self.value)
        return table

    def _execute_greater_than_or_equal(self, table: Table) -> Table:
        """Filtra linhas onde a coluna é maior ou igual que o valor especificado.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        type_required = Union[int, float]
        self._validate_column_exists(self.column_name, table)
        self._validate_type(type_required, "value")
        table.data = table.data.filter(pl.col(self.column_name) >= self.value)
        return table

    def _execute_less_than(self, table: Table) -> Table:
        """Filtra linhas onde a coluna é menor que o valor especificado.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        type_required = Union[int, float]
        self._validate_column_exists(self.column_name, table)
        self._validate_type(type_required, "value")
        table.data = table.data.filter(pl.col(self.column_name) < self.value)
        return table

    def _execute_less_than_or_equal(self, table: Table) -> Table:
        """Filtra linhas onde a coluna é menor ou igual que o valor especificado.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        type_required = Union[int, float]
        self._validate_column_exists(self.column_name, table)
        self._validate_type(type_required, "value")
        table.data = table.data.filter(pl.col(self.column_name) <= self.value)
        return table

    def _execute_in(self, table: Table) -> Table:
        """Filtra linhas onde a coluna está na lista de valores especificada.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        type_required = List[Union[str, int, float]]
        self._validate_column_exists(self.column_name, table)
        self._validate_type(type_required, "values")
        table.data = table.data.filter(pl.col(self.column_name).is_in(self.values))
        return table

    def _execute_not_in(self, table: Table) -> Table:
        """Filtra linhas onde a coluna não está na lista de valores especificada.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        type_required = List[Union[str, int, float]]
        self._validate_column_exists(self.column_name, table)
        self._validate_type(type_required, "values")
        table.data = table.data.filter(~pl.col(self.column_name).is_in(self.values))
        return table

    def _execute_is_null(self, table: Table) -> Table:
        """Filtra linhas onde a coluna é nula.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        self._validate_column_exists(self.column_name, table)
        table.data = table.data.filter(pl.col(self.column_name).is_null())
        return table

    def _execute_is_not_null(self, table: Table) -> Table:
        """Filtra linhas onde a coluna não é nula.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        self._validate_column_exists(self.column_name, table)
        table.data = table.data.filter(pl.col(self.column_name).is_not_null())
        return table

    def _execute_starts_with(self, table: Table) -> Table:
        """Filtra linhas onde a coluna começa com o prefixo especificado.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        type_required = str
        self._validate_column_exists(self.column_name, table)
        self._validate_type(type_required, "value")
        table.data = table.data.filter(
            pl.col(self.column_name).str.starts_with(self.value)
        )
        return table

    def _execute_ends_with(self, table: Table) -> Table:
        """Filtra linhas onde a coluna termina com o sufixo especificado.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        type_required = str
        self._validate_column_exists(self.column_name, table)
        self._validate_type(type_required, "value")
        table.data = table.data.filter(
            pl.col(self.column_name).str.ends_with(self.value)
        )
        return table

    def _execute_contains(self, table: Table) -> Table:
        """Filtra linhas onde a coluna contém a substring especificada.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        type_required = str
        self._validate_column_exists(self.column_name, table)
        self._validate_type(type_required, "value")
        table.data = table.data.filter(
            pl.col(self.column_name).str.contains(self.value)
        )
        return table

    def _execute_not_contains(self, table: Table) -> Table:
        """Filtra linhas onde a coluna não contém a substring especificada.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        type_required = str
        self._validate_column_exists(self.column_name, table)
        self._validate_type(type_required, "value")
        table.data = table.data.filter(
            ~pl.col(self.column_name).str.contains(self.value)
        )
        return table

    def _execute_between(self, table: Table) -> Table:
        """Filtra linhas onde a coluna está entre os valores especificados.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        type_required = Union[int, float]
        self._validate_column_exists(self.column_name, table)
        self._validate_type(type_required, "lower")
        self._validate_type(type_required, "upper")
        table.data = table.data.filter(
            pl.col(self.column_name).is_between(self.lower, self.upper)
        )
        return table

    def _execute_not_between(self, table: Table) -> Table:
        """Filtra linhas onde a coluna não está entre os valores especificados.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.
        """

        type_required = Union[int, float]
        self._validate_column_exists(self.column_name, table)
        self._validate_type(type_required, "lower")
        self._validate_type(type_required, "upper")
        table.data = table.data.filter(
            ~pl.col(self.column_name).is_between(self.lower, self.upper)
        )
        return table

    def _convert_str_to_date(self, value: str):
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
            raise ValueError(
                f"Falha ao converter valor '{value}'. "
                f"Formato esperado: {'YYYY-MM-DD' if isinstance(self.col_type, pl.Date) else 'YYYY-MM-DD HH:MM:SS'}. "
                f"Erro: {str(e)}"
            )

        return date_value

    def _execute_date_equals(self, table: Table) -> Table:
        """Filtra linhas onde a coluna de data é igual ao valor especificado.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.

        Raises:
            ValueError: Se o valor não puder ser convertido para data ou se os tipos forem incompatíveis.
        """

        self._validate_column_exists(table)
        self._validate_filter_date(table, "value")

        date_value = self._convert_str_to_date(self.value)

        table.data = table.data.filter(pl.col(self.column_name) == date_value)
        return table

    def _execute_date_not_equals(self, table: Table) -> Table:
        """Filtra linhas onde a coluna de data é diferente do valor especificado.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.

        Raises:
            ValueError: Se o valor não puder ser convertido para data ou se os tipos forem incompatíveis.
        """

        self._validate_column_exists(table)
        self._validate_filter_date(table, "value")

        date_value = self._convert_str_to_date(self.value)

        table.data = table.data.filter(pl.col(self.column_name) != date_value)
        return table

    def _execute_date_greater_than(self, table: Table) -> Table:
        """Filtra linhas onde a coluna de data é maior que o valor especificado.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.

        Raises:
            ValueError: Se o valor não puder ser convertido para data ou se os tipos forem incompatíveis.
        """

        self._validate_column_exists(table)
        self._validate_filter_date(table, "value")

        date_value = self._convert_str_to_date(self.value)

        table.data = table.data.filter(pl.col(self.column_name) > date_value)
        return table

    def _execute_date_greater_than_or_equal(self, table: Table) -> Table:
        """Filtra linhas onde a coluna de data é maior ou igual ao valor especificado.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.

        Raises:
            ValueError: Se o valor não puder ser convertido para data ou se os tipos forem incompatíveis.
        """

        self._validate_column_exists(table)
        self._validate_filter_date(table, "value")

        date_value = self._convert_str_to_date(self.value)

        table.data = table.data.filter(pl.col(self.column_name) >= date_value)
        return table

    def _execute_date_less_than(self, table: Table) -> Table:
        """Filtra linhas onde a coluna de data é menor que o valor especificado.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.

        Raises:
            ValueError: Se o valor não puder ser convertido para data ou se os tipos forem incompatíveis.
        """

        self._validate_column_exists(table)
        self._validate_filter_date(table, "value")

        date_value = self._convert_str_to_date(self.value)

        table.data = table.data.filter(pl.col(self.column_name) < date_value)
        return table

    def _execute_date_less_than_or_equal(self, table: Table) -> Table:
        """Filtra linhas onde a coluna de data é menor ou igual ao valor especificado.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.

        Raises:
            ValueError: Se o valor não puder ser convertido para data ou se os tipos forem incompatíveis.
        """

        self._validate_column_exists(table)
        self._validate_filter_date(table, "value")

        date_value = self._convert_str_to_date(self.value)

        table.data = table.data.filter(pl.col(self.column_name) <= date_value)
        return table

    def _execute_date_between(self, table: Table) -> Table:
        """Filtra linhas onde a coluna de data é entre os valores especificados.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.

        Raises:
            ValueError: Se o valor nao puder ser convertido para data ou se os tipos forem incompativeis.
        """

        self._validate_column_exists(table)
        self._validate_filter_date(table, "lower")
        self._validate_filter_date(table, "upper")

        date_lower = self._convert_str_to_date(self.lower)
        date_upper = self._convert_str_to_date(self.upper)

        table.data = table.data.filter(
            pl.col(self.column_name).is_between(date_lower, date_upper)
        )
        return table

    def _execute_date_not_between(self, table: Table) -> Table:
        """Filtra linhas onde a coluna de data é fora dos valores especificados.

        Args:
            table (Table): Objeto Table contendo os dados a serem filtrados.

        Returns:
            Table: Objeto Table com o filtro aplicado.

        Raises:
            ValueError: Se o valor nao puder ser convertido para data ou se os tipos forem incompativeis.
        """

        self._validate_column_exists(table)
        self._validate_filter_date(table, "lower")
        self._validate_filter_date(table, "upper")

        date_lower = self._convert_str_to_date(self.lower)
        date_upper = self._convert_str_to_date(self.upper)

        table.data = table.data.filter(
            ~pl.col(self.column_name).is_between(date_lower, date_upper)
        )
        return table
