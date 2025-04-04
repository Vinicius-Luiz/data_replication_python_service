from typing import List, Dict
from Entities.Shared.Types import OperationType


class FunctionColumn:
    """Classe que representa uma coluna de fun o de transforma o."""

    REQUIRED_COLUMN_TYPES = {}
    REQUIRED_PARAMS = {}

    @classmethod
    def get_required_column_types(cls, function: OperationType) -> List | Dict:
        """Retorna os tipos de coluna necessários para executar uma transformação.

        Args:
            function: Tipo de operação de transformação (enum OperationType)

        Returns:
            Union[List[type], Dict[str, type]]: Tipos de coluna aceitos pela operação.
                Pode ser:
                - Uma lista de tipos aceitos diretamente
                - Um dicionário com tipos específicos para diferentes parâmetros

        Example:
            >>> get_required_column_types(OperationType.FORMAT_DATE)
            [datetime.date, datetime.datetime]
        """

        return cls.REQUIRED_COLUMN_TYPES.get(function, [])

    @classmethod
    def get_required_params(cls, function: OperationType) -> list:
        """Retorna os parâmetros obrigatórios para executar uma transformação.

        Args:
            function: Tipo de operação de transformação (enum OperationType)

        Returns:
            List[str]: Nomes dos parâmetros obrigatórios para a operação especificada.

        Example:
            >>> get_required_params(OperationType.MATH_EXPRESSION)
            ['expression']
        """

        return cls.REQUIRED_PARAMS.get(function, [])
