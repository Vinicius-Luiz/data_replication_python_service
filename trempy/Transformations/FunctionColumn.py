from trempy.Shared.Types import TransformationOperationType
from typing import List, Dict


class FunctionColumn:
    """Classe que representa uma coluna de fun o de transforma o."""

    REQUIRED_COLUMN_TYPES = {}
    REQUIRED_PARAMS = {}

    @classmethod
    def get_required_column_types(
        cls, function: TransformationOperationType
    ) -> List | Dict:
        """Retorna os tipos de coluna necessários para executar uma transformação.

        Args:
            function: Tipo de operação de transformação (enum TransformationOperationType)

        Returns:
            Union[List[type], Dict[str, type]]: Tipos de coluna aceitos pela operação.
                Pode ser:
                - Uma lista de tipos aceitos diretamente
                - Um dicionário com tipos específicos para diferentes parâmetros

        Example:
            >>> get_required_column_types(TransformationOperationType.FORMAT_DATE)
            [datetime.date, datetime.datetime]
        """

        return cls.REQUIRED_COLUMN_TYPES.get(function, [])

    @classmethod
    def get_required_params(cls, function: TransformationOperationType) -> list:
        """Retorna os parâmetros obrigatórios para executar uma transformação.

        Args:
            function: Tipo de operação de transformação (enum TransformationOperationType)

        Returns:
            List[str]: Nomes dos parâmetros obrigatórios para a operação especificada.

        Example:
            >>> get_required_params(TransformationOperationType.MATH_EXPRESSION)
            ['expression']
        """

        return cls.REQUIRED_PARAMS.get(function, [])
