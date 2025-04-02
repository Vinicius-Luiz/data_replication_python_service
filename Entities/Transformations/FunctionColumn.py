from typing import List, Dict
from Entities.Shared.Types import OperationType


class FunctionColumn:
    REQUIRED_COLUMN_TYPES = {}
    REQUIRED_PARAMS = {}

    @classmethod
    def get_required_column_types(cls, function: OperationType) -> List | Dict:
        """Retorna as tipagens de coluna necess rias para execu o de uma
        fun o de transforma o.

        Parameters
        ----------
        function : str
            Nome da fun o de transforma o.

        Returns
        -------
        list
            Tipagens de coluna necess rias para a execu o da fun o.
        """
        return cls.REQUIRED_COLUMN_TYPES.get(function, [])

    @classmethod
    def get_required_params(cls, function: OperationType) -> list:
        """Retorna os par ametros necess rios para execu o de uma fun o de transforma o.

        Parameters
        ----------
        function : str
            Nome da fun o de transforma o.

        Returns
        -------
        list
            Par ametros necess rios para a execu o da fun o.
        """

        return cls.REQUIRED_PARAMS.get(function, [])
