from trempy.Replication.Strategies.FullLoadStrategy import FullLoadStrategy
from trempy.Replication.Strategies.CDCStrategy import CDCStrategy
from trempy.Replication.Exceptions.Exception import *
from trempy.Loggings.Logging import ReplicationLogger
from trempy.Shared.Types import TaskType

logger = ReplicationLogger()


class ReplicationStrategyFactory:
    @staticmethod
    def create_strategy(mode: TaskType, **kwargs):
        """
        Cria e retorna uma estratégia de replicação com base no modo especificado.

        Args:
            mode (TaskType): Tipo de tarefa que define a estratégia de replicação (FULL_LOAD ou CDC).
            **kwargs: Argumentos adicionais específicos para cada estratégia:
                - Para CDC: interval_seconds (int) define o intervalo entre execuções.

        Returns:
            ReplicationStrategy: Instância da estratégia de replicação correspondente ao modo.

        Raises:
            ValueError: Quando o modo especificado não é suportado.
        """
        if mode == TaskType.FULL_LOAD:
            return FullLoadStrategy()
        elif mode == TaskType.CDC:
            return CDCStrategy(interval_seconds=kwargs.get("interval_seconds"))
        else:
            e = NotSupportedReplicationTypeError(
                "Modo de replicação não suportado", mode
            )
            logger.critical(e)
