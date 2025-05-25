from trempy.Replication.Strategies.ReplicationStrategy import ReplicationStrategy


class CompositeReplicationStrategy(ReplicationStrategy):
    def __init__(self, *strategies: ReplicationStrategy):
        self.strategies = strategies

    def execute(self, task):
        for strategy in self.strategies:
            strategy.execute(task)
