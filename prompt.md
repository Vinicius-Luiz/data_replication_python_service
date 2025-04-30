refatore esse código.
Em FullLoadStrategy
1. producer.py deve ser finalizado antes que consumer.py execute

Em CDCStrategy
1. o processo de producer.py executa a cada `self.interval_seconds` segundos
2. o processo de consumer.py fica sempre ligado, esperando as mensagens do producer.py


FullLoadStrategy.py
```python
class FullLoadStrategy(ReplicationStrategy):
    """
    Estratégia de replicação para Full Load que executa o producer uma vez para
    extrair todos os dados e depois executa o consumer para carregá-los no destino.
    """

    def execute(self, task: Task):
        """
        Executa a estratégia Full Load, primeiro executando o producer para extração
        completa dos dados e depois o consumer para carregamento.

        Args:
            task (Task): Objeto Task contendo a configuração da tarefa de replicação.
        """
        Utils.write_task_pickle(task)
        Utils.configure_logging()
        logging.debug("Executando producer.py")
        self._run_process("producer.py")
        logging.debug("Executando consumer.py")
        self._run_process("consumer.py")
```

CDCStrategy.py
```python
class CDCStrategy(ReplicationStrategy):
    """
    Estratégia de replicação para CDC (Change Data Capture) que mantém o consumer
    rodando continuamente enquanto executa o producer em intervalos regulares.

    Args:
        interval_seconds (int): Intervalo em segundos entre execuções do producer.
    """

    def __init__(self, interval_seconds):
        self.interval_seconds = interval_seconds

    def execute(self, task: Task):
        """
        Executa a estratégia CDC, mantendo o consumer em execução contínua e
        acionando o producer periodicamente.

        Args:
            task (Task): Objeto Task contendo a configuração da tarefa de replicação.

        O método executa indefinidamente até ser interrompido por KeyboardInterrupt.
        """
        
        Utils.write_task_pickle(task)

        consumer_process = subprocess.Popen(
            [sys.executable, "consumer.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        try:
            while True:
                # Executa o producer e verifica se houve erro
                if not self._run_process("producer.py"):
                    consumer_process.terminate()
                    sys.exit(1)
                
                # Verifica se o consumer ainda está rodando
                if consumer_process.poll() is not None:  # Se não é None, o processo terminou
                    consumer_exit_code = consumer_process.returncode
                    if consumer_exit_code != 0:
                        sys.exit(consumer_exit_code)
                    
                    logging.critical("Consumer terminou inesperadamente - encerrando CDC")
                    sys.exit(1)
                
                logging.debug(f"Aguardando {self.interval_seconds} segundos...")
                sleep(self.interval_seconds)
                
        except KeyboardInterrupt:
            logging.info("Interrompendo processo CDC...")
            consumer_process.terminate()
            consumer_process.wait()
```

ReplicationStrategy.py
```python
import subprocess
import sys
import logging
from trempy.Tasks.Task import Task
from abc import ABC, abstractmethod


class ReplicationStrategy(ABC):
    """
    Classe abstrata que define a interface para todas as estratégias de replicação.
    Fornece métodos utilitários comuns para execução e log de subprocessos.
    """

    @abstractmethod
    def execute(self, task: Task):
        """
        Método abstrato que deve ser implementado pelas subclasses para executar
        a estratégia específica de replicação.

        Args:
            task (Task): Objeto Task contendo a configuração da tarefa de replicação.
        """
        pass
    
    def _run_process(self, script_name):
        """
        Executa um script Python como subprocesso e monitora sua execução.

        Args:
            script_name (str): Nome do script Python a ser executado.

        Returns:
            bool: True se o processo foi executado com sucesso (código de saída 0), False caso contrário.
        """

        processo = subprocess.Popen(
            [sys.executable, script_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        codigo_saida = processo.wait()
        saida, erros = processo.communicate()

        self._log_process_output(script_name, codigo_saida, saida, erros)
        
        if codigo_saida != 0:
            logging.critical(f"Processo {script_name} falhou com código {codigo_saida}")
            return False
        
        return True

    def _log_process_output(self, script_name, exit_code, output, errors):
        """
        Registra os resultados da execução de um subprocesso no log.

        Args:
            script_name (str): Nome do script executado.
            exit_code (int): Código de saída do processo.
            output (str): Saída padrão do processo.
            errors (str): Saída de erro do processo.
        """
        logging.info(f"{script_name} - Código de saída: {exit_code}")
        if output:
            logging.info(f"{script_name} - Saída: {output}")
        if errors:
            logging.critical(f"{script_name} - Erros: {errors}")
```


Como eles são chamados? (O CÓDIGO ABAIXO NÃO DEVE SER ALTERADO, É SÓ A TÍTULO DE INFORMAÇÃO)

```python
    def run(self):
        """
        Executa o fluxo completo de replicação:
        1. Configura logging e variáveis de ambiente
        2. Carrega configurações
        3. Cria a tarefa
        4. Seleciona e executa a estratégia de replicação apropriada

        Raises:
            Exception: Qualquer erro ocorrido durante a execução é registrado e relançado.
        """

        Utils.configure_logging()
        load_dotenv()

        try:
            task_settings = self.load_settings()
            self.task = self.create_task(task_settings)

            strategy = ReplicationStrategyFactory.create_strategy(
                mode=self.task.replication_type,
                interval_seconds=self.task.interval_seconds,
            )

            strategy.execute(task=self.task)
        except Exception as e:
            msg_raise = f"Erro durante a execução: {str(e)}"
            logging.critical(msg_raise)
            raise ValueError(msg_raise)
```