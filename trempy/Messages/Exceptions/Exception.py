class MessageError(Exception):
    """Exceção lançada quando ocorre um erro na mensagem."""

    def __init__(self, message: str):
        prefixed_message = f"MESSAGE - {message}"
        super().__init__(prefixed_message)


class MessageConsumerException(MessageError):
    """Exceção lançada quando ocorre um erro no consumidor de mensagens."""

    def __init__(self, message: str):
        super().__init__(message)

class MessageDlxException(MessageError):
    """Exceção lançada quando ocorre um erro no DLX de mensagens."""

    def __init__(self, message: str):
        super().__init__(message)

class MessageProducerException(MessageError):
    """Exceção lançada quando ocorre um erro no produtor de mensagens."""

    def __init__(self, message: str):
        super().__init__(message)