from trempy.Endpoints.Exceptions.Exception import *
from trempy.Shared.Types import EndpointType
from trempy.Shared.Utils import Utils
from functools import wraps

def source_method(func):
    """
    Decorador que limita o acesso a um método apenas para endpoints do tipo Source.

    Args:
        func (function): Fun o a ser decorada.

    Returns:
        function: Fun o decorada com a l gica de acesso limitado.
    """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if self.endpoint_type != EndpointType.SOURCE:
            e = MethodNotSupportedError(f"O método está disponível apenas para endpoints do tipo Source.", func.__name__)
            Utils.log_exception_and_exit(e)
        return func(self, *args, **kwargs)
    return wrapper

def target_method(func):
    """
    Decorador que limita o acesso a um método apenas para endpoints do tipo Target.

    Args:
        func (function): Fun o a ser decorada.

    Returns:
        function: Fun o decorada com a l gica de acesso limitado.
    """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if self.endpoint_type != EndpointType.TARGET:
            e = MethodNotSupportedError(f"O método está disponível apenas para endpoints do tipo Target.", func.__name__)
            Utils.log_exception_and_exit(e)
        return func(self, *args, **kwargs)
    return wrapper