from Entities.Shared.Types import DatabaseType, EndpointType
from functools import wraps

def source_method(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if self.endpoint_type != EndpointType.SOURCE:
            raise ValueError(f'O método {func.__name__} está disponível apenas para endpoints do tipo Source.')
        return func(self, *args, **kwargs)
    return wrapper

def target_method(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if self.endpoint_type != EndpointType.TARGET:
            raise ValueError(f'O método {func.__name__} está disponível apenas para endpoints do tipo Target.')
        return func(self, *args, **kwargs)
    return wrapper