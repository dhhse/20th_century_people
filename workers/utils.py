import sys

def suppress_exceptions(exceptions, log_fail=True):
    exceptions = tuple(exceptions)
    def decorator(func):
        def wrapper(*args,**kwargs):
            try:
                return func(*args, **kwargs)
            except exceptions as exc:
                if log_fail:
                    print(f"Exception `{exc}` occured in {func.__name__}(*{args},**{kwargs})", file=sys.stderr)
                return None
        return wrapper
    return decorator
