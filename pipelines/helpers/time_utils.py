import time
from typing import Any


def timer(func):
    """Computes the duration of the given callable.

    Args:
        func: the callable to time.

    Returns:
        The function return value, and the duration in ms
    """

    def wrapper_timer(*args: Any, **kwargs: dict) -> Any | float:
        """Actually times the function.

        Args:
            *args: the args passed to the function.
            **kwargs: the keyword arguments passed to the function.

        Returns:
            the function return value and its duration.
        """
        start = time.perf_counter()
        value = func(*args, **kwargs)
        stop = time.perf_counter()
        return value, 1000 * (stop - start)

    return wrapper_timer
