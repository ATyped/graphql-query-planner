from collections import Iterable
from typing import Callable, TypeVar, Union

T = TypeVar('T')
U = TypeVar('U')


def flat_map(iterable: Iterable[T], func: Callable[[T], Union[list[U], U]]) -> list[U]:
    result: list[U] = []
    for e in iterable:
        r = func(e)
        if isinstance(r, Iterable):
            result.extend(r)
        else:
            result.append(r)

    return result
