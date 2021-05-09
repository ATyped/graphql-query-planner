from collections import defaultdict
from typing import Callable, Iterable, TypeVar

T = TypeVar('T')
U = TypeVar('U')


def group_by(key_function: Callable[[T], U]) -> Callable[[Iterable[T]], dict[U, list[T]]]:
    def impl(iterable: Iterable[T]) -> dict[U, list[T]]:
        result = defaultdict(list)

        for element in iterable:
            result[key_function(element)].append(element)

        return result

    return impl
