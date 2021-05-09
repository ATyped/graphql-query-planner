from typing import Generic, TypeVar

K = TypeVar('K')
V = TypeVar('V')


class MultiMap(Generic[K, V], dict[K, list[V]]):
    def add(self, key: K, value: V):
        if (values := self.get(key)) is not None:
            values.append(value)
        else:
            self[key] = [value]
