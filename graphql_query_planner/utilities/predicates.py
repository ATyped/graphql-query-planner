from typing import Any, Union

from graphql import Undefined, UndefinedType


def is_not_null_or_undefined(value: Union[Any, None, UndefinedType]) -> bool:
    return value is not None and value is not Undefined
