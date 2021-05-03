from dataclasses import dataclass
from typing import Generic, Optional, TypeVar

from graphql import (
    DirectiveNode,
    FieldNode,
    GraphQLCompositeType,
    GraphQLField,
    GraphQLObjectType,
    SelectionSetNode,
)

TParent = TypeVar('TParent', bound=GraphQLCompositeType, contravariant=True)


@dataclass
class Field(Generic[TParent]):
    scope: 'Scope[TParent]'
    field_node: FieldNode
    field_def: GraphQLField


@dataclass
class Scope(Generic[TParent]):
    parent_type: TParent
    possible_types: tuple[GraphQLObjectType]
    directives: Optional[tuple[DirectiveNode]] = None
    enclosing_scope: Optional['Scope[GraphQLCompositeType]'] = None


FieldSet = list[Field]


# TODO
def selection_set_from_field_set(  # L108
    fields: FieldSet, parent_type: Optional[GraphQLCompositeType] = None
) -> SelectionSetNode:
    pass
