from dataclasses import dataclass
from typing import Generic, Optional, TypeVar

from graphql import DirectiveNode, FieldNode, GraphQLCompositeType, GraphQLField, GraphQLObjectType

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
