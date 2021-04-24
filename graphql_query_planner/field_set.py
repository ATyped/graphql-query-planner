from typing import Generic, Optional, TypeVar

from graphql import DirectiveNode, FieldNode, GraphQLCompositeType, GraphQLField, GraphQLObjectType

TParent = TypeVar('TParent', bound=GraphQLCompositeType, contravariant=True)


class Field(Generic[TParent]):
    scope: 'Scope[TParent]'
    field_node: FieldNode
    field_def: GraphQLField


class Scope(Generic[TParent]):
    parent_type: TParent
    possible_types: tuple[GraphQLObjectType]
    directives: Optional[tuple[DirectiveNode]]
    enclosing_scope: Optional['Scope[GraphQLCompositeType]']


FieldSet = list[Field]
