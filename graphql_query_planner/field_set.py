from dataclasses import dataclass
from typing import Callable, Generic, Optional, TypeVar, cast

from graphql import (
    DirectiveNode,
    FieldNode,
    GraphQLCompositeType,
    GraphQLObjectType,
    SelectionNode,
    SelectionSetNode,
)

from graphql_query_planner.polyfill import flat_map
from graphql_query_planner.shims import GraphQLField
from graphql_query_planner.utilities.array import group_by
from graphql_query_planner.utilities.graphql_ import get_response_name

TParent = TypeVar('TParent', bound=GraphQLCompositeType, contravariant=True)


@dataclass
class Field(Generic[TParent]):
    scope: 'Scope[TParent]'
    field_node: FieldNode
    field_def: GraphQLField


@dataclass
class Scope(Generic[TParent]):
    parent_type: TParent
    possible_types: list[GraphQLObjectType]
    directives: Optional[list[DirectiveNode]] = None
    enclosing_scope: Optional['Scope[GraphQLCompositeType]'] = None


FieldSet = list[Field]


def matches_field(field: Field) -> Callable[[Field], bool]:
    # TODO: Compare parent type and arguments
    def matcher(other_field: Field) -> bool:
        return field.field_def.name == other_field.field_def.name

    return matcher


group_by_response_name = cast(
    Callable[[list[Field]], dict[str, list[Field]]],
    group_by(lambda field: get_response_name(field.field_node)),
)


group_by_parent_type = cast(
    Callable[[list[Field]], dict[GraphQLCompositeType, list[Field]]],
    group_by(lambda field: field.scope.parent_type),
)


def selection_set_from_field_set(
    fields: FieldSet, parent_type: Optional[GraphQLCompositeType] = None
) -> SelectionSetNode:
    def make_selections(item: tuple[GraphQLCompositeType, list[Field]]) -> list[SelectionNode]:
        (type_condition, fields_by_parent_type) = item

        directives = fields_by_parent_type[0].scope.directives

        return wrap_in_inline_fragment_if_needed(
            [
                combine_fields(fields_by_response_name).field_node
                for fields_by_response_name in group_by_response_name(
                    fields_by_parent_type
                ).values()
            ],
            type_condition,
            parent_type,
            directives,
        )

    return SelectionSetNode(
        selections=flat_map(group_by_parent_type(fields).items(), make_selections)
    )


# TODO
def wrap_in_inline_fragment_if_needed(
    selections: list[SelectionNode],
    type_condition: GraphQLCompositeType,
    parent_type: Optional[GraphQLCompositeType] = None,
    directives: Optional[list[DirectiveNode]] = None,
) -> list[SelectionNode]:
    pass


# TODO
def combine_fields(fields: FieldSet) -> Field:
    pass
