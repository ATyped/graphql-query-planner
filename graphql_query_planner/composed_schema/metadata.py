from typing import Optional, Union, cast

from graphql import FieldNode, GraphQLObjectType, InlineFragmentNode

from graphql_query_planner.shims import GraphQLField
from graphql_query_planner.utilities.multi_map import MultiMap


def get_federation_metadata_for_type(
    type_: GraphQLObjectType,
) -> Optional['FederationTypeMetadata']:
    if type_.extensions is not None:
        return cast(Optional[FederationTypeMetadata], type_.extensions.get('federation'))
    else:
        return None


def get_federation_metadata_for_field(
    field: GraphQLField,
) -> Optional['FederationFieldMetadata']:
    if field.extensions is not None:
        return field.extensions.get('federation')
    else:
        return None


GraphName = str

FieldSet = list[Union[FieldNode, InlineFragmentNode]]


class Graph:
    name: str
    url: str


GraphMap = dict[str, Graph]


class FederationSchemaMetadata:
    graphs: GraphMap


FederationTypeMetadata = Union['FederationEntityTypeMetadata', 'FederationValueTypeMetadata']


class FederationEntityTypeMetadata:
    graph_name: GraphName
    keys: MultiMap[GraphName, FieldSet]
    is_value_type = False


class FederationValueTypeMetadata:
    is_value_type = True


def is_entity_type_metadata(metadata: FederationTypeMetadata) -> bool:
    return not metadata.is_value_type


class FederationFieldMetadata:
    graph_name: Optional[GraphName]
    requires: Optional[FieldSet]
    provides: Optional[FieldSet]
