from typing import Optional, cast

from graphql import (
    FieldNode,
    GraphQLCompositeType,
    GraphQLInterfaceType,
    GraphQLObjectType,
    GraphQLSchema,
    GraphQLUnionType,
    SchemaMetaFieldDef,
    TypeMetaFieldDef,
    TypeNameMetaFieldDef,
)

from graphql_query_planner.shims import GraphQLField


# Not exactly the same as the executor's definition of getFieldDef, in this
# statically evaluated environment we do not always have an Object type,
# and need to handle Interface and Union types.
def get_field_def(
    schema: GraphQLSchema, parent_type: GraphQLCompositeType, field_name: str
) -> Optional[GraphQLField]:
    if field_name == '__schema' and schema.query_type == parent_type:
        result = cast(GraphQLField, SchemaMetaFieldDef)
        result.name = '__schema'
        return result
    if field_name == '__type' and schema.query_type == parent_type:
        result = cast(GraphQLField, TypeMetaFieldDef)
        result.name = '__type'
        return result
    if field_name == '__typename' and isinstance(
        parent_type, (GraphQLObjectType, GraphQLInterfaceType, GraphQLUnionType)
    ):
        result = cast(GraphQLField, TypeNameMetaFieldDef)
        result.name = '__typename'
        return result
    if isinstance(parent_type, (GraphQLObjectType, GraphQLInterfaceType)):
        field: GraphQLField = parent_type.fields[field_name]
        field.name = field_name
        return field

    return None


def get_response_name(node: FieldNode) -> str:
    return node.alias.value if node.alias is not None else node.name.value
