from dataclasses import dataclass

from graphql import FragmentDefinitionNode, GraphQLSchema, OperationDefinitionNode


@dataclass
class OperationContext:
    schema: GraphQLSchema
    operation: OperationDefinitionNode
    fragments: 'FragmentMap'


FragmentName = str

FragmentMap = dict[FragmentName, FragmentDefinitionNode]


@dataclass
class BuildQueryPlanOptions:
    auto_fragmentization: bool
