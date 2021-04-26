from dataclasses import dataclass
from numbers import Number
from typing import Optional, Union

from graphql import SelectionNode as GraphQLJSSelectionNode

ResponsePath = list[Union[str, Number]]


@dataclass
class QueryPlan:
    kind = 'QueryPlan'
    node: Optional['PlanNode']


PlanNode = Union['SequenceNode', 'ParallelNode', 'FetchNode', 'FlattenNode']


@dataclass
class SequenceNode:
    kind = 'Sequence'
    nodes: list[PlanNode]


@dataclass
class ParallelNode:
    kind = 'Parallel'
    nodes: list[PlanNode]


@dataclass
class FetchNode:
    kind = 'Fetch'
    service_name: str
    variable_usages: Optional[list[str]]
    requires: Optional[list['QueryPlanSelectionNode']]
    operation: str


@dataclass
class FlattenNode:
    kind = 'Flatten'
    path: ResponsePath
    node: PlanNode


# SelectionNodes from GraphQL-js _can_ have a FragmentSpreadNode
# but this SelectionNode is specifically typing the `requires` key
# in a built query plan, where there can't be FragmentSpreadNodes
# since that info is contained in the `FetchNode.operation`
QueryPlanSelectionNode = Union['QueryPlanFieldNode', 'QueryPlanInlineFragmentNode']


@dataclass(frozen=True)
class QueryPlanFieldNode:
    kind = 'Field'
    alias: Optional[str]
    name: str
    selections: Optional[QueryPlanSelectionNode]


@dataclass(frozen=True)
class QueryPlanInlineFragmentNode:  # L56
    kind = 'InlineFragment'
    type_condition: Optional[str]
    selections: list[QueryPlanSelectionNode]


# TODO
def trim_selection_nodes(  # L80
    selections: list[GraphQLJSSelectionNode],
) -> list[QueryPlanSelectionNode]:
    pass
