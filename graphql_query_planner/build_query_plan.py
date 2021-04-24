from dataclasses import dataclass
from typing import Literal

from graphql import (
    FragmentDefinitionNode,
    GraphQLCompositeType,
    GraphQLError,
    GraphQLSchema,
    OperationDefinitionNode,
    OperationType,
    SelectionSetNode,
    VariableDefinitionNode,
    Visitor,
    get_operation_root_type,
    visit,
)

from graphql_query_planner.field_set import FieldSet, Scope, TParent
from graphql_query_planner.query_plan import PlanNode, QueryPlan


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


# TODO: impl debug
def build_query_plan(
    operation_context: OperationContext,
    options: BuildQueryPlanOptions = BuildQueryPlanOptions(auto_fragmentization=False),
) -> QueryPlan:
    context = build_query_planning_context(operation_context, options)

    if context.operation.operation == OperationType.SUBSCRIPTION:
        raise GraphQLError(
            'Query planning does not support subscriptions for now.', [context.operation]
        )

    root_type = get_operation_root_type(context.schema, context.operation)
    is_mutation = context.operation.operation == OperationType.MUTATION
    # debug.log(() => `Building plan for ${isMutation ? "mutation" : "query"} `
    #                 `"${rootType}" (fragments: [${Object.keys(context.fragments)}], `
    #                 `autoFragmentization: ${context.autoFragmentization})`);
    # debug.group(`Collecting root fields:`);
    fields = collect_fields(context, context.new_scope(root_type), context.operation.selection_set)
    # debug.groupEnd(`Collected root fields:`);
    # debug.groupedValues(fields, debugPrintField);

    # debug.group('Splitting root fields:');

    # Mutations are a bit more specific in how FetchGroups can be built, as some
    # calls to the same service may need to be executed serially.
    groups = (
        split_root_fields_serially(context, fields)
        if is_mutation
        else split_root_fields(context, fields)
    )
    # debug.groupEnd('Computed groups:');
    # debug.groupedValues(groups, debugPrintGroup);

    nodes = [execution_node_for_group(context, group, root_type) for group in groups]

    return QueryPlan(
        node=flat_wrap('Sequence' if is_mutation else 'Parallel', nodes) if nodes else None
    )


# TODO
def execution_node_for_group(
    context: 'QueryPlanningContext', group: 'FetchGroup', parent_type: GraphQLCompositeType = None
) -> PlanNode:
    pass


# TODO
def flat_wrap(kind: Literal['Parallel', 'Sequence'], nodes: list[PlanNode]) -> PlanNode:
    # Notice: the 'Parallel' is the value of ParallelNode.kind
    # and the 'Sequence' is the value of SequenceNode.kind
    pass


# TODO
def split_root_fields(
    context: 'QueryPlanningContext',
    fields: FieldSet,
) -> list['FetchGroup']:
    pass


# TODO
def split_root_fields_serially(
    context: 'QueryPlanningContext',
    fields: FieldSet,
) -> list['FetchGroup']:
    pass


# TODO
def collect_fields(
    context: 'QueryPlanningContext',
    scope: Scope[GraphQLCompositeType],
    selection_set: SelectionSetNode,
    fields: FieldSet = None,
    visited_fragment_names: dict[FragmentName, bool] = None,
) -> FieldSet:
    if fields is None:
        fields = []
    if visited_fragment_names is None:
        visited_fragment_names = {}
    return []


# TODO
class FetchGroup:
    pass


def build_query_planning_context(
    operation_context: OperationContext,
    options: BuildQueryPlanOptions = BuildQueryPlanOptions(auto_fragmentization=False),
) -> 'QueryPlanningContext':
    return QueryPlanningContext(
        operation_context.schema,
        operation_context.operation,
        operation_context.fragments,
        options.auto_fragmentization,
    )


VariableName = str


# TODO
class QueryPlanningContext:
    schema: GraphQLSchema
    operation: OperationDefinitionNode
    fragments: FragmentMap
    auto_fragmentization: bool

    _variable_definitions: dict[VariableName, VariableDefinitionNode]

    def __init__(
        self,
        schema: GraphQLSchema,
        operation: OperationDefinitionNode,
        fragments: FragmentMap,
        auto_fragmentization: bool,
    ):
        self.schema = schema
        self.operation = operation
        self.fragments = fragments
        self.auto_fragmentization = auto_fragmentization

        variable_definitions = {}

        # noinspection PyMethodMayBeStatic
        class VariableDefinitionVisitor(Visitor):
            def enter_variable_definition(self, definition: VariableDefinitionNode, *_):
                variable_definitions[definition.variable.name.value] = definition

        self._variable_definitions = variable_definitions

        visit(operation, VariableDefinitionVisitor())

    # TODO
    def new_scope(
        self, parent_type: TParent, enclosing_scope: GraphQLCompositeType = None
    ) -> Scope[TParent]:
        pass
