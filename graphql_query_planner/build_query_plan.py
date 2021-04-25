from dataclasses import dataclass
from typing import Literal, Optional

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
from graphql_query_planner.query_plan import PlanNode, QueryPlan, ResponsePath


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
        # if an operation is a mutation, we run the root fields in sequence,
        # otherwise we run them in parallel
        node=(flat_wrap('Sequence', nodes) if is_mutation else flat_wrap('Parallel', nodes))
        if nodes
        else None
    )


# TODO
def execution_node_for_group(
    context: 'QueryPlanningContext',
    group: 'FetchGroup',
    parent_type: Optional[GraphQLCompositeType] = None,
) -> PlanNode:
    pass


# TODO
# Wraps the given nodes in a ParallelNode or SequenceNode, unless there's only
# one node, in which case it is returned directly. Any nodes of the same kind
# in the given list have their sub-nodes flattened into the list: ie,
# flatWrap('Sequence', [a, flatWrap('Sequence', b, c), d]) returns a SequenceNode
# with four children.
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
# For mutations, we need to respect the order of the fields, in order to
# determine which fields can be batched together in the same request. If
# they're "split" by fields belonging to other services, then we need to manage
# the proper sequencing at the gateway level. In this example, we need 3
# FetchGroups (requests) in sequence:
#
#    mutation abc {
#      createReview() # reviews service (1)
#      updateReview() # reviews service (1)
#      login() # account service (2)
#      deleteReview() # reviews service (3)
#    }
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
    fields: Optional[FieldSet] = None,
    visited_fragment_names: Optional[dict[FragmentName, bool]] = None,
) -> FieldSet:
    if fields is None:
        fields = []
    if visited_fragment_names is None:
        visited_fragment_names = {}
    return []


ServiceName = str


class FetchGroup:
    service_name: str
    fields: FieldSet
    internal_fragments: set[FragmentDefinitionNode]

    _required_fields: FieldSet
    _provided_fields: FieldSet

    _merge_at: Optional[ResponsePath]

    _dependent_groups_by_service: dict[ServiceName, 'FetchGroup']

    other_dependent_groups: list['FetchGroup']

    def __init__(
        self,
        service_name: str,
        fields: Optional[FieldSet] = None,
        internal_fragments: Optional[set[FragmentDefinitionNode]] = None,
    ):
        if fields is None:
            fields = []
        if internal_fragments is None:
            internal_fragments = set()

        self.service_name = service_name
        self.fields = fields
        self.internal_fragments = internal_fragments

        self._required_fields = []
        self._provided_fields = []

        self._merge_at = None

        self._dependent_groups_by_service = {}

        self.other_dependent_groups = []

    # pylint: disable=protected-access
    def dependent_group_for_service(
        self, service_name: str, required_fields: FieldSet
    ) -> 'FetchGroup':
        group = self._dependent_groups_by_service.get(service_name)

        if group is None:
            group = FetchGroup(service_name)
            group._merge_at = self._merge_at
            self._dependent_groups_by_service[service_name] = group

        if required_fields:
            if group._required_fields:
                group._required_fields.extend(required_fields)
            else:
                group._required_fields = required_fields

            self.fields.extend(required_fields)

        return group

    @property
    def dependent_groups(self) -> list['FetchGroup']:
        groups = list(self._dependent_groups_by_service.values())
        groups.extend(self.other_dependent_groups)
        return groups


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
            def enter_variable_definition(self, definition: VariableDefinitionNode, *_) -> None:
                variable_definitions[definition.variable.name.value] = definition

        self._variable_definitions = variable_definitions

        visit(operation, VariableDefinitionVisitor())

    # TODO
    def new_scope(
        self, parent_type: TParent, enclosing_scope: Optional[GraphQLCompositeType] = None
    ) -> Scope[TParent]:
        pass
