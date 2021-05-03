from dataclasses import dataclass
from itertools import chain
from typing import Callable, Literal, Optional, Union, cast

from graphql import (
    ArgumentNode,
    DefinitionNode,
    DocumentNode,
    FieldNode,
    FragmentDefinitionNode,
    GraphQLCompositeType,
    GraphQLError,
    GraphQLField,
    GraphQLObjectType,
    GraphQLSchema,
    ListTypeNode,
    NamedTypeNode,
    NameNode,
    NonNullTypeNode,
    OperationDefinitionNode,
    OperationType,
    SelectionSetNode,
    VariableDefinitionNode,
    VariableNode,
    Visitor,
    get_operation_root_type,
    print_ast,
    strip_ignored_characters,
    visit,
)

from graphql_query_planner.field_set import (
    Field,
    FieldSet,
    Scope,
    TParent,
    selection_set_from_field_set,
)
from graphql_query_planner.query_plan import (
    FetchNode,
    FlattenNode,
    ParallelNode,
    PlanNode,
    QueryPlan,
    ResponsePath,
    SequenceNode,
    trim_selection_nodes,
)


@dataclass
class OperationContext:  # L87
    schema: GraphQLSchema
    operation: OperationDefinitionNode
    fragments: 'FragmentMap'


FragmentName = str

FragmentMap = dict[FragmentName, FragmentDefinitionNode]


@dataclass
class BuildQueryPlanOptions:
    auto_fragmentization: bool


# TODO: impl debug
def build_query_plan(  # L99
    operation_context: OperationContext,
    options: Optional[BuildQueryPlanOptions] = None,
) -> QueryPlan:
    if options is None:
        options = BuildQueryPlanOptions(auto_fragmentization=False)

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


def execution_node_for_group(  # L150
    context: 'QueryPlanningContext',
    group: 'FetchGroup',
    parent_type: Optional[GraphQLCompositeType] = None,
) -> PlanNode:
    selection_set = selection_set_from_field_set(group.fields, parent_type)
    requires = (
        selection_set_from_field_set(group.required_fields)
        if len(group.required_fields) > 0
        else None
    )
    variable_usages = context.get_variable_usages(selection_set, group.internal_fragments)

    operation = (
        operation_for_entities_fetch(selection_set, variable_usages, group.internal_fragments)
        if requires is not None
        else operation_for_root_fetch(
            selection_set, variable_usages, group.internal_fragments, context.operation.operation
        )
    )

    fetch_node = FetchNode(
        service_name=group.service_name,
        requires=trim_selection_nodes(requires.selections) if requires is not None else None,
        variable_usages=list(variable_usages.keys()),
        operation=strip_ignored_characters(print_ast(operation)),
    )

    node: PlanNode = (
        FlattenNode(path=group.merge_at, node=fetch_node)
        if group.merge_at is not None and len(group.merge_at) > 0
        else fetch_node
    )

    if len(group.dependent_groups) > 0:
        dependent_nodes = [
            execution_node_for_group(context, dependent_group)
            for dependent_group in group.dependent_groups
        ]

        return flat_wrap('Sequence', [node, flat_wrap('Parallel', dependent_nodes)])
    else:
        return node


VariableName = str
VariableUsages = dict[VariableName, VariableDefinitionNode]  # L213


def map_fetch_node_to_variable_definitions(
    variable_usages: VariableUsages,
) -> list[VariableDefinitionNode]:
    return list(variable_usages.values())


def operation_for_root_fetch(  # L223
    selection_set: SelectionSetNode,
    variable_usages: VariableUsages,
    internal_fragments: set[FragmentDefinitionNode],
    operation: Optional[OperationType] = None,
) -> DocumentNode:
    definitions: list[DefinitionNode] = [
        OperationDefinitionNode(
            operation=operation,
            selection_set=selection_set,
            variable_definitions=map_fetch_node_to_variable_definitions(variable_usages),
        )
    ]
    definitions.extend(internal_fragments)
    return DocumentNode(definitions=definitions)


def operation_for_entities_fetch(  # L248
    selection_set: SelectionSetNode,
    variable_usages: VariableUsages,
    internal_fragments: set[FragmentDefinitionNode],
) -> DocumentNode:
    representations_variable = VariableNode(name=NameNode(value='representations'))

    return DocumentNode(
        definitions=list(
            chain(
                [
                    OperationDefinitionNode(
                        operation=OperationType.QUERY,
                        variable_definitions=list(
                            chain(
                                [
                                    VariableDefinitionNode(
                                        variable=representations_variable,
                                        type=NonNullTypeNode(
                                            type=ListTypeNode(
                                                type=NonNullTypeNode(
                                                    type=NamedTypeNode(name=NameNode(value='_Any'))
                                                )
                                            )
                                        ),
                                    )
                                ],
                                map_fetch_node_to_variable_definitions(variable_usages),
                            )
                        ),
                        selection_set=SelectionSetNode(
                            selections=[
                                FieldNode(
                                    name=NameNode(value='_entities'),
                                    arguments=[
                                        ArgumentNode(
                                            name=NameNode(
                                                value=representations_variable.name.value
                                            ),
                                            value=representations_variable,
                                        )
                                    ],
                                    selection_set=selection_set,
                                )
                            ]
                        ),
                    ),
                ],
                internal_fragments,
            )
        )
    )


# Wraps the given nodes in a ParallelNode or SequenceNode, unless there's only
# one node, in which case it is returned directly. Any nodes of the same kind
# in the given list have their sub-nodes flattened into the list: ie,
# flatWrap('Sequence', [a, flatWrap('Sequence', b, c), d]) returns a SequenceNode
# with four children.
def flat_wrap(kind: Literal['Parallel', 'Sequence'], nodes: list[PlanNode]) -> PlanNode:  # L320
    # Notice: the 'Parallel' is the value of ParallelNode.kind
    # and the 'Sequence' is the value of SequenceNode.kind
    if len(nodes) == 0:
        raise Exception('programming error: should always be called with nodes')
    if len(nodes) == 1:
        return nodes[0]
    nodes = list(
        chain.from_iterable(
            cast(Union[ParallelNode, SequenceNode], n).nodes if n.kind == kind else [n]
            for n in nodes
        )
    )

    return ParallelNode(nodes=nodes) if kind == 'Parallel' else SequenceNode(nodes=nodes)


def split_root_fields(  # L336
    context: 'QueryPlanningContext',
    fields: FieldSet,
) -> list['FetchGroup']:
    groups_by_service: dict[ServiceName, FetchGroup] = {}

    def group_for_service(service_name: str) -> FetchGroup:
        group = groups_by_service.get(service_name)

        if group is None:
            group = FetchGroup(service_name)
            groups_by_service[service_name] = group

        return group

    def group_for_field(field: Field[GraphQLObjectType]) -> FetchGroup:
        scope = field.scope
        field_node = field.field_node
        field_def = field.field_def
        parent_type = scope.parent_type

        owning_service = context.get_owning_service(parent_type, field_def)

        if owning_service is None:
            raise GraphQLError(
                "Couldn't find owning service for field "
                f'{parent_type.name}.'
                f'{field_def.ast_node.name.value if field_def.ast_node else field_def}',
                field_node,
            )

        return group_for_service(owning_service)

    split_fields(context, [], fields, group_for_field)

    return list(groups_by_service.values())


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
def split_root_fields_serially(  # L386
    context: 'QueryPlanningContext',
    fields: FieldSet,
) -> list['FetchGroup']:
    pass


# TODO
def split_fields(
    context: 'QueryPlanningContext',
    path: ResponsePath,
    fields: FieldSet,
    group_for_field: Callable[[Field[GraphQLObjectType]], 'FetchGroup'],
):
    pass


# TODO
def collect_fields(  # L834
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


class FetchGroup:  # L954
    service_name: str
    fields: FieldSet
    internal_fragments: set[FragmentDefinitionNode]

    required_fields: FieldSet
    provided_fields: FieldSet

    merge_at: Optional[ResponsePath]

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

        self.required_fields = []
        self.provided_fields = []

        self.merge_at = None

        self._dependent_groups_by_service = {}

        self.other_dependent_groups = []

    # pylint: disable=protected-access
    def dependent_group_for_service(
        self, service_name: str, required_fields: FieldSet
    ) -> 'FetchGroup':
        group = self._dependent_groups_by_service.get(service_name)

        if group is None:
            group = FetchGroup(service_name)
            group.merge_at = self.merge_at
            self._dependent_groups_by_service[service_name] = group

        if required_fields:
            if group.required_fields:
                group.required_fields.extend(required_fields)
            else:
                group.required_fields = required_fields

            self.fields.extend(required_fields)

        return group

    @property
    def dependent_groups(self) -> list['FetchGroup']:
        groups = list(self._dependent_groups_by_service.values())
        groups.extend(self.other_dependent_groups)
        return groups


def build_query_planning_context(  # L1056
    operation_context: OperationContext,
    options: BuildQueryPlanOptions = BuildQueryPlanOptions(auto_fragmentization=False),
) -> 'QueryPlanningContext':
    return QueryPlanningContext(
        operation_context.schema,
        operation_context.operation,
        operation_context.fragments,
        options.auto_fragmentization,
    )


# TODO
class QueryPlanningContext:  # L1068
    schema: GraphQLSchema
    operation: OperationDefinitionNode
    fragments: FragmentMap
    auto_fragmentization: bool

    _variable_definitions: dict[VariableName, VariableDefinitionNode]

    def __init__(  # L1084
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
    def get_variable_usages(  # L1121
        self, selection_set: SelectionSetNode, fragments: set[FragmentDefinitionNode]
    ) -> VariableUsages:
        pass

    # TODO
    def new_scope(  # L1148
        self, parent_type: TParent, enclosing_scope: Optional[GraphQLCompositeType] = None
    ) -> Scope[TParent]:
        pass

    # TODO
    def get_owning_service(
        self, parent_type: GraphQLObjectType, field_def: GraphQLField
    ) -> Optional[str]:  # L1168
        pass
