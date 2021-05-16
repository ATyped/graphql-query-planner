from copy import copy
from dataclasses import dataclass
from itertools import chain
from typing import Callable, Literal, Optional, Union, cast

from graphql import (
    ArgumentNode,
    DefinitionNode,
    DocumentNode,
    FieldNode,
    FragmentDefinitionNode,
    FragmentSpreadNode,
    GraphQLCompositeType,
    GraphQLError,
    GraphQLObjectType,
    GraphQLSchema,
    GraphQLType,
    GraphQLWrappingType,
    InlineFragmentNode,
    ListTypeNode,
    NamedTypeNode,
    NameNode,
    NonNullTypeNode,
    OperationDefinitionNode,
    OperationType,
    SelectionSetNode,
    TypeNameMetaFieldDef,
    VariableDefinitionNode,
    VariableNode,
    Visitor,
    get_named_type,
    get_operation_root_type,
    is_abstract_type,
    is_composite_type,
    is_list_type,
    is_named_type,
    is_object_type,
    print_ast,
    strip_ignored_characters,
    type_from_ast,
    visit,
)

from graphql_query_planner.composed_schema.metadata import (
    get_federation_metadata_for_field,
    get_federation_metadata_for_type,
)
from graphql_query_planner.field_set import (
    Field,
    FieldSet,
    Scope,
    TParent,
    group_by_parent_type,
    group_by_response_name,
    matches_field,
    selection_set_from_field_set,
)
from graphql_query_planner.polyfill import flat_map
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
from graphql_query_planner.shims import GraphQLField
from graphql_query_planner.utilities.graphql_ import get_field_def, get_response_name
from graphql_query_planner.utilities.multi_map import MultiMap
from graphql_query_planner.utilities.predicates import is_not_null_or_undefined

typename_field = FieldNode(name=NameNode(value='__typename'))


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


def execution_node_for_group(
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
VariableUsages = dict[VariableName, VariableDefinitionNode]


def map_fetch_node_to_variable_definitions(
    variable_usages: VariableUsages,
) -> list[VariableDefinitionNode]:
    return list(variable_usages.values())


def operation_for_root_fetch(
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


def operation_for_entities_fetch(
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
def flat_wrap(kind: Literal['Parallel', 'Sequence'], nodes: list[PlanNode]) -> PlanNode:
    # Notice: the 'Parallel' is the value of ParallelNode.kind
    # and the 'Sequence' is the value of SequenceNode.kind
    if len(nodes) == 0:
        raise Exception('programming error: should always be called with nodes')
    if len(nodes) == 1:
        return nodes[0]

    return (
        ParallelNode(
            nodes=flat_map(nodes, lambda n: cast(ParallelNode, n).nodes if n.kind == kind else [n])
        )
        if kind == 'Parallel'
        else SequenceNode(nodes=nodes)
    )


# noinspection DuplicatedCode
def split_root_fields(
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
# noinspection DuplicatedCode
def split_root_fields_serially(
    context: 'QueryPlanningContext',
    fields: FieldSet,
) -> list['FetchGroup']:
    fetch_groups: list[FetchGroup] = []

    # TODO: raw codes use `groupForField`, which is unreasonable
    def group_for_service(service_name: str) -> FetchGroup:
        # If the most recent FetchGroup in the array belongs to the same service,
        # the field in question can be batched within that group.
        previous_group = fetch_groups[-1]
        if previous_group and previous_group.service_name == service_name:
            return previous_group

        # If there's no previous group, or the previous group is from a different
        # service, then we need to add a new FetchGroup.
        group = FetchGroup(service_name)
        fetch_groups.append(group)

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

    return fetch_groups


def split_subfields(
    context: 'QueryPlanningContext',
    path: ResponsePath,
    fields: FieldSet,
    parent_group: 'FetchGroup',
) -> None:
    def group_for_field(field: Field[GraphQLObjectType]) -> FetchGroup:
        scope = field.scope
        field_node = field.field_node
        field_def = field.field_def
        parent_type = scope.parent_type

        # Committed by @trevor-scheer but authored by @martijnwalraven
        # Treat abstract types as value types to replicate type explosion fix
        # XXX: this replicates the behavior of the Rust query planner implementation,
        # in order to get the tests passing before making further changes. But the
        # type explosion fix this depends on is fundamentally flawed and needs to
        # be replaced.
        parent_is_value_type = (
            not is_object_type(parent_type) or federation_metadata.is_value_type
            if (federation_metadata := get_federation_metadata_for_type(parent_type))
            else None
        )

        if parent_is_value_type:
            base_service: Optional[str] = parent_group.service_name
            owning_service: Optional[str] = parent_group.service_name
        else:
            base_service = context.get_base_service(parent_type)
            owning_service = context.get_owning_service(parent_type, field_def)

        if not base_service:
            raise GraphQLError(
                f"Couldn't find base service for type {parent_type.name}", field_node
            )

        if not owning_service:
            raise GraphQLError(
                f"Couldn't find owning service for field {parent_type.name}.{field_def.name}",
                field_node,
            )

        # Is the field defined on the base service?
        if owning_service == base_service:
            # Can we fetch the field from the parent group?

            if owning_service == parent_group.service_name or any(
                matches_field(field)(f) for f in parent_group.provided_fields
            ):
                return parent_group
            else:
                # We need to fetch the key fields from the parent group first, and then
                # use a dependent fetch from the owning service.
                key_fields = context.get_key_fields(parent_type, parent_group.service_name)
                if len(key_fields) == 0 or (
                    len(key_fields) == 1 and key_fields[0].field_def.name == '__typename'
                ):
                    # Only __typename key found.
                    # In some cases, the parent group does not have any @key directives.
                    # Fall back to owning group's keys
                    key_fields = context.get_key_fields(parent_type, owning_service)
                return parent_group.dependent_group_for_service(owning_service, key_fields)
        else:
            # It's an extension field, so we need to fetch the required fields first.
            required_fields = context.get_required_fields(parent_type, field_def, owning_service)

            # Can we fetch the required fields from the parent group?
            if all(
                any(
                    matches_field(required_field)(provided_field)
                    for provided_field in parent_group.provided_fields
                )
                for required_field in required_fields
            ):
                if owning_service == parent_group.service_name:
                    return parent_group
                else:
                    return parent_group.dependent_group_for_service(owning_service, required_fields)
            else:
                # We need to go through the base group first.

                key_fields = context.get_key_fields(parent_type, parent_group.service_name)

                if not key_fields:
                    raise GraphQLError(
                        f"Couldn't find keys for type \"{parent_type.name}\" "
                        f'in service "{base_service}"',
                        field_node,
                    )

                if base_service == parent_group.service_name:
                    return parent_group.dependent_group_for_service(owning_service, required_fields)

                base_group = parent_group.dependent_group_for_service(base_service, key_fields)

                return base_group.dependent_group_for_service(owning_service, required_fields)

    split_fields(context, path, fields, group_for_field)


def split_fields(
    context: 'QueryPlanningContext',
    path: ResponsePath,
    fields: FieldSet,
    group_for_field: Callable[[Field[GraphQLObjectType]], 'FetchGroup'],
):
    for fields_for_response_name in group_by_response_name(fields).values():
        for parent_type, fields_for_parent_type in group_by_parent_type(
            fields_for_response_name
        ).items():
            # Field nodes that share the same response name and parent type are guaranteed
            # to have the same field name and arguments. We only need the other nodes when
            # merging selection sets, to take node-specific subfields and directives
            # into account.

            # debug.group(() = > debugPrintFields(fieldsForParentType));

            field = fields_for_parent_type[0]
            scope = field.scope
            field_def = field.field_def

            # We skip `__typename` for root types.
            # TODO: graphql-core's GraphQLField doesn't conform to the origin
            # if (fieldDef.name === TypeNameMetaFieldDef.name) {
            if field_def.name == '__typename':
                schema = context.schema
                roots = [
                    # the redundant cast makes mypy understand `t is not None`
                    cast(GraphQLObjectType, t).name
                    for t in [schema.query_type, schema.mutation_type, schema.subscription_type]
                    if is_not_null_or_undefined(t)
                ]
                try:
                    roots.index(parent_type.name)
                    # debug.groupEnd("Skipping __typename for root types");
                    continue
                except ValueError:
                    pass

            # We skip introspection fields like `__schema` and `__type`.
            if is_object_type(parent_type) and parent_type in scope.possible_types:
                # If parent type is an object type, we can directly look for the right
                # group.

                # debug.log(() => `${parentType} = object and ${parentType}`
                #   ` ∈ [${scope.possibleTypes}]`);
                group = group_for_field(field)
                # debug.log(() => `Initial fetch group for fields: ${debugPrintGroup(group)}`);
                group.fields.append(
                    complete_field(context, scope, group, path, fields_for_parent_type)
                )
                # debug.groupEnd(() => `Updated fetch group: ${debugPrintGroup(group)}`);
            else:
                # debug.log(() => `${parentType} ≠ object or ${parentType}`
                #   ` ∉ [${scope.possibleTypes}]`);

                # For interfaces however, we need to look at all possible runtime types.

                # The following is an optimization to prevent an explosion of type
                # conditions to services when it isn't needed. If all possible runtime
                # types can be fulfilled by only one service then we don't need to
                # expand the fields into unique type conditions.

                # Collect all of the field defs on the possible runtime types
                possible_field_defs = [
                    context.get_field_def(runtime_type, field.field_node)
                    for runtime_type in scope.possible_types
                ]

                # If none of the field defs have a federation property, this interface's
                # implementors can all be resolved within the same service.
                has_no_extending_field_defs = not any(
                    get_federation_metadata_for_field(field) for field in possible_field_defs
                )

                # With no extending field definitions, we can engage the optimization
                if has_no_extending_field_defs:
                    # debug.group(() => `No field of ${scope.possibleTypes} have federation`
                    #   ` directives, avoid type explosion.`);
                    group = group_for_field(field)
                    # debug.groupEnd(() => `Initial fetch group for fields: `
                    #   `${debugPrintGroup(group)}`);
                    group.fields.append(
                        complete_field(context, scope, group, path, fields_for_parent_type)
                    )
                    # debug.groupEnd(() => `Updated fetch group: ${debugPrintGroup(group)}`);
                    continue

                # We keep track of which possible runtime parent types can be fetched
                # from which group,
                groups_by_runtime_parent_types = MultiMap[FetchGroup, GraphQLObjectType]()

                # debug.group('Computing fetch groups by runtime parent types');
                for runtime_parent_type in scope.possible_types:
                    field_def = context.get_field_def(runtime_parent_type, field.field_node)
                    groups_by_runtime_parent_types.add(
                        group_for_field(
                            Field(
                                scope=context.new_scope(runtime_parent_type, scope),
                                field_node=field.field_node,
                                field_def=field_def,
                            )
                        ),
                        runtime_parent_type,
                    )

                # debug.groupEnd(`Fetch groups to resolvable runtime types:`);
                # debug.groupedEntries(groupsByRuntimeParentTypes, debugPrintGroup, `
                #   `(v) => v.toString());

                # debug.group('Iterating on fetch groups');

                # We add the field separately for each runtime parent type.
                for group, runtime_parent_types in groups_by_runtime_parent_types.items():
                    # debug.group(() => `For initial fetch group ${debugPrintGroup(group)}:`);
                    for runtime_parent_type in runtime_parent_types:
                        # We need to adjust the fields to contain the right fieldDef for
                        # their runtime parent type.

                        # debug.group(`For runtime parent type ${runtimeParentType}:`);

                        field_def = context.get_field_def(runtime_parent_type, field.field_node)

                        fields_with_runtime_parent_type = []
                        for x in fields_for_parent_type:
                            x = copy(x)
                            x.field_def = field_def
                            fields_with_runtime_parent_type.append(x)

                        group.fields.append(
                            complete_field(
                                context,
                                context.new_scope(runtime_parent_type, scope),
                                group,
                                path,
                                fields_with_runtime_parent_type,
                            ),
                        )
                        # debug.groupEnd(() => `Updated fetch group: ${debugPrintGroup(group)}`);
                    # debug.groupEnd();
                # debug.groupEnd();  # Group started before the immediate for loop

                # debug.groupEnd();  # Group started at the beginning of this 'top-level' iteration.


def complete_field(
    context: 'QueryPlanningContext',
    scope: Scope[GraphQLCompositeType],
    parent_group: 'FetchGroup',
    path: ResponsePath,
    fields: FieldSet,
) -> Field:
    field_node = fields[0].field_node
    field_def = fields[0].field_def
    return_type = get_named_type(field_def.type)

    if not is_composite_type(return_type):
        # FIXME: We should look at all field nodes to make sure we take directives
        # into account (or remove directives for the time being).
        return Field(scope=scope, field_node=field_node, field_def=field_def)
    else:
        # For composite types, we need to recurse.

        field_path = add_path(path, get_response_name(field_node), field_def.type)

        sub_group = FetchGroup(parent_group.service_name)
        sub_group.merge_at = field_path

        sub_group.provided_fields = context.get_provided_fields(
            field_def, parent_group.service_name
        )

        # For abstract types, we always need to request `__typename`
        if is_abstract_type(return_type):

            sub_group.fields.append(
                Field(
                    scope=context.new_scope(cast(GraphQLCompositeType, return_type), scope),
                    field_node=typename_field,
                    field_def=GraphQLField(TypeNameMetaFieldDef, name='__typename'),
                )
            )

        sub_fields = collect_subfields(context, cast(GraphQLCompositeType, return_type), fields)
        # debug.group(() => `Splitting collected sub-fields (${debugPrintFields(subfields)})`);
        split_subfields(context, field_path, sub_fields, sub_group)
        # debug.groupEnd();

        parent_group.other_dependent_groups.extend(sub_group.dependent_groups)

        selection_set = selection_set_from_field_set(
            sub_group.fields, cast(GraphQLCompositeType, return_type)
        )

        if context.auto_fragmentization and len(sub_group.fields) > 2:
            internal_fragment = get_internal_fragment(
                selection_set, cast(GraphQLCompositeType, return_type), context
            )
            definition = internal_fragment.definition
            selection_set = internal_fragment.selection_set
            parent_group.internal_fragments.add(definition)

        # "Hoist" internalFragments of the subGroup into the parentGroup so all
        # fragments can be included in the final request for the root FetchGroup
        parent_group.internal_fragments.update(sub_group.internal_fragments)

        new_field_node = copy(field_node)
        new_field_node.selection_set = selection_set
        return Field(scope=scope, field_node=new_field_node, field_def=field_def)


def get_internal_fragment(
    selection_set: SelectionSetNode,
    return_type: GraphQLCompositeType,
    context: 'QueryPlanningContext',
) -> 'InternalFragment':
    # TODO: is key correct ?
    key = str(hash(selection_set))  # const key = JSON.stringify(selectionSet);
    if key not in context.internal_fragments:
        name = f'__QueryPlanFragment_{context.internal_fragment_count}'
        context.internal_fragment_count += 1

        definition = FragmentDefinitionNode(
            name=NameNode(value=name),
            type_condition=NamedTypeNode(name=NameNode(value=return_type.name)),
            selection_set=selection_set,
        )

        fragment_selection = SelectionSetNode(
            selections=[FragmentSpreadNode(name=NameNode(value=name))]
        )

        context.internal_fragments[key] = InternalFragment(
            name=name, definition=definition, selection_set=fragment_selection
        )

    return context.internal_fragments[key]


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

    def get_fragment_condition(
        fragment_: Union[FragmentDefinitionNode, InlineFragmentNode]
    ) -> GraphQLCompositeType:
        type_condition_node = fragment_.type_condition
        # FIXME: unreachable
        # if type_condition_node is None:
        #     return scope.parent_type

        return cast(GraphQLCompositeType, type_from_ast(context.schema, type_condition_node))

    for selection in selection_set.selections:
        if selection.kind == FieldNode.kind:
            selection = cast(FieldNode, selection)
            field_def = context.get_field_def(scope.parent_type, selection)
            fields.append(Field(scope=scope, field_node=selection, field_def=field_def))
        elif selection.kind == InlineFragmentNode.kind:
            selection = cast(InlineFragmentNode, selection)
            new_scope = context.new_scope(get_fragment_condition(selection), scope)
            if len(new_scope.possible_types) == 0:
                pass
            else:
                # Committed by @trevor-scheer but authored by @martijnwalraven
                # This replicates the behavior added to the Rust query planner in #178.
                # Unfortunately, the logic in there is seriously flawed, and may lead to
                # unexpected results. The assumption seems to be that fields with the
                # same parent type are always nested in the same inline fragment. That
                # is not necessarily true however, and because we take the directives
                # from the scope of the first field with a particular parent type,
                # those directives will be applied to all other fields that have the
                # same parent type even if the directives aren't meant to apply to them
                # because they were nested in a different inline fragment. (That also
                # means that if the scope of the first field doesn't have directives,
                # directives that would have applied to other fields will be lost.)
                # Note that this also applies to `@skip` and `@include`, which could
                # lead to invalid query plans that fail at runtime because expected
                # fields are missing from a subgraph response.
                new_scope.directives = selection.directives

                collect_fields(
                    context, new_scope, selection.selection_set, fields, visited_fragment_names
                )
        elif selection.kind == FragmentSpreadNode.kind:
            fragment_name = cast(FragmentSpreadNode, selection).name.value

            fragment = context.fragments[fragment_name]
            # FIXME: unreachable
            # if fragment is None:
            #     continue

            new_scope = context.new_scope(get_fragment_condition(fragment), scope)
            if len(new_scope.possible_types) == 0:
                continue

            if visited_fragment_names[fragment_name]:
                continue
            visited_fragment_names[fragment_name] = True

            collect_fields(
                context, new_scope, fragment.selection_set, fields, visited_fragment_names
            )

    return fields


# TODO
# Collecting subfields collapses parent types, because it merges
# selection sets without taking the runtime parent type of the field
# into account. If we want to keep track of multiple levels of possible
# types, this is where that would need to happen.
def collect_subfields(
    context: 'QueryPlanningContext', return_type: GraphQLCompositeType, fields: FieldSet
) -> FieldSet:
    pass


ServiceName = str


class FetchGroup:
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


@dataclass
class InternalFragment:
    name: str
    definition: FragmentDefinitionNode
    selection_set: SelectionSetNode


# TODO
class QueryPlanningContext:  # L1068
    internal_fragments: dict[str, InternalFragment]

    internal_fragment_count: int

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
        self.internal_fragments = {}
        self.internal_fragment_count = 0

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

    def get_field_def(
        self, parent_type: GraphQLCompositeType, field_node: FieldNode
    ) -> GraphQLField:
        field_name = field_node.name.value

        field_def = get_field_def(self.schema, parent_type, field_name)

        if field_def is None:
            raise GraphQLError(
                f'Cannot query field {field_node.name.value}' ' on type ${parent_type}', field_node
            )

        return field_def

    # TODO
    def get_variable_usages(  # L1121
        self, selection_set: SelectionSetNode, fragments: set[FragmentDefinitionNode]
    ) -> VariableUsages:
        pass

    # TODO
    def new_scope(  # L1148
        self, parent_type: TParent, enclosing_scope: Optional[Scope[GraphQLCompositeType]] = None
    ) -> Scope[TParent]:
        pass

    # TODO
    def get_base_service(self, parent_type: GraphQLObjectType) -> Optional[str]:
        pass

    # TODO
    def get_owning_service(
        self, parent_type: GraphQLObjectType, field_def: GraphQLField
    ) -> Optional[str]:  # L1168
        pass

    # TODO
    def get_key_fields(
        self, parent_type: GraphQLCompositeType, service_name: str, fetch_all: Optional[bool] = None
    ) -> FieldSet:
        pass

    # TODO
    def get_required_fields(
        self, parent_type: GraphQLCompositeType, field_def: GraphQLField, service_name: str
    ) -> FieldSet:
        pass

    # TODO
    def get_provided_fields(self, field_def: GraphQLField, service_name: str) -> FieldSet:
        pass


def add_path(path: ResponsePath, response_name: str, type_: GraphQLType):
    path = path[:]
    path.append(response_name)

    while not is_named_type(type_):
        if is_list_type(type_):
            path.append('@')

        # GraphQLType have two direct subtypes: GraphQLNamedType and GraphQLWrappingType
        # if type_ is not a GraphQLNamedType, then it must be a GraphQLWrappingType
        type_ = cast(GraphQLWrappingType, type_).of_type

    return path
