from typing import Optional

from graphql import GraphQLSchema

from graphql_query_planner.build_query_plan import BuildQueryPlanOptions, OperationContext
from graphql_query_planner.query_plan import QueryPlan


class QueryPlanner:
    # There isn't much in this class yet, and I didn't want to make too many
    # changes at once, but since we were already storing a pointer to a
    # Rust query planner instance in the gateway, I think it makes sense to retain
    # that structure. I suspect having a class instead of a stand-alone function
    # will come in handy to encapsulate schema-derived data that is used during
    # planning but isn't operation specific. The next step is likely to be to
    # convert `buildQueryPlan` into a method.

    schema: GraphQLSchema

    def __init__(self, schema: GraphQLSchema):
        self.schema = schema

    def build_query_plan(
        self,
        operation_context: OperationContext,
        options: Optional[BuildQueryPlanOptions],
    ) -> QueryPlan:
        # TODO(#632): We should change the API to avoid confusion, because
        # taking an operationContext with a schema on it isn't consistent
        # with a QueryPlanner instance being bound to a single schema.

        pass
