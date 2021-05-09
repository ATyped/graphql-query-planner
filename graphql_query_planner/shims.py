from graphql import GraphQLField as PyGraphQLField


class GraphQLField(PyGraphQLField):
    name: str
