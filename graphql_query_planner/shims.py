from graphql import GraphQLField as PyGraphQLField


class GraphQLField(PyGraphQLField):
    name: str

    # pylint: disable=[super-init-not-called]
    def __init__(self, raw_field: PyGraphQLField, name: str):
        self.__dict__ = raw_field.__dict__
        self.name = name
