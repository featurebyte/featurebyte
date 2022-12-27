"""
Feature dictionary node operators
"""
from sqlglot import Expression

from featurebyte.query_graph.sql.ast.base import SQLNode


class IsInDictionaryNode(SQLNode):
    """
    IsInDictionary node class
    """

    lookup_feature_expression: Expression
    target_dictionary_node: Expression

    @property
    def sql(self) -> Expression:
        object_keys = self.context.adapter.object_keys(self.target_dictionary_node)
        # TODO: do the lookup
        return object_keys
