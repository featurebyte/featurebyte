"""
Lag module
"""

from __future__ import annotations

from typing import List, TypeVar, Union

from typeguard import typechecked

from featurebyte.api.view import ViewColumn
from featurebyte.query_graph.enum import NodeOutputType, NodeType

LagColumnTypeT = TypeVar("LagColumnTypeT", bound=ViewColumn)


class LaggableViewColumn(ViewColumn):
    """
    LagMixin provides the `lag` function.
    """

    @typechecked
    def lag(
        self: LagColumnTypeT, entity_columns: Union[str, List[str]], offset: int = 1
    ) -> LagColumnTypeT:
        """
        Lag is a transform that enables the retrieval of the preceding value associated with a particular entity in
        a view.

        This makes it feasible to compute essential features, such as those that depend on inter-event time
        and the proximity to the previous point.

        Parameters
        ----------
        entity_columns : str | list[str]
            Entity columns used when retrieving the lag value.
        offset : int
            The number of rows backward from which to retrieve a value.

        Returns
        -------
        LagColumnTypeT

        Raises
        ------
        ValueError
            If a lag operation has already been applied to the column.

        Examples
        --------
        Create a new column that indicates the prior event timestamp for a Customer.

        >>> event_view = catalog.get_view("GROCERYINVOICE")
        >>> lagged_column = event_view["Timestamp"].lag("GroceryCustomerGuid")


        Create a new column that indicates the 2nd prior event timestamp for a Customer.

        >>> lagged_2_column = event_view["Timestamp"].lag("GroceryCustomerGuid", offset=2)
        """
        if not isinstance(entity_columns, list):
            entity_columns = [entity_columns]
        if NodeType.LAG in self.node_types_lineage:
            raise ValueError("lag can only be applied once per column")
        assert self._parent is not None

        timestamp_column = self._parent.timestamp_column
        assert timestamp_column
        required_columns = entity_columns + [timestamp_column]
        input_nodes = [self.node]
        for col in required_columns:
            input_nodes.append(self._parent[col].node)

        node = self.graph.add_operation(
            node_type=NodeType.LAG,
            node_params={
                "entity_columns": entity_columns,
                "timestamp_column": self._parent.timestamp_column,
                "offset": offset,
            },
            node_output_type=NodeOutputType.SERIES,
            input_nodes=input_nodes,
        )
        return type(self)(
            feature_store=self.feature_store,
            tabular_source=self.tabular_source,
            node_name=node.name,
            name=None,
            dtype=self.dtype,
        )
