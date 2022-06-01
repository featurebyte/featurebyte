"""
EventSource class
"""
from __future__ import annotations

from typing import Optional

from featurebyte.core.frame import Frame, Series
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import Node, QueryGraph
from featurebyte.session.base import BaseSession, TableName


class EventSource(Frame):
    """
    EventSource class
    """

    def __repr__(self) -> str:
        return f"EventSource(node.name={self.node.name})"

    @property
    def inception_node(self) -> Node:
        """
        Input node where the event source is introduced to the query graph
        """
        graph = QueryGraph()
        return graph.get_node_by_name(self.row_index_lineage[0])

    @property
    def timestamp_column(self) -> str:
        """
        Timestamp column of the event source
        """
        return self.inception_node.parameters["timestamp"]

    @property
    def entity_identifiers(self):
        """
        Entity ids of the event source
        """
        return self.inception_node.parameters.get("entity_identifiers")

    @property
    def protected_columns(self):
        """
        Special columns where its value should not be overridden
        """
        columns = [self.timestamp_column]
        if self.entity_identifiers:
            columns.extend(self.entity_identifiers)
        return set(columns)

    @classmethod
    def from_session(
        cls,
        session: BaseSession,
        table_name: TableName,
        timestamp_column: str,
        entity_identifiers: list[str] | None = None,
    ):
        """
        Construct an EventSource object using session object

        Parameters
        ----------
        session: BaseSession
            database session object to retrieve database metadata
        table_name: str
            table name of the event source
        timestamp_column: str
            timestamp column of the event source
        entity_identifiers: str
            entity id of the event source

        Returns
        -------
        EventSource
            constructed EventSource object

        Raises
        ------
        KeyError
            if the table name does not exist in the session's database metadata
        """
        if table_name not in session.database_metadata:
            raise KeyError(f"Could not find the {table_name} table!")

        column_var_type_map = session.database_metadata[table_name]
        required_columns = [timestamp_column]
        if entity_identifiers:
            required_columns.extend(entity_identifiers)

        for column in required_columns:
            if column not in column_var_type_map:
                raise KeyError(f'Could not find the "{column}" column from the table {table_name}!')

        node = QueryGraph().add_operation(
            node_type=NodeType.INPUT,
            node_params={
                "columns": sorted(column_var_type_map.keys()),
                "timestamp": timestamp_column,
                "entity_identifiers": entity_identifiers,
                "dbtable": table_name,
            },
            node_output_type=NodeOutputType.FRAME,
            input_nodes=[],
        )
        return EventSource(
            node=node,
            column_var_type_map=column_var_type_map,
            column_lineage_map={col: (node.name,) for col in column_var_type_map},
            row_index_lineage=(node.name,),
        )

    @classmethod
    def _construct_object(
        cls,
        node: Node,
        column_var_type_map: dict[str, DBVarType],
        column_lineage_map: dict[str, tuple[str, ...]],
        row_index_lineage: tuple[str, ...],
    ) -> Frame:
        return EventSource(
            node=node,
            column_var_type_map=column_var_type_map,
            column_lineage_map=column_lineage_map,
            row_index_lineage=row_index_lineage,
        )

    def __getitem__(self, item: str | list[str] | Series) -> Series | Frame:
        """
        Extract columns or perform row filtering on the event source

        Parameters
        ----------
        item: str | list[str] | Series
            input item used to perform column(s) projection or row filtering

        Returns
        -------
        Series | EventSource
            output of the operation

        Raises
        ------
        KeyError
            When the selected column does not exist
        TypeError
            When the item type does not support
        """
        self._check_any_missing_column(item)

        if self._is_list_of_str(item):
            item = sorted(set(self.protected_columns).union(item))
        return super().__getitem__(item)

    def __setitem__(self, key: str, value: int | float | str | bool | Series) -> None:
        """
        Assign a scalar value or Series object of the same `var_type` to the `EventSource` object

        Note that column of the `protected_columns` is not allowed to override

        Parameters
        ----------
        key: str
            column name to store the item
        value: int | float | str | bool | Series
            value to be assigned to the column

        Raises
        ------
        ValueError
            when the row indices between the Frame object & value object are not aligned
        TypeError
            when the key type & value type combination is not supported
        """
        if key in self.protected_columns:
            raise ValueError("Not allow to override timestamp column or entity identifiers!")
        super().__setitem__(key, value)
