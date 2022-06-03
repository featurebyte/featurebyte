"""
EventSource class
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from featurebyte.core.frame import Frame
from featurebyte.core.generic import ProtectedColumnsQueryObject
from featurebyte.core.series import Series
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.session.base import BaseSession, TableName

if TYPE_CHECKING:
    from featurebyte.core.groupby import EventSourceGroupBy


class EventSource(ProtectedColumnsQueryObject, Frame):
    """
    EventSource class
    """

    def __repr__(self) -> str:
        return f"EventSource(node.name={self.node.name})"

    @property
    def protected_attributes(self) -> list[str]:
        """
        List of protected attributes used to extract protected_columns

        Returns
        -------
        list[str]
        """
        return ["timestamp_column", "entity_identifiers"]

    @property
    def timestamp_column(self) -> str | None:
        """
        Timestamp column of the event source

        Returns
        -------
        str | None
        """
        return self.inception_node.parameters.get("timestamp")

    @property
    def entity_identifiers(self) -> list[str] | None:
        """
        Entity id columns of the event source

        Returns
        -------
        list[str] | None
        """
        return self.inception_node.parameters.get("entity_identifiers")

    @classmethod
    def from_session(
        cls,
        session: BaseSession,
        table_name: TableName,
        timestamp_column: str,
        entity_identifiers: list[str] | None = None,
    ) -> EventSource:
        """
        Construct an EventSource object using session object

        Parameters
        ----------
        session: BaseSession
            database session object to retrieve database metadata
        table_name: TableName
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
            session=session,
        )

    def __getitem__(self, item: str | list[str] | Series) -> Series | Frame:
        # pylint: disable=R0801 (duplicate-code)
        if isinstance(item, list) and all(isinstance(elem, str) for elem in item):
            item = sorted(self.protected_columns.union(item))
        return super().__getitem__(item)

    def __setitem__(self, key: str, value: int | float | str | bool | Series) -> None:
        if key in self.protected_columns:
            raise ValueError(
                f"Not allow to override timestamp or entity identifier column '{key}'!"
            )
        super().__setitem__(key, value)

    def groupby(self, by_keys: str | list[str]) -> EventSourceGroupBy:
        """
        Group EventSource using a column or list of columns of the EventSource object

        Parameters
        ----------
        by_keys: str | list[str]
            used to define the groups for the `groupby` operation

        Returns
        -------
        EventSourceGroupBy
            a groupby object that contains information about the groups
        """
        # pylint: disable=C0415
        from featurebyte.core.groupby import EventSourceGroupBy

        return EventSourceGroupBy(obj=self, keys=by_keys)
