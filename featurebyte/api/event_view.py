"""
EventView class
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from featurebyte.api.event_data import EventData
from featurebyte.core.frame import Frame
from featurebyte.core.generic import ProtectedColumnsQueryObject
from featurebyte.core.series import Series

if TYPE_CHECKING:
    from featurebyte.api.groupby import EventViewGroupBy


class EventView(ProtectedColumnsQueryObject, Frame):
    """
    EventView class
    """

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(node.name={self.node.name}, "
            f"timestamp_column={self.timestamp_column}, entity_identifiers={self.entity_identifiers})"
        )

    def __str__(self) -> str:
        return repr(self)

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
    def from_event_data(
        cls,
        event_data: EventData,
    ) -> EventView:
        """
        Construct an EventView object using session object

        Parameters
        ----------
        event_data: EventData
            EventData object used to construct EventView object

        Returns
        -------
        EventView
            constructed EventView object

        Raises
        ------
        KeyError
            if the table name does not exist in the session's database metadata
        """
        return EventView(
            node=event_data.node,
            column_var_type_map=event_data.column_var_type_map.copy(),
            column_lineage_map={
                col: (event_data.node.name,) for col in event_data.column_var_type_map
            },
            row_index_lineage=tuple(event_data.row_index_lineage),
            session=event_data.session,
        )

    def __getitem__(self, item: str | list[str] | Series) -> Series | Frame:
        # pylint: disable=R0801 (duplicate-code)
        if isinstance(item, list) and all(isinstance(elem, str) for elem in item):
            item = sorted(self.protected_columns.union(item))
        return super().__getitem__(item)

    def __setitem__(self, key: str, value: int | float | str | bool | Series) -> None:
        if key in self.protected_columns:
            raise ValueError(f"Timestamp or entity identifier column '{key}' cannot be modified!")
        super().__setitem__(key, value)

    def groupby(self, by_keys: str | list[str]) -> EventViewGroupBy:
        """
        Group EventView using a column or list of columns of the EventView object

        Parameters
        ----------
        by_keys: str | list[str]
            used to define the groups for the `groupby` operation

        Returns
        -------
        EventViewGroupBy
            a groupby object that contains information about the groups
        """
        # pylint: disable=C0415
        from featurebyte.api.groupby import EventViewGroupBy

        return EventViewGroupBy(obj=self, keys=by_keys)
