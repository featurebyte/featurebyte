"""
EventView class
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, List

from pydantic import Field, validator

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

    entity_identifiers: List[str] = Field(default_factory=list)

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(node.name={self.node.name}, "
            f"timestamp_column={self.timestamp_column}, entity_identifiers={self.entity_identifiers})"
        )

    def __str__(self) -> str:
        return repr(self)

    @validator("entity_identifiers")
    @classmethod
    def _check_entity_identifiers_exist(cls, value: list[str], values: dict[str, Any]) -> list[str]:
        for column in value:
            if column not in values["column_var_type_map"]:
                raise ValueError(f'Column "{column}" not found in the table!')
        return value

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

    @classmethod
    def from_event_data(
        cls,
        event_data: EventData,
        entity_identifiers: list[str] | None = None,
    ) -> EventView:
        """
        Construct an EventView object using session object

        Parameters
        ----------
        event_data: EventData
            EventData object used to construct EventView object
        entity_identifiers: list[str]
            List of columns to be used as entity identifiers

        Returns
        -------
        EventView
            constructed EventView object
        """
        return EventView(
            tabular_source=event_data.tabular_source,
            node=event_data.node,
            column_var_type_map=event_data.column_var_type_map.copy(),
            column_lineage_map={
                col: (event_data.node.name,) for col in event_data.column_var_type_map
            },
            row_index_lineage=tuple(event_data.row_index_lineage),
            entity_identifiers=entity_identifiers or [],
        )

    def __getitem__(self, item: str | list[str] | Series) -> Series | Frame:
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
