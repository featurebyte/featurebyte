"""
EventView class
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Optional

from beanie import PydanticObjectId
from pydantic import Field, PrivateAttr, StrictStr

from featurebyte.api.event_data import EventData
from featurebyte.api.util import get_entity
from featurebyte.core.frame import Frame
from featurebyte.core.generic import ProtectedColumnsQueryObject
from featurebyte.core.series import Series
from featurebyte.models.event_data import FeatureJobSetting

if TYPE_CHECKING:
    from featurebyte.api.groupby import EventViewGroupBy


class EventViewColumn(Series):
    """
    EventViewColumn class
    """

    _parent: Optional[EventView] = PrivateAttr(default=None)
    event_data_id: PydanticObjectId = Field(allow_mutation=False)

    def _binary_op_series_params(self, other: Series | None = None) -> dict[str, Any]:
        """
        Parameters that will be passed to series-like constructor in _binary_op method


        Parameters
        ----------
        other: Series
            Other Series object

        Returns
        -------
        dict[str, Any]
        """
        _ = other
        return {"event_data_id": self.event_data_id}

    def unary_op_series_params(self) -> dict[str, Any]:
        return {"event_data_id": self.event_data_id}

    def _validate_series_to_set_parent_attribute(self) -> None:
        """
        Check whether the current series has right to set parent frame

        Raises
        ------
        ValueError
            When the name or parent frame is missing
        """
        if self.name is None:
            raise ValueError("Series object does not have name!")
        if self.parent is None:
            raise ValueError("Series object does not have parent frame object!")

    def as_entity(self, entity_name: str | None) -> None:
        """
        Set the column as the specified entity

        Parameters
        ----------
        entity_name: str | None
            Associate column name to the entity, remove association if entity name is None

        Raises
        ------
        TypeError
            When the tag name has non-string type
        """
        self._validate_series_to_set_parent_attribute()
        if self.name and self.parent:
            if entity_name is None:
                column_entity_map = self.parent.column_entity_map or {}
                column_entity_map.pop(self.name)
                self.parent.column_entity_map = column_entity_map
            elif isinstance(entity_name, str):
                entity_dict = get_entity(entity_name)
                column_entity_map = self.parent.column_entity_map or {}
                column_entity_map[self.name] = entity_dict["_id"]
                self.parent.column_entity_map = column_entity_map
            else:
                raise TypeError(
                    f'Unsupported type "{type(entity_name)}" for tag name "{entity_name}"!'
                )

    def add_description(self, description: str) -> None:
        """
        Add description to the column at parent frame

        Parameters
        ----------
        description: str
            Description for current series

        Raises
        ------
        TypeError
            When the description has non-string type
        """
        self._validate_series_to_set_parent_attribute()
        if self.name and self.parent:
            if isinstance(description, str):
                self.parent.column_description_map[self.name] = str(description)
            else:
                raise TypeError(
                    f'Unsupported type "{type(description)}" for description "{description}"!'
                )


class EventView(ProtectedColumnsQueryObject, Frame):
    """
    EventView class
    """

    _series_class = EventViewColumn

    column_entity_map: Optional[Dict[StrictStr, PydanticObjectId]] = Field(default=None)
    column_description_map: Dict[StrictStr, StrictStr] = Field(default_factory=dict)
    default_feature_job_setting: Optional[FeatureJobSetting] = Field(allow_mutation=False)
    event_data_id: PydanticObjectId = Field(allow_mutation=False)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(node.name={self.node.name}, timestamp_column={self.timestamp_column})"

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
        return ["timestamp_column", "entity_columns"]

    @property
    def entity_columns(self) -> list[str]:
        """
        List of entity columns

        Returns
        -------
        list[str]
        """
        column_entity_map = self.column_entity_map or {}
        return list(column_entity_map.keys())

    @property
    def timestamp_column(self) -> str:
        """
        Timestamp column of the event data

        Returns
        -------
        str
        """
        timestamp_col: str = self.inception_node.parameters["timestamp"]
        return timestamp_col

    @property
    def inherited_columns(self) -> set[str]:
        """
        Special columns set which will be automatically added to the object of same class
        derived from current object

        Returns
        -------
        set[str]
        """
        return {self.timestamp_column}

    @classmethod
    def from_event_data(cls, event_data: EventData) -> EventView:
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
        """
        return EventView(
            feature_store=event_data.feature_store,
            tabular_source=event_data.tabular_source,
            node=event_data.node,
            column_var_type_map=event_data.column_var_type_map.copy(),
            column_lineage_map={
                col: (event_data.node.name,) for col in event_data.column_var_type_map
            },
            row_index_lineage=tuple(event_data.row_index_lineage),
            column_entity_map=event_data.column_entity_map,
            default_feature_job_setting=event_data.default_feature_job_setting,
            event_data_id=event_data.id,
        )

    @property
    def _getitem_frame_params(self) -> dict[str, Any]:
        """
        Parameters that will be passed to frame-like class constructor in __getitem__ method

        Returns
        -------
        dict[str, Any]
        """
        return {
            "default_feature_job_setting": self.default_feature_job_setting,
            "event_data_id": self.event_data_id,
        }

    @property
    def _getitem_series_params(self) -> dict[str, Any]:
        """
        Parameters that will be passed to series-like class constructor in __getitem__ method

        Returns
        -------
        dict[str, Any]
        """
        return {"event_data_id": self.event_data_id}

    def __getitem__(self, item: str | list[str] | Series) -> Series | Frame:
        if isinstance(item, list) and all(isinstance(elem, str) for elem in item):
            item = sorted(self.inherited_columns.union(item))
        output = super().__getitem__(item)
        if isinstance(output, EventView):
            if self.column_entity_map:
                output.column_entity_map = {
                    col: name
                    for col, name in self.column_entity_map.items()
                    if col in output.columns
                }
        return output

    def __setitem__(self, key: str, value: int | float | str | bool | Series) -> None:
        if key in self.protected_columns:
            raise ValueError(f"Timestamp or entity column '{key}' cannot be modified!")
        super().__setitem__(key, value)

    def groupby(self, by_keys: str | list[str], category: str | None = None) -> EventViewGroupBy:
        """
        Group EventView using a column or list of columns of the EventView object

        Parameters
        ----------
        by_keys: str | list[str]
            Define the key (entity) to for the `groupby` operation
        category : str | None
            Optional category parameter to enable aggregation per category. It should be a column
            name in the EventView.

        Returns
        -------
        EventViewGroupBy
            a groupby object that contains information about the groups
        """
        # pylint: disable=import-outside-toplevel
        from featurebyte.api.groupby import EventViewGroupBy

        return EventViewGroupBy(obj=self, keys=by_keys, category=category)
