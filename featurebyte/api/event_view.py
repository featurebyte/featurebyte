"""
EventView class
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, List, Optional, TypeVar, Union

from pydantic import Field, PrivateAttr
from typeguard import typechecked

from featurebyte.api.event_data import EventData
from featurebyte.core.frame import Frame
from featurebyte.core.generic import ProtectedColumnsQueryObject
from featurebyte.core.series import Series
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.event_data import FeatureJobSetting
from featurebyte.query_graph.enum import NodeOutputType, NodeType

if TYPE_CHECKING:
    from featurebyte.api.groupby import EventViewGroupBy
else:
    EventViewGroupBy = TypeVar("EventViewGroupBy")


class EventViewColumn(Series):
    """
    EventViewColumn class
    """

    _parent: Optional[EventView] = PrivateAttr(default=None)
    event_data_id: PydanticObjectId = Field(allow_mutation=False)

    def binary_op_series_params(self, other: Series | None = None) -> dict[str, Any]:
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

    @typechecked
    def lag(self, entity_columns: Union[str, List[str]], offset: int = 1) -> EventViewColumn:
        """
        Lag operation

        Parameters
        ----------
        entity_columns : str | list[str]
            Entity columns used when retrieving the lag value
        offset : int
            The number of rows backward from which to retrieve a value. Default is 1.

        Returns
        -------
        EventViewColumn

        Raises
        ------
        ValueError
            If a lag operation has already been applied to the column
        """
        if not isinstance(entity_columns, list):
            entity_columns = [entity_columns]
        if NodeType.LAG in self.node_types_lineage:
            raise ValueError("lag can only be applied once per column")
        assert self._parent is not None

        timestamp_column = self._parent.timestamp_column
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
            row_index_lineage=self.row_index_lineage,
            **self.unary_op_series_params(),
        )


class EventView(ProtectedColumnsQueryObject, Frame):
    """
    EventView class
    """

    _series_class = EventViewColumn

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
        return [col.name for col in self.columns_info if col.entity_id]

    @property
    def timestamp_column(self) -> str:
        """
        Timestamp column of the event data

        Returns
        -------
        str
        """
        timestamp_col = str(self.inception_node.parameters.timestamp)  # type: ignore
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
    @typechecked
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
            columns_info=event_data.columns_info,
            node_name=event_data.node_name,
            column_lineage_map={
                col.name: (event_data.node.name,) for col in event_data.columns_info
            },
            row_index_lineage=tuple(event_data.row_index_lineage),
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

    @typechecked
    def __getitem__(self, item: Union[str, List[str], Series]) -> Union[Series, Frame]:
        if isinstance(item, list) and all(isinstance(elem, str) for elem in item):
            item = sorted(self.inherited_columns.union(item))
        output = super().__getitem__(item)
        return output

    @typechecked
    def __setitem__(self, key: str, value: Union[int, float, str, bool, Series]) -> None:
        if key in self.protected_columns:
            raise ValueError(f"Timestamp or entity column '{key}' cannot be modified!")
        super().__setitem__(key, value)

    @typechecked
    def groupby(
        self, by_keys: Union[str, List[str]], category: Optional[str] = None
    ) -> EventViewGroupBy:
        """
        Group EventView using a column or list of columns of the EventView object

        Parameters
        ----------
        by_keys: Union[str, List[str]]
            Define the key (entity) to for the `groupby` operation
        category : Optional[str]
            Optional category parameter to enable aggregation per category. It should be a column
            name in the EventView.

        Returns
        -------
        EventViewGroupBy
            a groupby object that contains information about the groups
        """
        # pylint: disable=import-outside-toplevel
        from featurebyte.api.groupby import EventViewGroupBy

        return EventViewGroupBy(obj=self, keys=by_keys, category=category)  # type: ignore
