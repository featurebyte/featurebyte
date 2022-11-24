"""
View class
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, List, Optional, Type, TypeVar, Union

from abc import ABC

from pydantic import Field, PrivateAttr
from typeguard import typechecked

from featurebyte.api.data import DataApiObject
from featurebyte.core.frame import Frame
from featurebyte.core.generic import ProtectedColumnsQueryObject
from featurebyte.core.series import Series
from featurebyte.models.base import PydanticObjectId

if TYPE_CHECKING:
    from featurebyte.api.groupby import GroupBy
else:
    GroupBy = TypeVar("GroupBy")

ViewT = TypeVar("ViewT", bound="View")


class ViewColumn(Series):
    """
    ViewColumn class that is the base class of columns returned from any View (e.g. EventView)
    """

    _parent: Optional[View] = PrivateAttr(default=None)
    tabular_data_ids: List[PydanticObjectId] = Field(allow_mutation=False)

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
        return {"tabular_data_ids": self.tabular_data_ids}

    def unary_op_series_params(self) -> dict[str, Any]:
        return {"tabular_data_ids": self.tabular_data_ids}


class GroupByMixin:  # pylint: disable=too-few-public-methods
    """
    Mixin that provides groupby functionality to a View object
    """

    @typechecked
    def groupby(self, by_keys: Union[str, List[str]], category: Optional[str] = None) -> GroupBy:
        """
        Group View using a column or list of columns of the View object
        Refer to [GroupBy](/reference/featurebyte.api.groupby.GroupBy/)

        Parameters
        ----------
        by_keys: Union[str, List[str]]
            Define the key (entity) to for the `groupby` operation
        category : Optional[str]
            Optional category parameter to enable aggregation per category. It should be a column
            name in the View.

        Returns
        -------
        GroupBy
            a groupby object that contains information about the groups
        """
        # pylint: disable=import-outside-toplevel
        from featurebyte.api.groupby import GroupBy

        return GroupBy(obj=self, keys=by_keys, category=category)  # type: ignore

    def validate_aggregation_parameters(
        self, groupby_obj: GroupBy, value_column: Optional[str]
    ) -> None:
        """
        Perform View specific validation on the parameters provided for groupby and aggregate
        functions

        Parameters
        ----------
        groupby_obj: GroupBy
            GroupBy object
        value_column: Optional[str]
            Column to be aggregated
        """


class View(ProtectedColumnsQueryObject, Frame, ABC):
    """
    View class that is the base class of any View (e.g. EventView)
    """

    tabular_data_ids: List[PydanticObjectId] = Field(allow_mutation=False)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(node.name={self.node.name})"

    def __str__(self) -> str:
        return repr(self)

    @classmethod
    @typechecked
    def from_data(cls: Type[ViewT], data: DataApiObject, **kwargs: Any) -> ViewT:
        """
        Construct a View object

        Parameters
        ----------
        data: DataApiObject
            EventData object used to construct a View object
        kwargs: dict
            Additional parameters to be passed to the View constructor

        Returns
        -------
        ViewT
            constructed View object
        """
        return cls(
            feature_store=data.feature_store,
            tabular_source=data.tabular_source,
            columns_info=data.columns_info,
            node_name=data.node_name,
            column_lineage_map={col.name: (data.node.name,) for col in data.columns_info},
            row_index_lineage=tuple(data.row_index_lineage),
            tabular_data_ids=[data.id],
            **kwargs,
        )

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
    def protected_attributes(self) -> list[str]:
        """
        List of protected attributes used to extract protected_columns

        Returns
        -------
        list[str]
        """
        return ["entity_columns"]

    @property
    def _getitem_frame_params(self) -> dict[str, Any]:
        """
        Parameters that will be passed to frame-like class constructor in __getitem__ method

        Returns
        -------
        dict[str, Any]
        """
        return {"tabular_data_ids": self.tabular_data_ids}

    @property
    def _getitem_series_params(self) -> dict[str, Any]:
        """
        Parameters that will be passed to series-like class constructor in __getitem__ method

        Returns
        -------
        dict[str, Any]
        """
        return {"tabular_data_ids": self.tabular_data_ids}

    @typechecked
    def __getitem__(self, item: Union[str, List[str], Series]) -> Union[Series, Frame]:
        if isinstance(item, list) and all(isinstance(elem, str) for elem in item):
            item = sorted(self.inherited_columns.union(item))
        output = super().__getitem__(item)
        return output

    @typechecked
    def __setitem__(self, key: str, value: Union[int, float, str, bool, Series]) -> None:
        if key in self.protected_columns:
            raise ValueError(f"Column '{key}' cannot be modified!")
        super().__setitem__(key, value)
