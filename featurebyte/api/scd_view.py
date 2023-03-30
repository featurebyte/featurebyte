"""
SCDView class
"""
from __future__ import annotations

from typing import Any, ClassVar, Optional

from pydantic import Field

from featurebyte.api.view import GroupByMixin, View, ViewColumn
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.exception import JoinViewMismatchError
from featurebyte.logger import logger
from featurebyte.query_graph.enum import GraphNodeType
from featurebyte.query_graph.node.generic import SCDBaseParameters


class SCDViewColumn(ViewColumn):
    """
    SCDViewColumn class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Column"])


class SCDView(View, GroupByMixin):
    """
    A `SCDView` allows a user to transform a `SCDTable`.

    Transformations supported are the same as for `EventView` or `ItemView` except for lag that is not supported.

    See Also
    --------
    - [scd_table#get_view](/reference/featurebyte.api.scd_table.SCDTable.get_view/): get SCD view from a `SCDTable`
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(
        section=["View"],
        proxy_class="featurebyte.SCDView",
    )

    # class variables
    _series_class = SCDViewColumn
    _view_graph_node_type: ClassVar[GraphNodeType] = GraphNodeType.SCD_VIEW

    # pydantic instance variables
    natural_key_column: str = Field(allow_mutation=False)
    effective_timestamp_column: str = Field(allow_mutation=False)
    surrogate_key_column: Optional[str] = Field(allow_mutation=False)
    end_timestamp_column: Optional[str] = Field(allow_mutation=False)
    current_flag_column: Optional[str] = Field(allow_mutation=False)

    @property
    def timestamp_column(self) -> Optional[str]:
        return self.effective_timestamp_column

    @property
    def protected_attributes(self) -> list[str]:
        """
        List of protected attributes used to extract protected_columns

        Returns
        -------
        list[str]
        """
        return super().protected_attributes + [
            "natural_key_column",
            "surrogate_key_column",
            "effective_timestamp_column",
            "end_timestamp_column",
            "current_flag_column",
        ]

    def _get_additional_inherited_columns(self) -> set[str]:
        columns = {self.effective_timestamp_column}
        return columns

    @property
    def _getitem_frame_params(self) -> dict[str, Any]:
        """
        Parameters that will be passed to frame-like class constructor in __getitem__ method

        Returns
        -------
        dict[str, Any]
        """
        params = super()._getitem_frame_params
        params.update(
            {
                "natural_key_column": self.natural_key_column,
                "surrogate_key_column": self.surrogate_key_column,
                "effective_timestamp_column": self.effective_timestamp_column,
                "end_timestamp_column": self.end_timestamp_column,
                "current_flag_column": self.current_flag_column,
            }
        )
        return params

    def validate_join(self, other_view: View) -> None:
        """
        Validate join should be implemented by view classes that have extra requirements.

        Parameters
        ---------
        other_view: View
            the other view that we are joining with

        Raises
        ------
        JoinViewMismatchError
            raised when the other view is a slowly changing dimension view
        """
        if isinstance(other_view, SCDView):
            logger.error("columns from a SCDView can’t be added to a SCDView")
            raise JoinViewMismatchError

    def get_join_column(self) -> str:
        return self.natural_key_column

    def get_common_scd_parameters(self) -> SCDBaseParameters:
        """
        Get parameters related to SCDTable

        Returns
        -------
        SCDBaseParameters
        """
        return SCDBaseParameters(
            effective_timestamp_column=self.effective_timestamp_column,
            natural_key_column=self.natural_key_column,
            current_flag_column=self.current_flag_column,
            end_timestamp_column=self.end_timestamp_column,
        )

    def _get_join_parameters(self, calling_view: View) -> dict[str, Any]:

        # When calling_view doesn't have the timestamp_column attribute, it means that it is a
        # DimensionView. It is invalid to join DimensionView with SCDView on the right
        # side. A validation error would have been raised before reaching here.
        assert hasattr(calling_view, "timestamp_column") and isinstance(
            calling_view.timestamp_column, str
        )

        left_timestamp_column = calling_view.timestamp_column
        return {
            "scd_parameters": {
                "left_timestamp_column": left_timestamp_column,
                **self.get_common_scd_parameters().dict(),
            }
        }

    def _get_additional_excluded_columns_as_other_view(self) -> list[str]:
        excluded_columns = [self.effective_timestamp_column]
        if self.current_flag_column:
            excluded_columns.append(self.current_flag_column)
        if self.surrogate_key_column:
            excluded_columns.append(self.surrogate_key_column)
        if self.end_timestamp_column:
            excluded_columns.append(self.end_timestamp_column)
        return excluded_columns

    def _get_as_feature_parameters(self, offset: Optional[str] = None) -> dict[str, Any]:
        return {
            "scd_parameters": {
                "offset": offset,
                **self.get_common_scd_parameters().dict(),
            }
        }
