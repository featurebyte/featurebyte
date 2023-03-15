"""
DimensionView class
"""
from __future__ import annotations

from typing import Any, ClassVar

from pydantic import Field

from featurebyte.api.scd_view import SlowlyChangingView
from featurebyte.api.view import View, ViewColumn
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.exception import JoinViewMismatchError
from featurebyte.logger import logger
from featurebyte.query_graph.enum import GraphNodeType


class DimensionViewColumn(ViewColumn):
    """
    DimensionViewColumn class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Column"])


class DimensionView(View):
    """
    Dimension Views allow users to transform Dimension Data.

    Transformations supported are the same as for EventView or ItemView except for lag that is not supported.
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(
        section=["View"],
        proxy_class="featurebyte.DimensionView",
    )

    # class variables
    _series_class = DimensionViewColumn
    _view_graph_node_type: ClassVar[GraphNodeType] = GraphNodeType.DIMENSION_VIEW

    # pydantic instance variables
    dimension_id_column: str = Field(allow_mutation=False)

    @property
    def protected_attributes(self) -> list[str]:
        """
        List of protected attributes used to extract protected_columns

        Returns
        -------
        list[str]
        """
        return super().protected_attributes + ["dimension_id_column"]

    @property
    def _getitem_frame_params(self) -> dict[str, Any]:
        """
        Parameters that will be passed to frame-like class constructor in __getitem__ method

        Returns
        -------
        dict[str, Any]
        """
        params = super()._getitem_frame_params
        params.update({"dimension_id_column": self.dimension_id_column})
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
            raised when the other view is a slowly changing view
        """
        if isinstance(other_view, SlowlyChangingView):
            logger.error("columns from a SlowlyChangingView can’t be added to a DimensionView")
            raise JoinViewMismatchError

    def get_join_column(self) -> str:
        return self.dimension_id_column
