"""
DimensionView class
"""

from __future__ import annotations

from typing import Any, ClassVar, Optional

from pydantic import Field

from featurebyte.api.scd_view import SCDView
from featurebyte.api.view import RawMixin, View, ViewColumn
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.exception import JoinViewMismatchError
from featurebyte.logging import get_logger
from featurebyte.query_graph.enum import GraphNodeType

logger = get_logger(__name__)


class DimensionViewColumn(ViewColumn):
    """
    DimensionViewColumn class
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc()


class DimensionView(View, RawMixin):
    """
    A DimensionView object is a modified version of the DimensionTable object that provides additional capabilities
    for transforming data. With a DimensionView, you can create and transform columns and filter records prior to
    feature declaration.

    Dimension views are commonly used to create Lookup features, and they can also be used to enrich views of other
    tables through joins.

    See Also
    --------
    - [dimension_table#get_view](/reference/featurebyte.api.dimension_table.DimensionTable.get_view/): get dimension view from a `DimensionTable`
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(
        proxy_class="featurebyte.DimensionView",
        skip_params_and_signature_in_class_docs=True,
    )
    _series_class: ClassVar[Any] = DimensionViewColumn
    _view_graph_node_type: ClassVar[GraphNodeType] = GraphNodeType.DIMENSION_VIEW

    # pydantic instance variables
    dimension_id_column: str = Field(
        frozen=True,
        description="Returns the name of the column representing the primary key of the Dimension view.",
    )

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
            raised when the other view is a slowly changing dimension view
        """
        if isinstance(other_view, SCDView):
            logger.error("columns from a SCDView can’t be added to a DimensionView")
            raise JoinViewMismatchError

    def _get_join_column(self) -> Optional[str]:
        return self.dimension_id_column
