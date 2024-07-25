"""
RequestColumn related classes for on-demand features
"""

from __future__ import annotations

from typing import ClassVar, Optional

from pydantic import Field

from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.core.series import Series
from featurebyte.enum import DBVarType, SpecialColumnName
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.model.common_table import TabularSource


class RequestColumn(Series):
    """
    RequestColumn class
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.RequestColumn")

    # instance variables
    tabular_source: Optional[TabularSource] = Field(  # type: ignore[assignment]
        frozen=True, default=None
    )
    feature_store: Optional[FeatureStoreModel] = Field(  # type: ignore[assignment]
        exclude=True, frozen=True, default=None
    )

    @classmethod
    def create_request_column(cls, column_name: str, column_dtype: DBVarType) -> RequestColumn:
        """
        Create a RequestColumn object.

        Parameters
        ----------
        column_name: str
            Column name in the request data.
        column_dtype: DBVarType
            Variable type of the column.

        Returns
        -------
        RequestColumn

        Raises
        ------
        NotImplementedError
            If the request column is not the POINT_IN_TIME column
        """
        if not (
            column_name == SpecialColumnName.POINT_IN_TIME and column_dtype == DBVarType.TIMESTAMP
        ):
            raise NotImplementedError(
                "Currently only POINT_IN_TIME column is supported. Please use"
                " RequestColumn.point_in_time() instead."
            )

        node = GlobalQueryGraph().add_operation(
            node_type=NodeType.REQUEST_COLUMN,
            node_params={"column_name": column_name, "dtype": column_dtype},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[],
        )
        return cls(
            feature_store=None,
            tabular_source=None,
            node_name=node.name,
            name=column_name,
            dtype=column_dtype,
        )

    @classmethod
    def point_in_time(cls) -> RequestColumn:
        """
        Get a RequestColumn that represents the POINT_IN_TIME column in the request data.

        Returns
        -------
        RequestColumn

        Examples
        --------
        Create a feature that retrieves the timestamp of the latest invoice of a Customer.

        >>> invoice_view = catalog.get_view("GROCERYINVOICE")
        >>> latest_invoice = invoice_view.groupby("GroceryCustomerGuid").aggregate_over(
        ...     value_column="Timestamp",
        ...     method="latest",
        ...     windows=[None],
        ...     feature_names=["Customer Latest Visit"],
        ... )
        >>> # Create feature that computes the time since the latest invoice
        >>> feature = (
        ...     fb.RequestColumn.point_in_time() - latest_invoice["Customer Latest Visit"]
        ... ).dt.hour
        >>> feature.name = "Customer number of hours since last visit"
        """
        return RequestColumn.create_request_column(
            SpecialColumnName.POINT_IN_TIME.value, DBVarType.TIMESTAMP
        )

    @property
    def binary_op_output_class_priority(self) -> int:
        return 1
