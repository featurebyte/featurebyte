"""
RequestColumn related classes for on-demand features
"""

from __future__ import annotations

from typing import ClassVar, Optional

from pydantic import Field

from featurebyte.api.feature_store import FeatureStore
from featurebyte.common import get_active_catalog_id
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.core.series import Series
from featurebyte.enum import DBVarType, SpecialColumnName
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.node.schema import DummyTableDetails


class RequestColumn(Series):
    """
    RequestColumn class
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.RequestColumn")

    # instance variables
    tabular_source: TabularSource = Field(frozen=True)
    feature_store: FeatureStoreModel = Field(exclude=True, frozen=True)

    @classmethod
    def _create_request_column(
        cls,
        column_name: str,
        column_dtype: DBVarType,
        context_id: Optional[str] = None,
    ) -> RequestColumn:
        """
        Internal method to create a RequestColumn for any column name and dtype.

        This is not exposed publicly - users should use Context.get_user_provided_feature()
        to access user-provided columns as Feature objects.

        Parameters
        ----------
        column_name: str
            Column name in the request data.
        column_dtype: DBVarType
            Variable type of the column.
        context_id: Optional[str]
            Context ID for user-provided columns. Used in SDK code generation
            to produce Context.get_by_id(...).get_user_provided_feature(...) calls.

        Returns
        -------
        RequestColumn
        """
        node_params = {"column_name": column_name, "dtype": column_dtype}
        if context_id is not None:
            node_params["context_id"] = context_id
        node = GlobalQueryGraph().add_operation(
            node_type=NodeType.REQUEST_COLUMN,
            node_params=node_params,
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[],
        )

        # Import here to avoid circular import
        from featurebyte.api.catalog import Catalog  # pylint: disable=import-outside-toplevel

        # use feature store from active catalog
        catalog_id = get_active_catalog_id()
        assert catalog_id
        catalog = Catalog.get_by_id(catalog_id)
        feature_store = FeatureStore.get_by_id(catalog.default_feature_store_ids[0])

        return cls(
            feature_store=feature_store,
            tabular_source=TabularSource(
                feature_store_id=feature_store.id, table_details=DummyTableDetails()
            ),
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
        return RequestColumn._create_request_column(
            SpecialColumnName.POINT_IN_TIME.value, DBVarType.TIMESTAMP
        )

    @property
    def binary_op_output_class_priority(self) -> int:
        return 1
