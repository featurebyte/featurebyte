"""
DimensionTable class
"""
from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, List, Literal, Optional, Type, cast

from bson.objectid import ObjectId
from pydantic import Field, StrictStr, root_validator
from typeguard import typechecked

from featurebyte.api.base_table import TableApiObject
from featurebyte.api.source_table import SourceTable
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.common.validator import construct_data_model_root_validator
from featurebyte.enum import DBVarType, TableDataType, ViewMode
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.dimension_data import DimensionDataModel
from featurebyte.query_graph.graph import GlobalQueryGraph
from featurebyte.query_graph.model.table import AllTableDataT, DimensionTableData
from featurebyte.query_graph.node.cleaning_operation import ColumnCleaningOperation
from featurebyte.query_graph.node.input import InputNode
from featurebyte.query_graph.node.nested import ViewMetadata
from featurebyte.schema.dimension_data import DimensionDataCreate, DimensionDataUpdate

if TYPE_CHECKING:
    from featurebyte.api.dimension_view import DimensionView


class DimensionTable(TableApiObject):
    """
    Dimension Table is a data source object connected with a Dimension table in the data warehouse that has static data.

    To build features, users create Dimension Views from Dimension Table
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Table"], proxy_class="featurebyte.DimensionTable")

    # class variables
    _route = "/dimension_data"
    _update_schema_class = DimensionDataUpdate
    _create_schema_class = DimensionDataCreate
    _get_schema = DimensionDataModel
    _table_data_class: ClassVar[Type[AllTableDataT]] = DimensionTableData

    # pydantic instance variable (public)
    type: Literal[TableDataType.DIMENSION_DATA] = Field(TableDataType.DIMENSION_DATA, const=True)

    # pydantic instance variable (internal use)
    internal_dimension_id_column: StrictStr = Field(alias="dimension_id_column")

    # pydantic validators
    _root_validator = root_validator(allow_reuse=True)(
        construct_data_model_root_validator(
            columns_info_key="internal_columns_info",
            expected_column_field_name_type_pairs=[
                ("internal_record_creation_date_column", DBVarType.supported_timestamp_types()),
                ("internal_dimension_id_column", DBVarType.supported_id_types()),
            ],
        )
    )

    def get_view(
        self,
        view_mode: Literal[ViewMode.AUTO, ViewMode.MANUAL] = ViewMode.AUTO,
        drop_column_names: Optional[List[str]] = None,
        column_cleaning_operations: Optional[List[ColumnCleaningOperation]] = None,
    ) -> DimensionView:
        """
        Construct an DimensionView object

        Parameters
        ----------
        view_mode: Literal[ViewMode.AUTO, ViewMode.MANUAL]
            View mode to use (manual or auto), when auto, the view will be constructed with cleaning operations
            from the data and the record creation date column will be dropped
        drop_column_names: Optional[List[str]]
            List of column names to drop (manual mode only)
        column_cleaning_operations: Optional[List[ColumnCleaningOperation]]
            Column cleaning operations to apply (manual mode only)

        Returns
        -------
        DimensionView
            constructed DimensionView object
        """
        # pylint: disable=import-outside-toplevel
        from featurebyte.api.dimension_view import DimensionView

        self._validate_view_mode_params(
            view_mode=view_mode,
            drop_column_names=drop_column_names,
            column_cleaning_operations=column_cleaning_operations,
        )

        # The input of view graph node is the data node. The final graph looks like this:
        #    +-----------+     +--------------------------------+
        #    | InputNode + --> | GraphNode(type:dimension_view) +
        #    +-----------+     +--------------------------------+
        drop_column_names = drop_column_names or []
        if view_mode == ViewMode.AUTO and self.record_creation_date_column:
            drop_column_names.append(self.record_creation_date_column)

        data_node = self.frame.node
        assert isinstance(data_node, InputNode)
        dimension_table_data = cast(DimensionTableData, self.table_data)
        column_cleaning_operations = column_cleaning_operations or []
        (
            dimension_table_data,
            column_cleaning_operations,
        ) = self._prepare_table_data_and_column_cleaning_operations(
            table_data=dimension_table_data,
            column_cleaning_operations=column_cleaning_operations,
            view_mode=view_mode,
        )

        view_graph_node, columns_info = dimension_table_data.construct_dimension_view_graph_node(
            dimension_data_node=data_node,
            drop_column_names=drop_column_names,
            metadata=ViewMetadata(
                view_mode=view_mode,
                drop_column_names=drop_column_names,
                column_cleaning_operations=column_cleaning_operations,
                data_id=data_node.parameters.id,
            ),
        )
        inserted_graph_node = GlobalQueryGraph().add_node(view_graph_node, input_nodes=[data_node])
        return DimensionView(
            feature_store=self.feature_store,
            tabular_source=self.tabular_source,
            columns_info=columns_info,
            node_name=inserted_graph_node.name,
            tabular_data_ids=[self.id],
            dimension_id_column=self.dimension_id_column,
        )

    @property
    def dimension_id_column(self) -> str:
        """
        Dimension ID column name of the DimensionTable

        Returns
        -------
        str
        """
        try:
            return self.cached_model.dimension_id_column
        except RecordRetrievalException:
            return self.internal_dimension_id_column

    @classmethod
    @typechecked
    def from_tabular_source(
        cls,
        tabular_source: SourceTable,
        name: str,
        dimension_id_column: str,
        record_creation_date_column: Optional[str] = None,
        _id: Optional[ObjectId] = None,
    ) -> DimensionTable:
        """
        Create DimensionTable object from tabular source.

        Parameters
        ----------
        tabular_source: SourceTable
            DatabaseTable object constructed from FeatureStore
        name: str
            Dimension data name
        dimension_id_column: str
            Dimension data ID column from the given tabular source
        record_creation_date_column: str
            Record creation datetime column from the given tabular source
        _id: Optional[ObjectId]
            Identity value for constructed object

        Returns
        -------
        DimensionTable

        Examples
        --------
        Create DimensionTable from a table in the feature store

        >>> us_postal_codes = DimensionTable.from_tabular_source(  # doctest: +SKIP
        ...    name="US Postal Codes",
        ...    tabular_source=feature_store.get_table(
        ...      database_name="DEMO",
        ...      schema_name="US",
        ...      table_name="POSTAL_CODES"
        ...    ),
        ...    dimension_id_column="POSTAL_CODE_ID",
        ...    record_creation_date_column="RECORD_AVAILABLE_AT",
        ... )

        Get information about the DimensionTable

        >>> us_postal_codes.info(verbose=True)  # doctest: +SKIP
        """
        return super().create(
            tabular_source=tabular_source,
            name=name,
            record_creation_date_column=record_creation_date_column,
            _id=_id,
            dimension_id_column=dimension_id_column,
        )
