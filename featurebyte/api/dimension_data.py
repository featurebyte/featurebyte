"""
DimensionData class
"""
from __future__ import annotations

from typing import ClassVar, Literal, Optional, Type

from bson.objectid import ObjectId
from pydantic import Field, StrictStr, root_validator
from typeguard import typechecked

from featurebyte.api.base_data import DataApiObject
from featurebyte.api.database_table import DatabaseTable
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import DBVarType, TableDataType
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.feature_store import FrozenDataModel
from featurebyte.models.validator import construct_data_model_root_validator
from featurebyte.query_graph.model.table import AllTableDataT, DimensionTableData
from featurebyte.schema.dimension_data import DimensionDataCreate, DimensionDataUpdate


class DimensionData(FrozenDataModel, DataApiObject):
    """
    DimensionData class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Data"], proxy_class="featurebyte.DimensionData")

    # class variables
    _route = "/dimension_data"
    _update_schema_class = DimensionDataUpdate
    _create_schema_class = DimensionDataCreate
    _table_data_class: ClassVar[Type[AllTableDataT]] = DimensionTableData

    # pydantic instance variable
    type: Literal[TableDataType.DIMENSION_DATA] = Field(TableDataType.DIMENSION_DATA, const=True)

    # pydantic instance variable (internal use)
    int_dimension_id_column: StrictStr = Field(alias="dimension_id_column")

    # pydantic validators
    _root_validator = root_validator(allow_reuse=True)(
        construct_data_model_root_validator(
            columns_info_key="int_columns_info",
            expected_column_field_name_type_pairs=[
                ("int_record_creation_date_column", DBVarType.supported_timestamp_types()),
                ("int_dimension_id_column", DBVarType.supported_id_types()),
            ],
        )
    )

    @property
    def dimension_id_column(self) -> str:
        """
        Dimension ID column name of the DimensionData

        Returns
        -------
        str
        """
        try:
            return self.cached_model.dimension_id_column
        except RecordRetrievalException:
            return self.int_dimension_id_column

    @classmethod
    @typechecked
    def from_tabular_source(
        cls,
        tabular_source: DatabaseTable,
        name: str,
        dimension_id_column: str,
        record_creation_date_column: Optional[str] = None,
        _id: Optional[ObjectId] = None,
    ) -> DimensionData:
        """
        Create DimensionData object from tabular source

        Parameters
        ----------
        tabular_source: DatabaseTable
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
        DimensionData
        """
        return super().create(
            tabular_source=tabular_source,
            name=name,
            record_creation_date_column=record_creation_date_column,
            _id=_id,
            dimension_id_column=dimension_id_column,
        )
