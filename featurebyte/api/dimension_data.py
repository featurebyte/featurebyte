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
from featurebyte.common.validator import construct_data_model_root_validator
from featurebyte.enum import DBVarType, TableDataType
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.dimension_data import DimensionDataModel
from featurebyte.query_graph.model.table import AllTableDataT, DimensionTableData
from featurebyte.schema.dimension_data import DimensionDataCreate, DimensionDataUpdate


class DimensionData(DataApiObject):
    """
    Dimension Data is a data source object connected with a Dimension table in the data warehouse that has static data.

    To build features, users create Dimension Views from Dimension Data
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Data"], proxy_class="featurebyte.DimensionData")

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
            return self.internal_dimension_id_column

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

        Examples
        --------
        Create DimensionData from a table in the feature store

        >>> us_postal_codes = DimensionData.from_tabular_source(  # doctest: +SKIP
        ...    name="US Postal Codes",
        ...    tabular_source=feature_store.get_table(
        ...      database_name="DEMO",
        ...      schema_name="US",
        ...      table_name="POSTAL_CODES"
        ...    ),
        ...    dimension_id_column="POSTAL_CODE_ID",
        ...    record_creation_date_column="RECORD_AVAILABLE_AT",
        ... )

        Get information about the DimensionData

        >>> us_postal_codes.info(verbose=True)  # doctest: +SKIP
        """
        return super().create(
            tabular_source=tabular_source,
            name=name,
            record_creation_date_column=record_creation_date_column,
            _id=_id,
            dimension_id_column=dimension_id_column,
        )
