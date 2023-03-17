"""
SlowlyChangingData class
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
from featurebyte.models.scd_data import SCDDataModel
from featurebyte.query_graph.model.table import AllTableDataT, SCDTableData
from featurebyte.schema.scd_data import SCDDataCreate, SCDDataUpdate


class SlowlyChangingData(DataApiObject):
    """
    SlowlyChangingData is a data source object connected with a Slowly Changing Dimension table of Type 2 in
    the data warehouse that has:\n
    - a natural key (key for which there is one unique active record)\n
    - a surrogate key (the primary key of the SCD)\n
    - an effective date or timestamp\n

    and optionally,\n
    - an end date or timestamp and\n
    - a current flag

    To create an instance of this class, see the `from_tabular_source` method.

    To build features, users can create SlowlyChangingViews from SlowlyChangingData.
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Data"], proxy_class="featurebyte.SlowlyChangingData")

    # class variables
    _route = "/scd_data"
    _update_schema_class = SCDDataUpdate
    _create_schema_class = SCDDataCreate
    _get_schema = SCDDataModel
    _table_data_class: ClassVar[Type[AllTableDataT]] = SCDTableData

    # pydantic instance variable (public)
    type: Literal[TableDataType.SCD_DATA] = Field(TableDataType.SCD_DATA, const=True)

    # pydantic instance variable (internal use)
    internal_natural_key_column: StrictStr = Field(alias="natural_key_column")
    internal_effective_timestamp_column: StrictStr = Field(alias="effective_timestamp_column")
    internal_surrogate_key_column: Optional[StrictStr] = Field(alias="surrogate_key_column")
    internal_end_timestamp_column: Optional[StrictStr] = Field(
        default=None, alias="end_timestamp_column"
    )
    internal_current_flag_column: Optional[StrictStr] = Field(
        default=None, alias="current_flag_column"
    )

    # pydantic validators
    _root_validator = root_validator(allow_reuse=True)(
        construct_data_model_root_validator(
            columns_info_key="internal_columns_info",
            expected_column_field_name_type_pairs=[
                ("internal_record_creation_date_column", DBVarType.supported_timestamp_types()),
                ("internal_natural_key_column", DBVarType.supported_id_types()),
                ("internal_effective_timestamp_column", DBVarType.supported_timestamp_types()),
                ("internal_surrogate_key_column", DBVarType.supported_id_types()),
                ("internal_end_timestamp_column", DBVarType.supported_timestamp_types()),
                ("internal_current_flag_column", None),
            ],
        )
    )

    @property
    def natural_key_column(self) -> str:
        """
        Natural key column name of the SlowlyChangingData

        Returns
        -------
        str
        """
        try:
            return self.cached_model.natural_key_column
        except RecordRetrievalException:
            return self.internal_natural_key_column

    @property
    def effective_timestamp_column(self) -> str:
        """
        Effective timestamp column name of the SlowlyChangingData

        Returns
        -------
        str
        """
        try:
            return self.cached_model.effective_timestamp_column
        except RecordRetrievalException:
            return self.internal_effective_timestamp_column

    @property
    def surrogate_key_column(self) -> Optional[str]:
        """
        Surrogate key column name of the SlowlyChangingData

        Returns
        -------
        Optional[str]
        """
        try:
            return self.cached_model.surrogate_key_column
        except RecordRetrievalException:
            return self.internal_surrogate_key_column

    @property
    def end_timestamp_column(self) -> Optional[str]:
        """
        End timestamp column name of the SlowlyChangingData

        Returns
        -------
        Optional[str]
        """
        try:
            return self.cached_model.end_timestamp_column
        except RecordRetrievalException:
            return self.internal_end_timestamp_column

    @property
    def current_flag_column(self) -> Optional[str]:
        """
        Current flag column name of the SlowlyChangingData

        Returns
        -------
        Optional[str]
        """
        try:
            return self.cached_model.current_flag_column
        except RecordRetrievalException:
            return self.internal_current_flag_column

    @property
    def timestamp_column(self) -> Optional[str]:
        """
        Timestamp column name of the SlowlyChangingData

        Returns
        -------
        Optional[str]
        """
        return self.effective_timestamp_column

    @classmethod
    @typechecked
    def from_tabular_source(
        cls,
        tabular_source: DatabaseTable,
        name: str,
        natural_key_column: str,
        effective_timestamp_column: str,
        end_timestamp_column: Optional[str] = None,
        surrogate_key_column: Optional[str] = None,
        current_flag_column: Optional[str] = None,
        record_creation_date_column: Optional[str] = None,
        _id: Optional[ObjectId] = None,
    ) -> SlowlyChangingData:
        """
        Create SlowlyChangingData object from tabular source

        Parameters
        ----------
        tabular_source: DatabaseTable
            DatabaseTable object constructed from FeatureStore
        name: str
            SlowlyChanging data name
        natural_key_column: str
            Natural key column from the given tabular source
        effective_timestamp_column: str
            Effective timestamp column from the given tabular source
        end_timestamp_column: Optional[str]
            End timestamp column from the given tabular source
        surrogate_key_column: Optional[str]
            Surrogate key column from the given tabular source
        current_flag_column: Optional[str]
            Column to indicates whether the keys are for the current data point
        record_creation_date_column: str
            Record creation datetime column from the given tabular source
        _id: Optional[ObjectId]
            Identity value for constructed object

        Returns
        -------
        SlowlyChangingData

        Examples
        --------
        Create SlowlyChangingData from a table in the feature store

        >>> user_profiles = SlowlyChangingData.from_tabular_source(  # doctest: +SKIP
        ...    name="User Profiles",
        ...    tabular_source=feature_store.get_table(
        ...      database_name="DEMO",
        ...      schema_name="USER",
        ...      table_name="PROFILES"
        ...    ),
        ...    natural_key_column="USER_ID",
        ...    effective_timestamp_column="EFFECTIVE_AT",
        ...    end_timestamp_column="END_AT",
        ...    surrogate_key_column="ID",
        ...    current_flag_column="IS_CURRENT",
        ...    record_creation_date_column="RECORD_AVAILABLE_AT",
        ... )

        Get information about the SlowlyChangingData

        >>> user_profiles.info(verbose=True)  # doctest: +SKIP
        """
        return super().create(
            tabular_source=tabular_source,
            name=name,
            record_creation_date_column=record_creation_date_column,
            _id=_id,
            natural_key_column=natural_key_column,
            surrogate_key_column=surrogate_key_column,
            effective_timestamp_column=effective_timestamp_column,
            end_timestamp_column=end_timestamp_column,
            current_flag_column=current_flag_column,
        )
