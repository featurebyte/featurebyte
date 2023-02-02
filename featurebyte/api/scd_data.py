"""
SlowlyChangingData class
"""
from __future__ import annotations

from typing import Optional

from bson.objectid import ObjectId
from pydantic import Field, StrictStr, root_validator
from typeguard import typechecked

from featurebyte.api.base_data import DataApiObject
from featurebyte.api.database_table import DatabaseTable
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import DBVarType
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.feature_store import FrozenDataModel
from featurebyte.models.validator import construct_data_model_root_validator
from featurebyte.query_graph.model.table import SCDTableData
from featurebyte.schema.scd_data import SCDDataCreate, SCDDataUpdate


class SlowlyChangingData(SCDTableData, FrozenDataModel, DataApiObject):
    """
    SlowlyChangingData class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Data"], proxy_class="featurebyte.SlowlyChangingData")

    # class variables
    _route = "/scd_data"
    _update_schema_class = SCDDataUpdate
    _create_schema_class = SCDDataCreate
    _table_data_class = SCDTableData

    # pydantic instance variable (internal use)
    int_natural_key_column: StrictStr = Field(alias="natural_key_column")
    int_effective_timestamp_column: StrictStr = Field(alias="effective_timestamp_column")
    int_surrogate_key_column: Optional[StrictStr] = Field(alias="surrogate_key_column")
    int_end_timestamp_column: Optional[StrictStr] = Field(
        default=None, alias="end_timestamp_column"
    )
    int_current_flag_column: Optional[StrictStr] = Field(default=None, alias="current_flag_column")

    # pydantic validators
    _root_validator = root_validator(allow_reuse=True)(
        construct_data_model_root_validator(
            expected_column_field_name_type_pairs=[
                ("int_record_creation_date_column", DBVarType.supported_timestamp_types()),
                ("natural_key_column", DBVarType.supported_id_types()),
                ("effective_timestamp_column", DBVarType.supported_timestamp_types()),
                ("surrogate_key_column", DBVarType.supported_id_types()),
                ("end_timestamp_column", DBVarType.supported_timestamp_types()),
                ("current_flag_column", None),
            ],
        )
    )

    @property
    def natural_key_column(self):
        try:
            return self.cached_model.natural_key_column
        except RecordRetrievalException:
            return self.int_natural_key_column

    @property
    def effective_timestamp_column(self):
        try:
            return self.cached_model.effective_timestamp_column
        except RecordRetrievalException:
            return self.int_effective_timestamp_column

    @property
    def surrogate_key_column(self):
        try:
            return self.cached_model.surrogate_key_column
        except RecordRetrievalException:
            return self.int_surrogate_key_column

    @property
    def end_timestamp_column(self):
        try:
            return self.cached_model.end_timestamp_column
        except RecordRetrievalException:
            return self.int_end_timestamp_column

    @property
    def current_flag_column(self):
        try:
            return self.cached_model.current_flag_column
        except RecordRetrievalException:
            return self.current_flag_column

    @property
    def timestamp_column(self) -> Optional[str]:
        """
        Effective timestamp column

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
