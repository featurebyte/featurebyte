"""
SCDData class
"""
from __future__ import annotations

from typing import Any, Optional

from bson.objectid import ObjectId
from typeguard import typechecked

from featurebyte.api.data import DataApiObject
from featurebyte.api.database_table import DatabaseTable
from featurebyte.enum import TableDataType
from featurebyte.models.scd_data import SCDDataModel
from featurebyte.schema.scd_data import SCDDataCreate, SCDDataUpdate


class SCDData(SCDDataModel, DataApiObject):
    """
    SCDData class
    """

    # class variables
    _route = "/scd_data"
    _update_schema_class = SCDDataUpdate
    _create_schema_class = SCDDataCreate

    def _get_create_payload(self) -> dict[str, Any]:
        data = SCDDataCreate(**self.json_dict())
        return data.json_dict()

    @classmethod
    def _get_other_input_node_parameters(cls, values: dict[str, Any]) -> dict[str, Any]:
        # the key `_id` is used during deserialization, the key `id` is used during setattr
        return {
            "type": TableDataType.SCD_DATA,
            "id": values.get("_id", values.get("id")),
        }

    @classmethod
    @typechecked
    def from_tabular_source(
        cls,
        tabular_source: DatabaseTable,
        name: str,
        natural_key_column: str,
        surrogate_key_column: str,
        effective_timestamp_column: str,
        end_timestamp_column: Optional[str] = None,
        current_flag: Optional[str] = None,
        record_creation_date_column: Optional[str] = None,
        _id: Optional[ObjectId] = None,
    ) -> SCDData:
        """
        Create SCDData object from tabular source

        Parameters
        ----------
        tabular_source: DatabaseTable
            DatabaseTable object constructed from FeatureStore
        name: str
            SCD data name
        natural_key_column: str
            Natural key column from the given tabular source
        surrogate_key_column: str
            Surrogate key column from the given tabular source
        effective_timestamp_column: str
            Effective timestamp column from the given tabular source
        end_timestamp_column: Optional[str]
            End timestamp column from the given tabular source
        current_flag: Optional[str]
            Indicates whether the keys are for the current data point
        record_creation_date_column: str
            Record creation datetime column from the given tabular source
        _id: Optional[ObjectId]
            Identity value for constructed object

        Returns
        -------
        SCDData
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
            current_flag=current_flag,
        )
