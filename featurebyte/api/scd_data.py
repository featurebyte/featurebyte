"""
SlowlyChangingData class
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from bson.objectid import ObjectId
from typeguard import typechecked

from featurebyte.api.data import DataApiObject
from featurebyte.api.database_table import DatabaseTable
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import TableDataType
from featurebyte.models.scd_data import SCDDataModel
from featurebyte.schema.scd_data import SCDDataCreate, SCDDataUpdate


class SlowlyChangingData(SCDDataModel, DataApiObject):
    """
    SlowlyChangingData class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Data"], proxy_class="featurebyte.SlowlyChangingData")

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

    @property
    def timestamp_column(self) -> Optional[str]:
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
        current_flag: Optional[str] = None,
        record_creation_date_column: Optional[str] = None,
        _id: Optional[ObjectId] = None,
    ) -> SlowlyChangingData:
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
        effective_timestamp_column: str
            Effective timestamp column from the given tabular source
        end_timestamp_column: Optional[str]
            End timestamp column from the given tabular source
        surrogate_key_column: Optional[str]
            Surrogate key column from the given tabular source
        current_flag: Optional[str]
            Indicates whether the keys are for the current data point
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
            current_flag=current_flag,
        )

    def info(self, verbose: bool = False) -> Dict[str, Any]:
        """
        Override info temporarily until we implement the info route properly.

        Parameters
        ----------
        verbose: bool
            Control verbose level of the summary

        Returns
        -------
        Dict[str, Any]
        """
        return {
            "name": self.name,
            "record_creation_date_column": self.record_creation_date_column,
            "updated_at": self.updated_at,
            "status": self.status,
            "entities": self.entity_ids,
            "natural_key_column": self.natural_key_column,
            "surrogate_key_column": self.surrogate_key_column,
            "effective_timestamp_column": self.effective_timestamp_column,
            "end_timestamp_column": self.end_timestamp_column,
            "current_flag": self.current_flag,
            "tabular_source": self.tabular_source,
            "warning": "The full info route is not implemented yet. Expect some changes shortly.",
        }
