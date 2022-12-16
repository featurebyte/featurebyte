"""
SlowlyChangingData class
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from bson.objectid import ObjectId
from typeguard import typechecked

from featurebyte.api.api_object import PrettyDict
from featurebyte.api.data import DataApiObject
from featurebyte.api.database_table import DatabaseTable
from featurebyte.common.doc_util import FBAutoDoc
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

    def info(self, verbose: bool = False) -> Dict[str, Any]:
        """
        Provide basic info for SCD data.

        Parameters
        ----------
        verbose: bool
            This is a no-op for now. This will be used when we add more functionality to this funciton.

        Returns
        -------
        Dict[str, Any]
        """
        return PrettyDict(
            {
                "name": self.name,
                "record_creation_date_column": self.record_creation_date_column,
                "updated_at": self.updated_at,
                "status": self.status,
                "entities": self.entity_ids,
                "natural_key_column": self.natural_key_column,
                "surrogate_key_column": self.surrogate_key_column,
                "effective_timestamp_column": self.effective_timestamp_column,
                "end_timestamp_column": self.end_timestamp_column,
                "current_flag_column": self.current_flag_column,
                "tabular_source": self.tabular_source,
            }
        )
