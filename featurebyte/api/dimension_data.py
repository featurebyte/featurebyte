"""
DimensionData class
"""
from __future__ import annotations

from typing import Optional

from bson.objectid import ObjectId
from typeguard import typechecked

from featurebyte.api.data import DataApiObject
from featurebyte.api.database_table import DatabaseTable
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.models.dimension_data import DimensionDataModel
from featurebyte.schema.dimension_data import DimensionDataCreate, DimensionDataUpdate


class DimensionData(DimensionDataModel, DataApiObject):
    """
    DimensionData class
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(section=["Data"], proxy_class="featurebyte.DimensionData")

    # class variables
    _route = "/dimension_data"
    _update_schema_class = DimensionDataUpdate
    _create_schema_class = DimensionDataCreate

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
