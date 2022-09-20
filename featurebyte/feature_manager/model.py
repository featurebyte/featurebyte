"""
This modules contains feature manager specific models
"""
from __future__ import annotations

from typing import List, Optional

from pydantic import Field, StrictStr

from featurebyte.core.generic import ExtendedFeatureStoreModel
from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId, VersionIdentifier
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_list import FeatureListModel, FeatureListStatus
from featurebyte.models.tile import TileSpec
from featurebyte.query_graph.interpreter import GraphInterpreter


class ExtendedFeatureModel(FeatureModel):
    """
    ExtendedFeatureModel contains tile manager specific methods or properties
    """

    is_default: Optional[bool] = Field(allow_mutation=False)
    feature_store: ExtendedFeatureStoreModel
    version: VersionIdentifier

    @property
    def tile_specs(self) -> list[TileSpec]:
        """
        Get a list of TileSpec objects required by this Feature

        Returns
        -------
        list[TileSpec]
        """
        interpreter = GraphInterpreter(self.graph)
        tile_infos = interpreter.construct_tile_gen_sql(self.node, is_on_demand=False)
        out = []
        for info in tile_infos:
            entity_column_names = info.entity_columns[:]
            if info.value_by_column is not None:
                entity_column_names.append(info.value_by_column)
            tile_spec = TileSpec(
                time_modulo_frequency_second=info.time_modulo_frequency,
                blind_spot_second=info.blind_spot,
                frequency_minute=info.frequency // 60,
                tile_sql=info.sql,
                entity_column_names=entity_column_names,
                value_column_names=info.tile_value_columns,
                tile_id=info.tile_table_id,
                aggregation_id=info.aggregation_id,
                category_column_name=info.value_by_column,
            )
            out.append(tile_spec)
        return out


class FeatureSignature(FeatureByteBaseModel):
    """
    FeatureSignature class used in FeatureList object

    id: PydanticObjectId
        Feature id of the object
    name: str
        Name of the feature
    version: FeatureVersionIdentifier
        Feature version
    """

    id: PydanticObjectId
    name: Optional[StrictStr]
    version: Optional[VersionIdentifier]


class ExtendedFeatureListModel(FeatureListModel):
    """
    ExtendedFeatureListModel class has additional features attribute
    """

    feature_signatures: List[FeatureSignature] = Field(default_factory=list)
    status: FeatureListStatus = Field(allow_mutation=False, default=FeatureListStatus.DRAFT)
