"""
This modules contains feature manager specific models
"""
from __future__ import annotations

from typing import Tuple

from featurebyte.core.generic import ExtendedFeatureStoreModel
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_store import TableDetails
from featurebyte.models.tile import TileSpec
from featurebyte.query_graph.interpreter import GraphInterpreter


class ExtendedFeatureModel(FeatureModel):
    """
    ExtendedFeatureModel contains tile manager specific methods or properties
    """

    tabular_source: Tuple[ExtendedFeatureStoreModel, TableDetails]

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
            tile_spec = TileSpec(
                time_modulo_frequency_second=info.time_modulo_frequency,
                blind_spot_second=info.blind_spot,
                frequency_minute=info.frequency // 60,
                tile_sql=info.sql,
                entity_column_names=info.entity_columns,
                value_column_names=info.tile_value_columns,
                tile_id=info.tile_table_id,
            )
            out.append(tile_spec)
        return out
