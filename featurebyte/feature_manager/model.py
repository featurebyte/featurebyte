"""
This modules contains feature manager specific models
"""

from __future__ import annotations

from featurebyte.models.feature import FeatureModel
from featurebyte.models.tile import TileSpec
from featurebyte.query_graph.sql.interpreter import GraphInterpreter


class ExtendedFeatureModel(FeatureModel):
    """
    ExtendedFeatureModel contains tile manager specific methods or properties
    """

    @property
    def tile_specs(self) -> list[TileSpec]:
        """
        Get a list of TileSpec objects required by this Feature

        Returns
        -------
        list[TileSpec]
        """
        interpreter = GraphInterpreter(self.graph, self.get_source_info())
        node = self.graph.get_node_by_name(self.node_name)
        tile_infos = interpreter.construct_tile_gen_sql(node, is_on_demand=False)
        out = []
        for info in tile_infos:
            entity_column_names = info.entity_columns[:]
            if info.value_by_column is not None:
                entity_column_names.append(info.value_by_column)
            tile_spec = TileSpec(
                time_modulo_frequency_second=info.time_modulo_frequency,
                blind_spot_second=info.blind_spot,
                frequency_minute=info.frequency // 60,
                tile_compute_query=info.tile_compute_query,
                entity_column_names=entity_column_names,
                value_column_names=info.tile_value_columns,
                value_column_types=info.tile_value_types,
                tile_id=info.tile_table_id,
                aggregation_id=info.aggregation_id,
                aggregation_function_name=info.agg_func,
                parent_column_name=info.parent,
                category_column_name=info.value_by_column,
                feature_store_id=self.tabular_source.feature_store_id,
                windows=info.windows,
                offset=info.offset,
            )
            out.append(tile_spec)
        return out
