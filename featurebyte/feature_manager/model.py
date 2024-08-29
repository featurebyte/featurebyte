"""
This modules contains feature manager specific models
"""

from __future__ import annotations

from featurebyte.models.feature import FeatureModel
from featurebyte.models.tile import TileSpec
from featurebyte.query_graph.sql.interpreter import GraphInterpreter
from featurebyte.query_graph.sql.source_info import SourceInfo


class ExtendedFeatureModel(FeatureModel):
    """
    ExtendedFeatureModel contains tile manager specific methods or properties
    """

    def get_source_info(self) -> SourceInfo:
        """
        Get source info corresponding to the feature store

        Returns
        -------
        SourceInfo
        """
        return self.graph.get_input_node(self.node.name).parameters.get_source_info()

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
                tile_sql=info.sql,
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
