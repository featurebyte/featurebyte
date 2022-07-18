"""
Feature and FeatureList classes
"""
from __future__ import annotations

from typing import Any, Tuple

import time

import pandas as pd

from featurebyte.api.feature_store import FeatureStore
from featurebyte.config import Credentials
from featurebyte.core.frame import Frame
from featurebyte.core.generic import ProtectedColumnsQueryObject
from featurebyte.core.series import Series
from featurebyte.enum import SpecialColumnName
from featurebyte.logger import logger
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_store import TableDetails
from featurebyte.models.tile import TileSpec
from featurebyte.query_graph.feature_preview import get_feature_preview_sql
from featurebyte.query_graph.interpreter import GraphInterpreter


class FeatureQueryObject(ProtectedColumnsQueryObject):
    """
    FeatureMixin contains common properties & operations shared between FeatureList & Feature
    """

    def __repr__(self) -> str:
        return f"{type(self).__name__}(node.name={self.node.name}, entity_identifiers={self.entity_identifiers})"

    @property
    def protected_attributes(self) -> list[str]:
        """
        List of protected attributes used to extract protected_columns

        Returns
        -------
        list[str]
        """
        return ["entity_identifiers"]

    @property
    def entity_identifiers(self) -> list[str]:
        """
        Entity identifiers column names

        Returns
        -------
        list[str]
        """
        entity_ids: list[str] = self.inception_node.parameters["keys"]
        return entity_ids

    @property
    def serving_names(self) -> list[str]:
        """
        Serving name columns

        Returns
        -------
        list[str]
        """
        serving_names: list[str] = self.inception_node.parameters["serving_names"]
        return serving_names

    @property
    def inherited_columns(self) -> set[str]:
        """
        Special columns set which will be automatically added to the object of same class
        derived from current object

        Returns
        -------
        set[str]
        """
        return set(self.entity_identifiers)

    def preview(  # type: ignore[override]  # pylint: disable=arguments-renamed
        self,
        point_in_time_and_entity_id: dict[str, Any],
        credentials: Credentials | None = None,
    ) -> pd.DataFrame:
        """
        Preview a FeatureGroup

        Parameters
        ----------
        point_in_time_and_entity_id : dict
            Dictionary consisting the point in time and entity ids based on which the feature
            preview will be computed
        credentials: Credentials | None
            credentials to create a database session

        Returns
        -------
        pd.DataFrame
        """
        tic = time.time()
        self._validate_point_in_time_and_entity_id(point_in_time_and_entity_id)
        preview_sql = get_feature_preview_sql(
            graph=self.graph,
            node=self.node,
            point_in_time_and_entity_id=point_in_time_and_entity_id,
        )
        session = self.get_session(credentials)
        result = session.execute_query(preview_sql)
        elapsed = time.time() - tic
        logger.debug(f"Preview took {elapsed:.2f}s")
        return result

    def _validate_point_in_time_and_entity_id(
        self, point_in_time_and_entity_id: dict[str, Any]
    ) -> None:

        if not isinstance(point_in_time_and_entity_id, dict):
            raise ValueError("point_in_time_and_entity_id should be a dict")

        if SpecialColumnName.POINT_IN_TIME not in point_in_time_and_entity_id:
            raise KeyError(f"Point in time column not provided: {SpecialColumnName.POINT_IN_TIME}")

        if self.serving_names is not None:
            for col in self.serving_names:
                if col not in point_in_time_and_entity_id:
                    raise KeyError(f"Entity column not provided: {col}")


class Feature(FeatureQueryObject, Series, FeatureModel):
    """
    Feature class
    """

    # Although tabular_source is already defined in FeatureModel, here it is redefined so that
    # pydantic knows to deserialize the first element as a FeatureStore instead of a
    # FeatureStoreModel
    tabular_source: Tuple[FeatureStore, TableDetails]

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


class FeatureGroup(FeatureQueryObject, Frame):
    """
    FeatureList class
    """

    _series_class = Feature

    def __getitem__(self, item: str | list[str] | Series) -> Series | Frame:
        if isinstance(item, list) and all(isinstance(elem, str) for elem in item):
            item = sorted(self.protected_columns.union(item))
        return super().__getitem__(item)

    def __setitem__(self, key: str, value: int | float | str | bool | Series) -> None:
        if key in self.protected_columns:
            raise ValueError(f"Entity identifier column '{key}' cannot be modified!")
        super().__setitem__(key, value)
