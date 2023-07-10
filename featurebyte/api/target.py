"""
Target API object
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from http import HTTPStatus

import pandas as pd
from bson import ObjectId
from pydantic import Field, StrictStr, root_validator
from typeguard import typechecked

from featurebyte.api.api_object_util import ForeignKeyMapping
from featurebyte.api.entity import Entity
from featurebyte.api.feature_store import FeatureStore
from featurebyte.api.observation_table import ObservationTable
from featurebyte.api.savable_api_object import SavableApiObject
from featurebyte.api.target_table import TargetTable
from featurebyte.common.utils import (
    dataframe_from_json,
    dataframe_to_arrow_bytes,
    enforce_observation_set_row_order,
)
from featurebyte.config import Configurations
from featurebyte.core.series import Series
from featurebyte.enum import DBVarType
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.target import TargetModel
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.schema.preview import FeatureOrTargetPreview
from featurebyte.schema.target import TargetUpdate
from featurebyte.schema.target_table import TargetTableCreate


class Target(Series, SavableApiObject):
    """
    Target class used to represent a Target in FeatureByte.
    """

    internal_entity_ids: Optional[List[PydanticObjectId]] = Field(alias="entity_ids")
    internal_horizon: Optional[StrictStr] = Field(alias="horizon")
    internal_node_name: Optional[str] = Field(allow_mutation=False, alias="node_name")
    internal_dtype: DBVarType = Field(allow_mutation=False, alias="dtype")
    internal_tabular_source: TabularSource = Field(allow_mutation=False, alias="tabular_source")

    # pydantic instance variable (public)
    feature_store: FeatureStoreModel = Field(
        exclude=True,
        allow_mutation=False,
        description="Provides information about the feature store that the target is connected to.",
    )

    _route = "/target"
    _update_schema_class = TargetUpdate

    _list_schema = TargetModel
    _get_schema = TargetModel
    _list_fields = ["name", "entities"]
    _list_foreign_keys = [
        ForeignKeyMapping("entity_ids", Entity, "entities"),
    ]

    @root_validator(pre=True)
    @classmethod
    def _set_feature_store(cls, values: dict[str, Any]) -> dict[str, Any]:
        if "feature_store" not in values:
            tabular_source = values.get("tabular_source")
            if isinstance(tabular_source, dict):
                feature_store_id = TabularSource(**tabular_source).feature_store_id
                values["feature_store"] = FeatureStore.get_by_id(id=feature_store_id)
        return values

    def _get_init_params_from_object(self) -> dict[str, Any]:
        return {
            "feature_store": self.feature_store,
        }

    @property
    def entities(self) -> List[Entity]:
        """
        Returns a list of entities associated with this target.

        Returns
        -------
        List[Entity]
        """
        try:
            entity_ids = self.cached_model.entity_ids  # type: ignore[attr-defined]
        except RecordRetrievalException:
            entity_ids = self.internal_entity_ids
        return [Entity.get_by_id(entity_id) for entity_id in entity_ids]

    @property
    def horizon(self) -> Optional[str]:
        """
        Returns the horizon of this target.

        Returns
        -------
        Optional[str]
        """
        try:
            return self.cached_model.horizon
        except RecordRetrievalException:
            return self.internal_horizon

    def _get_pruned_target_model(self) -> TargetModel:
        """
        Get pruned model of target

        Returns
        -------
        FeatureModel
        """
        pruned_graph, mapped_node = self.extract_pruned_graph_and_node()
        target_dict = self.dict(by_alias=True)
        target_dict["graph"] = pruned_graph.dict()
        target_dict["node_name"] = mapped_node.name
        return TargetModel(**target_dict)

    @enforce_observation_set_row_order
    @typechecked
    def preview(
        self,
        observation_set: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Materializes a Target object using a small observation set of up to 50 rows.

        The small observation set should combine points-in-time and key values of the primary entity from
        the target. Associated serving entities can also be utilized.

        Parameters
        ----------
        observation_set : pd.DataFrame
            Observation set DataFrame which combines points-in-time and values of the target primary entity
            or its descendant (serving entities). The column containing the point-in-time values should be named
            `POINT_IN_TIME`, while the columns representing entity values should be named using accepted serving
            names for the entity.

        Returns
        -------
        pd.DataFrame
            Materialized target values.
            The returned DataFrame will have the same number of rows, and include all columns from the observation set.

            **Note**: `POINT_IN_TIME` values will be converted to UTC time.

        Raises
        ------
        RecordRetrievalException
            Failed to materialize feature preview.
        """
        target = self._get_pruned_target_model()
        graph = target.graph
        node_name = target.node_name
        assert node_name is not None
        payload = FeatureOrTargetPreview(
            feature_store_name=self.feature_store.name,
            graph=graph,
            node_name=node_name,
            point_in_time_and_serving_name_list=observation_set.to_dict(orient="records"),
        )

        client = Configurations().get_client()
        response = client.post(url="/target/preview", json=payload.json_dict())
        if response.status_code != HTTPStatus.OK:
            raise RecordRetrievalException(response)
        result = response.json()

        return dataframe_from_json(result)  # pylint: disable=no-member

    @enforce_observation_set_row_order
    @typechecked
    def compute_target(
        self,
        observation_set: pd.DataFrame,
        serving_names_mapping: Optional[Dict[str, str]] = None,
    ) -> pd.DataFrame:
        """
        Returns a DataFrame with target values for analysis, model training, or evaluation. The target
        request data consists of an observation set that combines points-in-time and key values of the
        primary entity from the target.

        Associated serving entities can also be utilized.

        Parameters
        ----------
        observation_set : pd.DataFrame
            Observation set DataFrame or ObservationTable object, which combines points-in-time and values
            of the target primary entity or its descendant (serving entities). The column containing the point-in-time
            values should be named `POINT_IN_TIME`, while the columns representing entity values should be named using
            accepted serving names for the entity.
        serving_names_mapping : Optional[Dict[str, str]]
            Optional serving names mapping if the training events table has different serving name columns than those
            defined in Entities, mapping from original serving name to new name.

        Returns
        -------
        pd.DataFrame
            Materialized target.

            **Note**: `POINT_IN_TIME` values will be converted to UTC time.
        """
        temp_target_table_name = f"__TEMPORARY_TARGET_TABLE_{ObjectId()}"
        temp_target_table = self.compute_target_table(
            observation_table=observation_set,
            target_table_name=temp_target_table_name,
            serving_names_mapping=serving_names_mapping,
        )
        try:
            return temp_target_table.to_pandas()
        finally:
            temp_target_table.delete()

    @typechecked
    def compute_target_table(
        self,
        observation_table: Union[ObservationTable, pd.DataFrame],
        target_table_name: str,
        serving_names_mapping: Optional[Dict[str, str]] = None,
    ) -> TargetTable:
        """
        Materialize feature list using an observation table asynchronously. The targets
        will be materialized into a target table.

        Parameters
        ----------
        observation_table: Union[ObservationTable, pd.DataFrame]
            Observation set with `POINT_IN_TIME` and serving names columns. This can be either an
            ObservationTable of a pandas DataFrame.
        target_table_name: str
            Name of the target table to be created
        serving_names_mapping : Optional[Dict[str, str]]
            Optional serving names mapping if the training events table has different serving name

        Returns
        -------
        TargetTable
        """
        target_table_create_params = TargetTableCreate(
            name=target_table_name,
            observation_table_id=(
                observation_table.id if isinstance(observation_table, ObservationTable) else None
            ),
            feature_store_id=self.feature_store.id,
            serving_names_mapping=serving_names_mapping,
            target_id=self.id,
            graph=self.graph,
            node_names=[self.node.name],
        )
        if isinstance(observation_table, ObservationTable):
            files = None
        else:
            assert isinstance(observation_table, pd.DataFrame)
            files = {"observation_set": dataframe_to_arrow_bytes(observation_table)}
        target_table_doc = self.post_async_task(
            route="/target_table",
            payload={"payload": target_table_create_params.json()},
            is_payload_json=False,
            files=files,
        )
        return TargetTable.get_by_id(target_table_doc["_id"])
