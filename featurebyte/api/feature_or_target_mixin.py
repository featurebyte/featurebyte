"""
Mixin class containing common methods for feature or target classes
"""

import time
from abc import ABC
from http import HTTPStatus
from typing import Any, Sequence, Union, cast

import pandas as pd
from bson import ObjectId
from pydantic import Field
from typeguard import typechecked

from featurebyte.api.observation_table import ObservationTable
from featurebyte.api.primary_entity_mixin import PrimaryEntityMixin
from featurebyte.common import get_active_catalog_id
from featurebyte.common.formatting_util import CodeStr
from featurebyte.common.utils import dataframe_from_json
from featurebyte.config import Configurations
from featurebyte.core.generic import QueryObject
from featurebyte.exception import RecordRetrievalException
from featurebyte.logging import get_logger
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature import BaseFeatureModel
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node.generic import AliasNode, ProjectNode
from featurebyte.schema.preview import FeatureOrTargetPreview

logger = get_logger(__name__)


class FeatureOrTargetMixin(QueryObject, PrimaryEntityMixin, ABC):
    """
    Mixin class containing common methods for feature or target classes
    """

    # pydantic instance variable (internal use)
    internal_catalog_id: PydanticObjectId = Field(
        default_factory=get_active_catalog_id, alias="catalog_id"
    )

    @property
    def _cast_cached_model(self) -> BaseFeatureModel:
        return cast(BaseFeatureModel, self.cached_model)

    def _get_version(self) -> str:
        # helper function to get version
        return self._cast_cached_model.version.to_str()

    def _get_catalog_id(self) -> ObjectId:
        # helper function to get catalog id
        try:
            return self._cast_cached_model.catalog_id
        except RecordRetrievalException:
            return self.internal_catalog_id

    def _get_entity_ids(self) -> Sequence[ObjectId]:
        # helper function to get entity ids
        try:
            return self._cast_cached_model.entity_ids
        except RecordRetrievalException:
            return self.graph.get_entity_ids(node_name=self.node_name)

    def _get_table_ids(self) -> Sequence[ObjectId]:
        try:
            return self._cast_cached_model.table_ids
        except RecordRetrievalException:
            return self.graph.get_table_ids(node_name=self.node_name)

    def _generate_definition(self) -> str:
        # helper function to generate definition
        try:
            definition = self._cast_cached_model.definition
            object_type = type(self).__name__.lower()
            assert definition is not None, f"Saved {object_type}'s definition should not be None."
        except RecordRetrievalException:
            definition = self._generate_code(to_format=True, to_use_saved_data=True)
        return CodeStr(definition)

    def _preview(
        self, observation_set: Union[ObservationTable, pd.DataFrame], url: str
    ) -> pd.DataFrame:
        # helper function to preview
        tic = time.time()

        pruned_graph, mapped_node = self.extract_pruned_graph_and_node()
        preview_params = {
            "graph": QueryGraph(**pruned_graph.model_dump(by_alias=True)),
            "node_name": mapped_node.name,
            "feature_store_id": self.feature_store.id,
        }
        if isinstance(observation_set, ObservationTable):
            preview_params["observation_table_id"] = observation_set.id
        else:
            preview_params["point_in_time_and_serving_name_list"] = observation_set.to_dict(
                orient="records"
            )

        payload = FeatureOrTargetPreview(**preview_params)
        client = Configurations().get_client()
        response = client.post(url=url, json=payload.json_dict())
        if response.status_code != HTTPStatus.OK:
            raise RecordRetrievalException(response)
        result = response.json()

        elapsed = time.time() - tic
        logger.debug(f"Preview took {elapsed:.2f}s")
        return dataframe_from_json(result)

    @typechecked
    def __setattr__(self, key: str, value: Any) -> Any:
        """
        Custom __setattr__ to handle setting of special attributes such as name

        Parameters
        ----------
        key : str
            Key
        value : Any
            Value

        Raises
        ------
        ValueError
            if the name parameter is invalid

        Returns
        -------
        Any
        """
        if key != "name":
            return super().__setattr__(key, value)

        if value is None:
            raise ValueError("None is not a valid feature name")

        # For now, only allow updating name if the feature is unnamed (i.e. created on-the-fly by
        # combining different features)
        name = value
        node = self.node
        if node.type in {NodeType.PROJECT, NodeType.ALIAS}:
            if isinstance(node, ProjectNode):
                existing_name = node.parameters.columns[0]
            else:
                assert isinstance(node, AliasNode)
                existing_name = node.parameters.name  # type: ignore
            if name != existing_name:
                raise ValueError(f'Feature "{existing_name}" cannot be renamed to "{name}"')
            # FeatureGroup sets name unconditionally, so we allow this here
            return super().__setattr__(key, value)

        # Here, node could be any node resulting from series operations, e.g. DIV. This
        # validation was triggered by setting the name attribute of a Feature object
        new_node = self.graph.add_operation(
            node_type=NodeType.ALIAS,
            node_params={"name": name},
            node_output_type=NodeOutputType.SERIES,
            input_nodes=[node],
        )
        self.node_name = new_node.name
        return super().__setattr__(key, value)
