"""
Mixin class containing common methods for feature or target classes
"""
from typing import List, Sequence, cast

import time
from http import HTTPStatus

import pandas as pd
from bson import ObjectId
from pydantic import Field

from featurebyte import Entity
from featurebyte.api.api_object import ApiObject
from featurebyte.common.formatting_util import CodeStr
from featurebyte.common.utils import dataframe_from_json
from featurebyte.config import Configurations
from featurebyte.core.generic import QueryObject
from featurebyte.exception import RecordRetrievalException
from featurebyte.logging import get_logger
from featurebyte.models.base import PydanticObjectId, get_active_catalog_id
from featurebyte.models.feature import BaseFeatureModel
from featurebyte.schema.preview import FeatureOrTargetPreview

logger = get_logger(__name__)


class FeatureOrTargetMixin(QueryObject, ApiObject):
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
        return self._cast_cached_model.version.to_str()  # pylint: disable=no-member

    def _get_catalog_id(self) -> ObjectId:
        # helper function to get catalog id
        try:
            return self._cast_cached_model.catalog_id  # pylint: disable=no-member
        except RecordRetrievalException:
            return self.internal_catalog_id

    def _get_entity_ids(self) -> Sequence[ObjectId]:
        # helper function to get entity ids
        try:
            return self._cast_cached_model.entity_ids  # pylint: disable=no-member
        except RecordRetrievalException:
            return self.graph.get_entity_ids(node_name=self.node_name)

    def _get_table_ids(self) -> Sequence[ObjectId]:
        try:
            return self._cast_cached_model.table_ids  # pylint: disable=no-member
        except RecordRetrievalException:
            return self.graph.get_table_ids(node_name=self.node_name)

    def _generate_definition(self) -> str:
        # helper function to generate definition
        try:
            definition = self._cast_cached_model.definition  # pylint: disable=no-member
            object_type = type(self).__name__.lower()
            assert definition is not None, f"Saved {object_type}'s definition should not be None."
        except RecordRetrievalException:
            definition = self._generate_code(to_format=True, to_use_saved_data=True)
        return CodeStr(definition)

    def _preview(self, observation_set: pd.DataFrame, url: str) -> pd.DataFrame:
        # helper function to preview
        tic = time.time()
        pruned_graph, mapped_node = self.extract_pruned_graph_and_node()
        payload = FeatureOrTargetPreview(
            feature_store_name=self.feature_store.name,
            graph=pruned_graph,
            node_name=mapped_node.name,
            point_in_time_and_serving_name_list=observation_set.to_dict(orient="records"),
        )

        client = Configurations().get_client()
        response = client.post(url=url, json=payload.json_dict())
        if response.status_code != HTTPStatus.OK:
            raise RecordRetrievalException(response)
        result = response.json()

        elapsed = time.time() - tic
        logger.debug(f"Preview took {elapsed:.2f}s")
        return dataframe_from_json(result)  # pylint: disable=no-member

    def _primary_entity(self) -> List[Entity]:
        """
        Returns the primary entity of the Feature object.

        Returns
        -------
        List[Entity]
            Primary entity
        """
        entities = []
        for entity_id in self._get_entity_ids():
            entities.append(Entity.get_by_id(entity_id))
        primary_entity = derive_primary_entity(entities)  # type: ignore
        return primary_entity
