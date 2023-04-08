"""
Feature Namespace module.
"""
from typing import List, Optional, Union

import pandas as pd

from featurebyte.api.api_object import ApiObject
from featurebyte.api.feature_util import (
    FEATURE_COMMON_LIST_FIELDS,
    FEATURE_LIST_FOREIGN_KEYS,
    filter_feature_list,
)
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature import (
    DefaultVersionMode,
    FeatureReadiness,
    FrozenFeatureNamespaceModel,
)
from featurebyte.schema.feature_namespace import (
    FeatureNamespaceModelResponse,
    FeatureNamespaceUpdate,
)


class FeatureNamespace(FrozenFeatureNamespaceModel, ApiObject):
    """
    FeatureNamespace represents a Feature set, in which all the features in the set have the same name. The different
    elements typically refer to different versions of a Feature.
    """

    # class variables
    _route = "/feature_namespace"
    _update_schema_class = FeatureNamespaceUpdate
    _list_schema = FeatureNamespaceModelResponse
    _get_schema = FeatureNamespaceModelResponse
    _list_fields = [
        "name",
        *FEATURE_COMMON_LIST_FIELDS,
    ]
    _list_foreign_keys = FEATURE_LIST_FOREIGN_KEYS

    @property
    def feature_ids(self) -> List[PydanticObjectId]:
        """
        List of feature IDs from the same feature namespace

        Returns
        -------
        List[PydanticObjectId]
        """
        return self.cached_model.feature_ids

    @property
    def online_enabled_feature_ids(self) -> List[PydanticObjectId]:
        """
        List of online-enabled feature IDs from the same feature namespace

        Returns
        -------
        List[PydanticObjectId]
        """
        return self.cached_model.online_enabled_feature_ids

    @property
    def readiness(self) -> FeatureReadiness:
        """
        Feature readiness of the default feature of this feature namespace

        Returns
        -------
        FeatureReadiness
        """
        return self.cached_model.readiness

    @property
    def default_feature_id(self) -> PydanticObjectId:
        """
        Default feature ID of this feature namespace

        Returns
        -------
        PydanticObjectId
        """
        return self.cached_model.default_feature_id

    @property
    def default_version_mode(self) -> DefaultVersionMode:
        """
        Default feature namespace version mode of this feature namespace

        Returns
        -------
        DefaultVersionMode
        """
        return self.cached_model.default_version_mode

    @classmethod
    def _post_process_list(cls, item_list: pd.DataFrame) -> pd.DataFrame:
        features = super()._post_process_list(item_list)

        # replace id with default_feature_id
        features["id"] = features["default_feature_id"]

        # add online_enabled
        features["online_enabled"] = features[
            ["default_feature_id", "online_enabled_feature_ids"]
        ].apply(lambda row: row[0] in row[1], axis=1)
        return features

    @classmethod
    def list(
        cls,
        include_id: Optional[bool] = False,
        primary_entity: Optional[Union[str, List[str]]] = None,
        primary_table: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """
        List saved features

        Parameters
        ----------
        include_id: Optional[bool]
            Whether to include id in the list
        primary_entity: Optional[Union[str, List[str]]]
            Name of entity used to filter results. If multiple entities are provided, the filtered results will
            contain features that are associated with all the entities.
        primary_table: Optional[Union[str, List[str]]]
            Name of table used to filter results. If multiple tables are provided, the filtered results will
            contain features that are associated with all the tables.

        Returns
        -------
        pd.DataFrame
            Table of features
        """
        feature_list = super().list(include_id=include_id)
        return filter_feature_list(feature_list, primary_entity, primary_table)
