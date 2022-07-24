"""
FeatureListVersion class
"""
from __future__ import annotations

from typing import List, Optional, Union

import pandas as pd
from pydantic import Field

from featurebyte.api.feature import Feature, FeatureGroup
from featurebyte.common.model_util import get_version
from featurebyte.config import Configurations, Credentials
from featurebyte.models.feature import FeatureListModel, FeatureListStatus, FeatureReadiness
from featurebyte.query_graph.feature_historical import get_historical_features


class FeatureList(FeatureListModel):
    """FeatureList class

    Parameters
    ----------
    items : list[Union[Feature, FeatureGroup]]
        List of feature like objects to be used to create the FeatureList
    name : str
        Name of the FeatureList
    """

    feature_objects: Optional[List[Feature]] = Field(exclude=True)

    def __init__(self, items: list[Union[Feature, FeatureGroup]], name: str):

        if not isinstance(items, list):
            raise ValueError(f"Cannot create feature list using {type(items)}; expected a list")

        for item in items:
            if not isinstance(item, (Feature, FeatureGroup)):
                raise ValueError(
                    f"Unexpected item type {type(item)}; expected Feature or FeatureGroup"
                )

        feature_versions = self._flatten_input_items(items)
        readiness = self.derive_features_readiness(feature_versions)
        versions_with_names = [(feature.name, feature.version) for feature in feature_versions]

        super().__init__(
            name=name,
            description=None,
            features=versions_with_names,
            readiness=readiness,
            status=FeatureListStatus.DRAFT,
            version=get_version(),
            created_at=None,
        )
        self.feature_objects = feature_versions

    def get_historical_features(
        self,
        training_events: pd.DataFrame,
        credentials: Credentials | None = None,
        serving_names_mapping: dict[str, str] | None = None,
    ) -> pd.DataFrame:
        """Get historical features

        Parameters
        ----------
        training_events : pd.DataFrame
            Training events DataFrame
        credentials : Credentials | None
            Optional feature store to credential mapping
        serving_names_mapping : dict[str, str] | None
            Optional serving names mapping if the training events data has different serving name
            columns than those defined in Entities. Mapping from original serving name to new
            serving name.

        Returns
        -------
        pd.DataFrame
        """
        assert self.feature_objects is not None
        if credentials is None:
            credentials = Configurations().credentials
        return get_historical_features(
            self.feature_objects,
            training_events,
            credentials=credentials,
            serving_names_mapping=serving_names_mapping,
        )

    @staticmethod
    def derive_features_readiness(features: list[Feature]) -> Optional[FeatureReadiness]:
        """Derive the features readiness based on the readiness of provided Features

        Parameters
        ----------
        features : list[Feature]
            List of Features to consider

        Returns
        -------
        FeatureReadiness
        """
        minimum_feature_readiness = min(
            features,
            key=lambda feature: FeatureReadiness(feature.readiness or FeatureReadiness.min()),
        ).readiness
        return minimum_feature_readiness

    @staticmethod
    def _flatten_input_items(items: list[Union[Feature, FeatureGroup]]) -> list[Feature]:
        flattened_items = []
        for item in items:
            if isinstance(item, Feature):
                flattened_items.append(item)
            else:
                feature_group = item
                for column_name in feature_group.columns:
                    if column_name in feature_group.entity_identifiers:
                        continue
                    feature = feature_group[column_name]
                    assert isinstance(feature, Feature)
                    flattened_items.append(feature)
        return flattened_items
