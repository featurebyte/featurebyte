"""
Feature Namespace module.
"""

from __future__ import annotations

from typing import Any, ClassVar, Dict, List, Optional, Union

import pandas as pd
from typeguard import typechecked

from featurebyte.api.api_handler.base import ListHandler
from featurebyte.api.api_handler.feature_namespace import FeatureNamespaceListHandler
from featurebyte.api.api_object_util import ForeignKeyMapping, resolve_context_id
from featurebyte.api.feature_or_target_namespace_mixin import FeatureOrTargetNamespaceMixin
from featurebyte.api.feature_util import (
    FEATURE_COMMON_LIST_FIELDS,
    FEATURE_LIST_FOREIGN_KEYS,
    filter_feature_list,
)
from featurebyte.enum import FeatureType
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_namespace import FeatureReadiness
from featurebyte.schema.feature_namespace import (
    FeatureNamespaceModelResponse,
    FeatureNamespaceUpdate,
)


class FeatureNamespace(FeatureOrTargetNamespaceMixin):
    """
    FeatureNamespace represents a Feature set, in which all the features in the set have the same name. The different
    elements typically refer to different versions of a Feature.
    """

    # class variables
    _route: ClassVar[str] = "/feature_namespace"
    _update_schema_class: ClassVar[Any] = FeatureNamespaceUpdate
    _list_schema: ClassVar[Any] = FeatureNamespaceModelResponse
    _get_schema: ClassVar[Any] = FeatureNamespaceModelResponse
    _list_fields: ClassVar[List[str]] = [
        "name",
        *FEATURE_COMMON_LIST_FIELDS,
    ]
    _list_foreign_keys: ClassVar[List[ForeignKeyMapping]] = FEATURE_LIST_FOREIGN_KEYS

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
    def feature_type(self) -> FeatureType:
        """
        Feature type of the default feature of this feature namespace

        Returns
        -------
        FeatureType
        """
        return self.cached_model.feature_type

    @classmethod
    def _list_handler(cls) -> ListHandler:
        return FeatureNamespaceListHandler(
            route=cls._route,
            list_schema=cls._list_schema,
            list_fields=cls._list_fields,
            list_foreign_keys=cls._list_foreign_keys,
        )

    @classmethod
    def get(
        cls,
        name: str,
        context: Optional[str] = None,
        use_case: Optional[str] = None,
    ) -> FeatureNamespace:
        """
        Retrieve the FeatureNamespace from the persistent data store given the object's name.

        This assumes that the object has been saved to the persistent data store. If the object has not been saved,
        an exception will be raised and you should create and save the object first.

        Parameters
        ----------
        name: str
            Name of the FeatureNamespace to retrieve.
        context: Optional[str]
            Name of context used to filter results. If provided, results include both regular features
            and features specific to that context. If not provided, context-specific features
            (e.g. from user-provided columns) are excluded.
        use_case: Optional[str]
            Name of use case used to filter results. The context associated with the use case will be
            used for filtering. Cannot be specified together with context.

        Returns
        -------
        FeatureNamespace
            FeatureNamespace object.
        """
        params: Dict[str, Any] = {}
        context_id = resolve_context_id(context, use_case)
        if context_id is not None:
            params["context_id"] = context_id
        return cls._get(name=name, other_params=params)

    @classmethod
    def list(
        cls,
        include_id: Optional[bool] = False,
        primary_entity: Optional[Union[str, List[str]]] = None,
        primary_table: Optional[Union[str, List[str]]] = None,
        context: Optional[str] = None,
        use_case: Optional[str] = None,
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
        context: Optional[str]
            Name of context used to filter results. If provided, results include both regular features
            and features specific to that context. If not provided, context-specific features
            (e.g. from user-provided columns) are excluded.
        use_case: Optional[str]
            Name of use case used to filter results. The context associated with the use case will be
            used for filtering. Cannot be specified together with context.

        Returns
        -------
        pd.DataFrame
            Table of features
        """
        params: Dict[str, Any] = {}
        context_id = resolve_context_id(context, use_case)
        if context_id is not None:
            params["context_id"] = context_id
        feature_list = cls._list(include_id=include_id, params=params)
        return filter_feature_list(feature_list, primary_entity, primary_table)

    @typechecked
    def update_feature_type(self, feature_type: Union[FeatureType, str]) -> None:
        """
        Update feature type of the default feature of this feature.

        Parameters
        ----------
        feature_type: Union[FeatureType, str]
            Feature type to update
        """
        feature_type_value = FeatureType(feature_type).value
        self.update(
            update_payload={"feature_type": feature_type_value},
            allow_update_local=False,
        )
