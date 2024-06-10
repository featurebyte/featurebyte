"""
Base feature target namespace
"""

from typing import Any, ClassVar, List

from pydantic import Field

from featurebyte.api.api_object import ApiObject
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_namespace import BaseFeatureNamespaceModel, DefaultVersionMode


class FeatureOrTargetNamespaceMixin(ApiObject):
    """
    Base feature target namespace
    """

    # class variables
    _get_schema: ClassVar[Any] = BaseFeatureNamespaceModel

    # instance variables
    internal_entity_ids: List[PydanticObjectId] = Field(default_factory=list, alias="entity_ids")

    @property
    def default_version_mode(self) -> DefaultVersionMode:
        """
        Default feature namespace version mode of this namespace

        Returns
        -------
        DefaultVersionMode
        """
        return self.cached_model.default_version_mode

    @property
    def entity_ids(self) -> List[PydanticObjectId]:
        """
        List of entity IDs used by the namespace

        Returns
        -------
        List[PydanticObjectId]
        """
        try:
            return self.cached_model.entity_ids
        except RecordRetrievalException:
            return self.internal_entity_ids
