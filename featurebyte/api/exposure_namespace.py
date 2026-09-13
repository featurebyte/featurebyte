"""
Exposure Namespace module.
"""

from __future__ import annotations

from typing import Any, ClassVar, List, Optional

from pydantic import Field

from featurebyte.api.api_handler.base import ListHandler
from featurebyte.api.api_handler.exposure_namespace import ExposureNamespaceListHandler
from featurebyte.api.api_object_util import ForeignKeyMapping
from featurebyte.api.entity import Entity
from featurebyte.api.feature_or_target_namespace_mixin import FeatureOrTargetNamespaceMixin
from featurebyte.api.savable_api_object import DeletableApiObject, SavableApiObject
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import DBVarType
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.exposure_namespace import ExposureNamespaceModel
from featurebyte.schema.exposure_namespace import ExposureNamespaceUpdate


class ExposureNamespace(FeatureOrTargetNamespaceMixin, DeletableApiObject, SavableApiObject):
    """
    ExposureNamespace represents an Exposure set, in which all the exposures in the set have the same
    name. The different elements typically refer to different versions of an Exposure.
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.ExposureNamespace")
    _route: ClassVar[str] = "/exposure_namespace"
    _update_schema_class: ClassVar[Any] = ExposureNamespaceUpdate
    _list_schema: ClassVar[Any] = ExposureNamespaceModel
    _get_schema: ClassVar[Any] = ExposureNamespaceModel
    _list_fields: ClassVar[List[str]] = [
        "name",
        "dtype",
        "entities",
        "created_at",
    ]
    _list_foreign_keys: ClassVar[List[ForeignKeyMapping]] = [
        ForeignKeyMapping("entity_ids", Entity, "entities"),
    ]

    # pydantic instance variables
    internal_window: Optional[str] = Field(alias="window", default=None)
    internal_dtype: DBVarType = Field(alias="dtype")

    @classmethod
    def create(
        cls,
        name: str,
        primary_entity: List[str],
        dtype: DBVarType,
        window: Optional[str] = None,
    ) -> ExposureNamespace:
        """
        Create a new ExposureNamespace.

        Parameters
        ----------
        name: str
            Name of the ExposureNamespace
        primary_entity: List[str]
            List of entities.
        dtype: DBVarType
            Data type of the ExposureNamespace
        window: Optional[str]
            Window of the ExposureNamespace

        Returns
        -------
        ExposureNamespace
            The created ExposureNamespace

        Examples
        --------
        >>> exposure_namespace = fb.ExposureNamespace.create(  # doctest: +SKIP
        ...     name="amount_exposure",
        ...     window="7d",
        ...     dtype=DBVarType.FLOAT,
        ...     primary_entity=["customer"],
        ... )
        """
        entity_ids = [Entity.get(entity_name).id for entity_name in primary_entity]
        exposure_namespace = ExposureNamespace(
            name=name,
            entity_ids=entity_ids,
            dtype=dtype,
            window=window,
        )
        exposure_namespace.save()
        return exposure_namespace

    @property
    def dtype(self) -> DBVarType:
        """
        Database variable type of the exposure namespace.

        Returns
        -------
        DBVarType
        """
        try:
            return self.cached_model.dtype
        except RecordRetrievalException:
            return self.internal_dtype

    @property
    def window(self) -> Optional[str]:
        """
        Window of the exposure namespace.

        Returns
        -------
        str
        """
        try:
            return self.cached_model.window
        except RecordRetrievalException:
            return self.internal_window

    @property
    def exposure_ids(self) -> List[PydanticObjectId]:
        """
        List of exposure IDs from the same exposure namespace

        Returns
        -------
        List[PydanticObjectId]
        """
        return self.cached_model.exposure_ids

    @property
    def default_exposure_id(self) -> PydanticObjectId:
        """
        Default exposure ID of this exposure namespace

        Returns
        -------
        PydanticObjectId
        """
        return self.cached_model.default_exposure_id

    @classmethod
    def _list_handler(cls) -> ListHandler:
        return ExposureNamespaceListHandler(
            route=cls._route,
            list_schema=cls._list_schema,
            list_fields=cls._list_fields,
            list_foreign_keys=cls._list_foreign_keys,
        )

    def delete(self) -> None:
        """
        Delete an exposure namespace from the persistent data store. An exposure namespace can only
        be deleted from the persistent data store if

        - the exposure namespace is not used in any use case
        - the exposure namespace is not used in any exposure

        Examples
        --------
        >>> exposure_namespace = fb.ExposureNamespace.create(  # doctest: +SKIP
        ...     name="amount_exposure",
        ...     window="7d",
        ...     dtype=DBVarType.FLOAT,
        ...     primary_entity=["customer"],
        ... )
        >>> exposure_namespace.delete()  # doctest: +SKIP
        """
        self._delete()
