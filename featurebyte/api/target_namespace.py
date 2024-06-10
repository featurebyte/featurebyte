"""
Feature Namespace module.
"""

from __future__ import annotations

from typing import Any, ClassVar, List, Optional

from pydantic import Field

from featurebyte.api.api_handler.base import ListHandler
from featurebyte.api.api_handler.target_namespace import TargetNamespaceListHandler
from featurebyte.api.api_object_util import ForeignKeyMapping
from featurebyte.api.entity import Entity
from featurebyte.api.feature_or_target_namespace_mixin import FeatureOrTargetNamespaceMixin
from featurebyte.api.savable_api_object import DeletableApiObject, SavableApiObject
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import DBVarType
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.target_namespace import TargetNamespaceModel
from featurebyte.schema.target_namespace import TargetNamespaceUpdate


class TargetNamespace(FeatureOrTargetNamespaceMixin, DeletableApiObject, SavableApiObject):
    """
    TargetNamespace represents a Target set, in which all the targets in the set have the same name. The different
    elements typically refer to different versions of a Target.
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.TargetNamespace")
    _route: ClassVar[str] = "/target_namespace"
    _update_schema_class: ClassVar[Any] = TargetNamespaceUpdate
    _list_schema: ClassVar[Any] = TargetNamespaceModel
    _get_schema: ClassVar[Any] = TargetNamespaceModel
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
    internal_window: Optional[str] = Field(alias="window")
    internal_dtype: DBVarType = Field(alias="dtype")

    @classmethod
    def create(
        cls, name: str, primary_entity: List[str], dtype: DBVarType, window: Optional[str] = None
    ) -> TargetNamespace:
        """
        Create a new TargetNamespace.

        Parameters
        ----------
        name: str
            Name of the TargetNamespace
        primary_entity: List[str]
            List of entities.
        dtype: DBVarType
            Data type of the TargetNamespace
        window: Optional[str]
            Window of the TargetNamespace

        Returns
        -------
        TargetNamespace
            The created TargetNamespace

        Examples
        --------
        >>> target_namespace = fb.TargetNamespace.create(  # doctest: +SKIP
        ...     name="amount_7d_target",
        ...     window="7d",
        ...     dtype=DBVarType.FLOAT,
        ...     primary_entity=["customer"]
        ... )
        """
        entity_ids = [Entity.get(entity_name).id for entity_name in primary_entity]
        target_namespace = TargetNamespace(
            name=name, entity_ids=entity_ids, dtype=dtype, window=window
        )
        target_namespace.save()
        return target_namespace

    @property
    def dtype(self) -> DBVarType:
        """
        Database variable type of the target namespace.

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
        Window of the target namespace.

        Returns
        -------
        str
        """
        try:
            return self.cached_model.window
        except RecordRetrievalException:
            return self.internal_window

    @property
    def target_ids(self) -> List[PydanticObjectId]:
        """
        List of target IDs from the same target namespace

        Returns
        -------
        List[PydanticObjectId]
        """
        return self.cached_model.target_ids

    @property
    def default_target_id(self) -> PydanticObjectId:
        """
        Default target ID of this target namespace

        Returns
        -------
        PydanticObjectId
        """
        return self.cached_model.default_target_id

    @classmethod
    def _list_handler(cls) -> ListHandler:
        return TargetNamespaceListHandler(
            route=cls._route,
            list_schema=cls._list_schema,
            list_fields=cls._list_fields,
            list_foreign_keys=cls._list_foreign_keys,
        )

    def delete(self) -> None:
        """
        Delete a target namespace from the persistent data store. A target namespace can only be deleted
        from the persistent data store if

        - the target namespace is not used in any use case
        - the target namespace is not used in any target

        Examples
        --------
        >>> target_namespace = fb.TargetNamespace.create(  # doctest: +SKIP
        ...     name="amount_7d_target",
        ...     window="7d",
        ...     dtype=DBVarType.FLOAT,
        ...     primary_entity=["customer"]
        ... )
        >>> target_namespace.delete()  # doctest: +SKIP
        """
        self._delete()
