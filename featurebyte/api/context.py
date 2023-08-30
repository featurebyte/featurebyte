"""
UseCase module
"""
from __future__ import annotations

from typing import Any, List

from typeguard import typechecked

from featurebyte.api.savable_api_object import SavableApiObject
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.context import ContextModel
from featurebyte.schema.context import ContextCreate, ContextUpdate


class Context(SavableApiObject):
    """
    Context class to represent a Context in FeatureByte.

    A context defines the circumstances in which features are expected to be served.
    """

    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.Context")

    # class variables
    _route = "/context"
    _list_schema = ContextModel
    _get_schema = ContextModel
    _update_schema_class = ContextUpdate
    _list_fields = [
        "name",
        "entity_ids",
        "description",
    ]

    # pydantic instance variable (public)
    entity_ids: List[PydanticObjectId]

    def _get_create_payload(self) -> dict[str, Any]:
        """
        Get the payload for creating a new Context.

        Returns
        -------
        dict[str, Any]
        """
        data = ContextCreate(**self.dict(by_alias=True))
        return data.json_dict()

    @classmethod
    @typechecked
    def create(
        cls,
        name: str,
        entity_ids: List[PydanticObjectId],
    ) -> Context:
        """
        Create a new Context.

        Parameters
        ----------
        name: str
            Name of the UseCase.
        entity_ids: List[PydanticObjectId]
            List of entity ids.

        Returns
        -------
        Context
            The newly created Context.

        Examples
        --------
        >>> fb.Context.create(  # doctest: +SKIP
        ...     name="context_1",
        ...     entity_ids=entity_ids,
        ... )
        >>> context_1 = catalog.get_context("context_1")  # doctest: +SKIP
        """
        context = Context(name=name, entity_ids=entity_ids)
        context.save()
        return context
