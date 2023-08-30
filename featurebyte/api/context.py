"""
Context module
"""
from typing import List

from typeguard import typechecked

from featurebyte.api.entity import Entity
from featurebyte.api.savable_api_object import SavableApiObject
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.context import ContextModel
from featurebyte.schema.context import ContextUpdate


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

    @property
    def entities(self) -> List[Entity]:
        """
        Returns the history of the entity name of the Entity object.

        Returns
        -------
        list[Entity]
            list of Entity objects.

        Examples
        --------
        Get the enitity objects of the Context object:

        >>> context_1 = catalog.get_context("context_1")  # doctest: +SKIP
        >>> context_1.entities  # doctest: +SKIP
        """
        entities = []
        for entity_id in self.entity_ids:
            entities.append(Entity.get_by_id(entity_id))
        return entities

    @classmethod
    @typechecked
    def create(
        cls,
        name: str,
        entity_names: List[str],
    ) -> "Context":
        """
        Create a new Context.

        Parameters
        ----------
        name: str
            Name of the UseCase.
        entity_names: List[str]
            List of entity names.

        Returns
        -------
        Context
            The newly created Context.

        Examples
        --------
        >>> fb.Context.create(  # doctest: +SKIP
        ...     name="context_1",
        ...     entity_names=entity_names,
        ... )
        >>> context_1 = catalog.get_context("context_1")  # doctest: +SKIP
        """
        entity_ids = []
        for entity_name in entity_names:
            entity_ids.append(Entity.get(entity_name).id)

        context = Context(name=name, entity_ids=entity_ids)
        context.save()
        return context
