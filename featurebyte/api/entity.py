"""
Entity class
"""
from __future__ import annotations

from typing import Any, List

from http import HTTPStatus

from bson import ObjectId
from pydantic import Field
from typeguard import typechecked

from featurebyte.api.api_object import SavableApiObject
from featurebyte.api.api_object_util import NameAttributeUpdatableMixin
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.config import Configurations
from featurebyte.exception import RecordRetrievalException, RecordUpdateException
from featurebyte.models.entity import EntityModel, ParentEntity
from featurebyte.schema.entity import EntityCreate, EntityUpdate


class Entity(NameAttributeUpdatableMixin, SavableApiObject):
    """
    Entity class to represent an entity in FeatureByte.

    An entity is a real-world object or concept that is represented by fields in the source tables.
    Entities facilitate automatic table join definitions, serve as the unit of analysis for feature engineering,
    and aid in organizing features, feature lists, and use cases.
    """

    # documentation metadata
    __fbautodoc__ = FBAutoDoc(proxy_class="featurebyte.Entity")

    # class variables
    _route = "/entity"
    _update_schema_class = EntityUpdate
    _list_schema = EntityModel
    _get_schema = EntityModel
    _list_fields = ["name", "serving_names", "created_at"]

    # pydantic instance variable (internal use)
    internal_serving_names: List[str] = Field(alias="serving_names")

    def _get_create_payload(self) -> dict[str, Any]:
        data = EntityCreate(serving_name=self.serving_name, **self.json_dict())
        return data.json_dict()

    @property
    def serving_names(self) -> List[str]:
        """
        Lists the serving names of an Entity object.

        An entity's serving name is the name of the unique identifier that is used to identify the entity during a
        preview or serving request. Typically, the serving name for an entity is the name of the primary key (or
        natural key) of the table that represents the entity. For convenience, an entity can have multiple serving
        names but the unique identifier should remain unique.

        For example, the serving names of a Customer entity could be 'CustomerID' and 'CustID'.

        Returns
        -------
        List[str]
            Serving names of the entity.

        Examples
        --------
        >>> entity = catalog.get_entity("grocerycustomer")
        >>> entity.serving_names
        ['GROCERYCUSTOMERGUID']

        See Also
        --------
        - [Feature.preview](/reference/featurebyte.api.feature.Feature.preview/)
        - [FeatureList.preview](/reference/featurebyte.api.feature_list.FeatureList.preview/)
        - [FeatureList.get_historical_features](/reference/featurebyte.api.feature_list.FeatureList.get_historical_features/)
        - [FeatureList.get_online_serving_code](/reference/featurebyte.api.feature_list.FeatureList.get_online_serving_code/)
        """
        try:
            return self.cached_model.serving_names
        except RecordRetrievalException:
            return self.internal_serving_names

    @property
    def serving_name(self) -> str:
        """
        First serving name of the entity serving names. An entity's serving names is the name of the unique
        identifier that is used to identify the entity during a preview or serving request. Typically, the
        serving name for an entity is the name of the primary key (or natural key) of the table that
        represents the entity.

        Returns
        -------
        str
            First serving name of the entity serving names.
        """
        return self.serving_names[0]

    @property
    def parents(self) -> List[ParentEntity]:
        """
        Get the list of parent entities. A parent-child relationship is a hierarchical connection that links one
        entity (the child) to another entity (the parent). Each child entity key value can have only one parent
        entity key value, but a parent entity key value can have multiple child entity key values.

        The parent-child relationship is automatically established when the primary key (or natural key in the
        context of a SCD table) identifies one entity. This entity is the child entity. Other entities that are
        referenced in the table are identified as the parent entities.

        Returns
        -------
        List[ParentEntity]
            List of parent entities.

        Examples
        --------

        >>> entity = catalog.get_entity("grocerycustomer")
        >>> entity.parents  # doctest: +ELLIPSIS
        [ParentEntity(id=ObjectId(...), table_type='scd_table', table_id=ObjectId(...))]

        See Also
        --------
        - [TableColumn.as_entity](/reference/featurebyte.api.base_table.TableColumn.as_entity/)
        """
        return self.cached_model.parents

    @property
    def ancestor_ids(self) -> List[ObjectId]:
        """
        Get the list of ancestor entity ids. An ancestor entity is an entity that is a parent of the current entity,
        or a parent of a parent, and so on.

        Returns
        -------
        List[ObjectId]
            List of ancestor entity ids.
        """
        return self.cached_model.ancestor_ids

    @typechecked
    def update_name(self, name: str) -> None:
        """
        Updates the name of the Entity object.

        Parameters
        ----------
        name: str
            New entity name.

        Examples
        --------
        Update entity name:

        >>> entity = catalog.get_entity(name="grocerycustomer")
        >>> entity.update_name(name="grocery_customer")
        >>> entity.name
        'grocery_customer'
        >>> entity.update_name(name="grocerycustomer")
        >>> entity.name
        'grocerycustomer'

        Show the history of the entity name:

        >>> entity.name_history  # doctest: +ELLIPSIS
        [{'created_at': ..., 'name': 'grocerycustomer'},
        {'created_at': ..., 'name': 'grocery_customer'},
        {'created_at': ..., 'name': 'grocerycustomer'}...]

        See Also
        --------
        - [Entity.name](/reference/featurebyte.api.entity.Entity.name/)
        - [Entity.name_history](/reference/featurebyte.api.entity.Entity.name_history/)
        - [TableColumn.as_entity](/reference/featurebyte.api.base_table.TableColumn.as_entity/)
        """
        self.update(update_payload={"name": name}, allow_update_local=True)

    @property
    def name_history(self) -> list[dict[str, Any]]:
        """
        Get the history of the entity name. Entity name is used to associate a table column with the entity.

        Returns
        -------
        list[dict[str, Any]]
            History of the entity name.

        Examples
        --------
        Get the history of the entity name:

        >>> entity = catalog.get_entity(name="groceryproduct")
        >>> entity.name_history  # doctest: +ELLIPSIS
        [{'created_at': ..., 'name': 'groceryproduct'}]

        See Also
        --------
        - [Entity.name](/reference/featurebyte.api.entity.Entity.name/)
        - [Entity.update_name](/reference/featurebyte.api.entity.Entity.update_name/)
        - [TableColumn.as_entity](/reference/featurebyte.api.base_table.TableColumn.as_entity/)
        """
        return self._get_audit_history(field_name="name")

    @classmethod
    def create(cls, name: str, serving_names: List[str]) -> Entity:
        """
        Create a new entity.

        Parameters
        ----------
        name: str
            Name of the entity.
        serving_names: List[str]
            Names of the serving columns.

        Returns
        -------
        Entity
            The newly created entity.

        See Also
        --------
        - [Entity.get_or_create](/reference/featurebyte.api.entity.Entity.get_or_create/): Entity.get_or_create
        """
        entity = Entity(name=name, serving_names=serving_names)
        entity.save()
        return entity

    @classmethod
    def get_or_create(
        cls,
        name: str,
        serving_names: List[str],
    ) -> Entity:
        """
        Get entity, or create one if we cannot find an entity with the given name.

        Parameters
        ----------
        name: str
            Name of the entity.
        serving_names: List[str]
            Names of the serving columns.

        Returns
        -------
        Entity
            The newly created entity.

        Examples
        --------
        >>> entity = fb.Entity.get_or_create(
        ...     name="grocerycustomer",
        ...     serving_names=["GROCERYCUSTOMERGUID"]
        ... )
        >>> entity.name
        'grocerycustomer'

        See Also
        --------
        - [Catalog.create_entity](/reference/featurebyte.api.catalog.Catalog.create_entity/): Catalog.create_entity
        """
        try:
            return Entity.get(name=name)
        except RecordRetrievalException:
            return Entity.create(name=name, serving_names=serving_names)

    @typechecked
    def add_parent(self, parent_entity_name: str, relation_dataset_name: str) -> None:
        """
        Adds other entity as the parent of this current entity.

        Parameters
        ----------
        parent_entity_name: str
            the entity that will become the parent of this entity.
        relation_dataset_name: str
            the name of the dataset that the parent is from

        Raises
        ------
        RecordUpdateException
            error updating record
        """

        client = Configurations().get_client()
        response = client.get("/table", params={"name": relation_dataset_name})
        assert response.status_code == HTTPStatus.OK
        json_response = response.json()
        data_response = json_response["data"]
        assert len(data_response) == 1

        parent_entity = Entity.get(parent_entity_name)
        data = ParentEntity(
            table_type=data_response[0]["type"],
            table_id=data_response[0]["_id"],
            id=parent_entity.id,
        )

        post_response = client.post(
            f"{self._route}/{self.id}/parent",
            json=data.json_dict(),
        )
        if post_response.status_code != HTTPStatus.CREATED:
            raise RecordUpdateException(post_response)

    @typechecked
    def remove_parent(self, parent_entity_name: str) -> None:
        """
        Removes other entity as the parent of this current entity.

        Parameters
        ----------
        parent_entity_name: str
            the other entity that we want to remove as a parent.

        Raises
        ------
        RecordUpdateException
            error updating record
        """

        client = Configurations().get_client()
        parent_entity = Entity.get(parent_entity_name)
        post_response = client.delete(
            f"{self._route}/{self.id}/parent/{parent_entity.id}",
        )
        if post_response.status_code != HTTPStatus.OK:
            raise RecordUpdateException(post_response)
