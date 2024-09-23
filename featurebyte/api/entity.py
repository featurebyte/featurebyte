"""
Entity class
"""

from __future__ import annotations

from typing import Any, ClassVar, Dict, List, Optional

from bson import ObjectId
from pydantic import Field
from typeguard import typechecked

from featurebyte.api.api_object_util import NameAttributeUpdatableMixin
from featurebyte.api.savable_api_object import DeletableApiObject, SavableApiObject
from featurebyte.common.doc_util import FBAutoDoc
from featurebyte.enum import DBVarType
from featurebyte.exception import RecordRetrievalException
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.entity import EntityModel, ParentEntity
from featurebyte.schema.entity import EntityCreate, EntityUpdate


class Entity(NameAttributeUpdatableMixin, SavableApiObject, DeletableApiObject):
    """
    Entity class to represent an entity in FeatureByte.

    An entity is a real-world object or concept that is represented by fields in the source tables.
    Entities facilitate automatic table join definitions, serve as the unit of analysis for feature engineering,
    and aid in organizing features, feature lists, and use cases.
    """

    # class variables
    __fbautodoc__: ClassVar[FBAutoDoc] = FBAutoDoc(proxy_class="featurebyte.Entity")
    _route: ClassVar[str] = "/entity"
    _update_schema_class: ClassVar[Any] = EntityUpdate
    _list_schema: ClassVar[Any] = EntityModel
    _get_schema: ClassVar[Any] = EntityModel
    _list_fields: ClassVar[List[str]] = ["name", "serving_names", "created_at"]

    # pydantic instance variable (internal use)
    internal_serving_names: List[str] = Field(alias="serving_names")

    def _get_create_payload(self) -> dict[str, Any]:
        data = EntityCreate(serving_name=self.serving_name, **self.model_dump(by_alias=True))
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
        - [FeatureList.compute_historical_features](/reference/featurebyte.api.feature_list.FeatureList.compute_historical_features/)
        - [Deployment.get_online_serving_code](/reference/featurebyte.api.deployment.Deployment.get_online_serving_code/)
        """
        try:
            return self.cached_model.serving_names
        except RecordRetrievalException:
            return self.internal_serving_names

    @property
    def catalog_id(self) -> PydanticObjectId:
        """
        Returns the unique identifier (ID) of the Catalog that is associated with the Entity object.

        Returns
        -------
        PydanticObjectId
            Catalog ID of the entity.

        See Also
        --------
        - [Catalog](/reference/featurebyte.api.catalog.Catalog)
        """
        return self.cached_model.catalog_id

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
        Displays a list of the entity parents along with their corresponding IDs and the table ID of the relation
        table that establishes the parent-child relationship.

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

    @property
    def dtype(self) -> Optional[DBVarType]:
        """
        Get the data type of the entity.

        Returns
        -------
        Optional[str]
            Data type of the entity.
        """
        return self.cached_model.dtype

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
        Returns the history of the entity name of the Entity object.

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
    @typechecked
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
    @typechecked
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
        ...     name="grocerycustomer", serving_names=["GROCERYCUSTOMERGUID"]
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

    def info(self, verbose: bool = False) -> Dict[str, Any]:
        """
        Returns a dictionary that summarizes the essential information of an Entity object. The dictionary contains
        the following keys:

        - `name`: The name of the Entity object.
        - `created_at`: The timestamp indicating when the Entity object was created.
        - `updated_at`: The timestamp indicating when the Entity object was last updated.
        - `serving_names`: The list of approved labels used to identify the unique identifier for the
            Entity during preview or serving requests.
        - `catalog_name`: The name of the catalog that the Entity belongs to.

        Parameters
        ----------
        verbose: bool
            Control verbose level of the summary.

        Returns
        -------
        Dict[str, Any]
            Key-value mapping of properties of the object.

        Examples
        --------
        >>> entity = catalog.get_entity("grocerycustomer")
        >>> entity.info()  # doctest: +SKIP
        """
        return super().info(verbose)

    def delete(self) -> None:
        """
        Delete the entity from the persistent data store. The entity can only be deleted if

        - the entity is not referenced by any other feature or target objects
        - the entity is not referenced by any other context objects
        - the entity is not referenced by any other table objects

        Examples
        --------
        >>> entity = catalog.get_entity("grocerycustomer")
        >>> entity.delete()  # doctest: +SKIP
        """
        super()._delete()
