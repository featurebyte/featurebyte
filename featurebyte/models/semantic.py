"""
This module contains Semantic related models
"""
from typing import List

import pymongo

from featurebyte.models.base import UniqueValuesConstraint
from featurebyte.models.relationship import Relationship


class SemanticModel(Relationship):
    """
    Model for Semantic

    id: PydanticObjectId
        Semantic id of the object
    name: SemanticName
        Semantic name
    ancestor_ids: List[PydanticObjectId]
        Ancestor semantics of this semantic
    parents: List[Parent]
        Parent semantics of this semantic
    created_at: datetime
        Datetime when the Entity object was first saved or published
    updated_at: datetime
        Datetime when the Entity object was last updated
    """

    class Settings:
        """
        MongoDB settings
        """

        collection_name: str = "semantic"
        unique_constraints: List[UniqueValuesConstraint] = [
            UniqueValuesConstraint(
                fields=("_id",),
                conflict_fields_signature={"id": ["_id"]},
                resolution_signature=None,
            ),
            UniqueValuesConstraint(
                fields=("name",),
                conflict_fields_signature={"name": ["name"]},
                resolution_signature=None,
            ),
        ]

        indexes = [
            pymongo.operations.IndexModel("user_id"),
            pymongo.operations.IndexModel("name"),
            pymongo.operations.IndexModel("created_at"),
            pymongo.operations.IndexModel("updated_at"),
            pymongo.operations.IndexModel("parents"),
            pymongo.operations.IndexModel("ancestor_ids"),
            [
                ("name", pymongo.TEXT),
            ],
        ]
