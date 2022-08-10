"""
FeatureByte specific BaseModel
"""
# pylint: disable=too-few-public-methods
from __future__ import annotations

from typing import Any, Dict, List, Optional, Type, TypeVar

import json
from datetime import datetime
from enum import Enum

from beanie import PydanticObjectId
from bson.objectid import ObjectId
from pydantic import BaseModel, Field, StrictStr
from pydantic.errors import DictError, PydanticTypeError

Model = TypeVar("Model", bound="FeatureByteBaseModel")


class FeatureByteTypeError(PydanticTypeError):
    """
    FeatureByte specific type error
    """

    object_type: str
    msg_template = "value is not a valid {object_type} type"


class FeatureByteBaseModel(BaseModel):
    """
    FeatureByte specific BaseModel
    """

    @classmethod
    def validate(cls: Type[Model], value: Any) -> Model:
        try:
            return super().validate(value)
        except DictError as exc:
            raise FeatureByteTypeError(object_type=cls.__name__) from exc

    def json_dict(self) -> dict[str, Any]:
        """
        Convert model into json dictionary that can be used as request payload

        Returns
        -------
        dict[str, Any]
        """
        output: dict[str, Any] = json.loads(self.json(by_alias=True))
        return output

    class Config:
        """
        Configurations for FeatureByteBaseModel
        """

        # With `validate_assignment` flag enabled, pydantic model runs validation check during attribute assignment.
        # This also enables the feature to make attribute immutable by using Field(allow_mutation=False).
        validate_assignment = True

        # With `use_enum_values` flag enabled, pydantic model converts the enum attribute to the enum's value when
        # storing the value in the model (`<model>.<enum_attribute>` should return enum's value rather than enum type).
        use_enum_values = True

        # With this mapping, `ObjectId` type attribute is converted to string during json serialization.
        json_encoders = {ObjectId: str}


class UniqueConstraintResolutionSignature(str, Enum):
    """
    UniqueConstraintResolutionSignature
    """

    GET_BY_ID = "get_by_id"
    GET_NAME = "get_name"
    GET_NAME_VERSION = "get_name_version"
    RENAME = "rename"

    @staticmethod
    def _get_by_id(class_name: str, document: dict[str, Any]) -> str:
        return f'{class_name}.get_by_id(id="{document["_id"]}")'

    @staticmethod
    def _get_name(class_name: str, document: dict[str, Any]) -> str:
        return f'{class_name}.get(name="{document["name"]}")'

    @staticmethod
    def _get_name_version(class_name: str, document: dict[str, Any]) -> str:
        return f'{class_name}.get(name="{document["name"]}", version="{document["version"]}")'

    @classmethod
    def get_existing_object_type(cls):
        """
        Get existing object resolution type
        """
        return {cls.GET_BY_ID, cls.GET_NAME, cls.GET_NAME_VERSION}

    @classmethod
    def get_resolution_statement(
        cls,
        resolution_signature: UniqueConstraintResolutionSignature,
        class_name: str,
        document: dict[str, Any],
    ) -> str:
        """
        Retrieve conflict resolution statement used in conflict error message

        Parameters
        ----------
        resolution_signature: UniqueConstraintResolutionSignature
            Type of resolution used to control the function call used in resolution statement
        class_name: str
            Class used to trigger the resolution statement
        document: dict[str, Any]
            Conflict document used to extract information for resolution statement

        Returns
        -------
        str
            Resolution statement used in conflict error message
        """
        return {
            cls.GET_BY_ID: cls._get_by_id,
            cls.GET_NAME: cls._get_name,
            cls.GET_NAME_VERSION: cls._get_name_version,
        }[resolution_signature](class_name=class_name, document=document)


class UniqueValuesConstraint(FeatureByteBaseModel):
    """
    Unique values constraints for fields in a collection
    """

    fields: List[str]
    conflict_fields_signature: Dict[str, Any]
    resolution_signature: Optional[UniqueConstraintResolutionSignature]


class FeatureByteBaseDocumentModel(FeatureByteBaseModel):
    """
    FeatureByte specific BaseDocumentModel

    id: PydanticObjectId
        Identity value of the child class document model object
    name: Optional[StrictStr]
        Name of the child class document model object (value is None when name has not been set)
    created_at: Optional[datetime]
        Record creation datetime when the document get stored at the persistent
    """

    id: PydanticObjectId = Field(default_factory=ObjectId, alias="_id", allow_mutation=False)
    user_id: Optional[PydanticObjectId] = Field(default=None, allow_mutation=False)
    name: Optional[StrictStr]
    created_at: Optional[datetime] = Field(default=None, allow_mutation=False)
    updated_at: Optional[datetime] = Field(default=None, allow_mutation=False)

    @classmethod
    def collection_name(cls) -> str:
        """
        Retrieve collection name
        Returns
        -------
        str
            collection name
        """
        return cls.Settings.collection_name

    @classmethod
    def unique_constraints(cls) -> List[UniqueValuesConstraint]:
        """
        Retrieve unique_constraints
        Returns
        -------
        List[UniqueValuesConstraint]
            collection name
        """
        return cls.Settings.unique_constraints

    class Settings:
        """
        MongoDB settings
        """

        collection_name: str
        unique_constraints: List[UniqueValuesConstraint]
