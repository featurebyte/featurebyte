"""
FeatureByte specific BaseModel
"""
# pylint: disable=too-few-public-methods
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple, Type, TypeVar

import json
from datetime import datetime

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


class UniqueValuesConstraint(FeatureByteBaseModel):
    """
    Unique values constraints for fields in a collection
    """

    fields: Tuple[str]
    conflict_fields_signature: Dict[str, Any]
    resolution_signature: str


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
