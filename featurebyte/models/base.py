"""
FeatureByte specific BaseModel
"""
from __future__ import annotations

from typing import Any, Type, TypeVar

from bson.objectid import ObjectId
from pydantic import BaseModel
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

    class Config:
        """
        Configurations for FeatureByteBaseModel
        """

        # pylint: disable=too-few-public-methods

        use_enum_values = True
        json_encoders = {ObjectId: str}
