"""
FeatureByte specific BaseModel
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Type, TypeVar, Union

import json
import re
from datetime import datetime
from pathlib import Path

import numpy as np
from bson.errors import InvalidId
from bson.objectid import ObjectId
from pydantic import BaseModel, Field, StrictStr, root_validator, validator
from pydantic.errors import DictError, PydanticTypeError
from pymongo.operations import IndexModel

from featurebyte.enum import StrEnum

Model = TypeVar("Model", bound="FeatureByteBaseModel")

DEFAULT_CATALOG_ID = ObjectId("23eda344d0313fb925f7883a")
ACTIVE_CATALOG_ID: Optional[ObjectId] = None
CAMEL_CASE_TO_SNAKE_CASE_PATTERN = re.compile("((?!^)(?<!_)[A-Z][a-z]+|(?<=[a-z0-9])[A-Z])")


if TYPE_CHECKING:
    NameStr = str
else:

    class NameStr(StrictStr):
        """
        Name string type
        """

        min_length = 0
        max_length = 255


def get_active_catalog_id() -> Optional[ObjectId]:
    """
    Get active catalog id

    Returns
    -------
    Optional[ObjectId]
    """
    return ACTIVE_CATALOG_ID


def activate_catalog(catalog_id: Optional[ObjectId]) -> None:
    """
    Set active catalog

    Parameters
    ----------
    catalog_id: Optional[ObjectId]
        Catalog ID to set as active, or None to set no active catalog
    """
    global ACTIVE_CATALOG_ID  # pylint: disable=global-statement
    ACTIVE_CATALOG_ID = catalog_id


class PydanticObjectId(ObjectId):
    """
    Pydantic-compatible Object Id type
    """

    @classmethod
    def __get_validators__(cls) -> Any:
        yield cls.validate

    @classmethod
    def validate(cls, value: Union[str, bytes, ObjectId]) -> ObjectId:
        """
        Validate value is ObjectID type and convert Object ID string to ObjectID

        Parameters
        ----------
        value: Union[str, bytes, ObjectId]
            value to validate / convert

        Returns
        -------
        ObjectId
            Converted / validated value

        Raises
        ------
        TypeError
            Input string is not a valid ObjectId value
        """
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        try:
            return ObjectId(value)
        except InvalidId as exc:
            raise TypeError("Id must be of type PydanticObjectId") from exc

    @classmethod
    def __modify_schema__(cls, field_schema: Dict[str, Any]) -> Any:
        field_schema.update(type="string")


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

    def json_dict(self, **kwargs: Any) -> dict[str, Any]:
        """
        Convert model into json dictionary that can be used as request payload

        Parameters
        ----------
        kwargs: Any
            Additional parameter(s) passed to pydantic json method

        Returns
        -------
        dict[str, Any]
        """
        output: dict[str, Any] = json.loads(self.json(by_alias=True, **kwargs))
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
        json_encoders = {
            ObjectId: str,
            np.ndarray: lambda arr: arr.tolist(),
        }


class UniqueConstraintResolutionSignature(StrEnum):
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
    def get_existing_object_type(cls) -> set[UniqueConstraintResolutionSignature]:
        """
        Get existing object resolution type

        Returns
        -------
        set[UniqueConstraintResolutionSignature]
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
    extra_query_params: Optional[Dict[str, Any]] = Field(default=None)


class ReferenceInfo(FeatureByteBaseModel):
    """
    Reference information for a document
    """

    asset_name: str
    document_id: PydanticObjectId

    @property
    def collection_name(self) -> str:
        """
        Collection name of the reference document

        Returns
        -------
        str
        """
        return CAMEL_CASE_TO_SNAKE_CASE_PATTERN.sub(r"_\1", self.asset_name).lower()


class FeatureByteBaseDocumentModel(FeatureByteBaseModel):
    """
    FeatureByte specific BaseDocumentModel

    id: PydanticObjectId
        Identity value of the child class document model object
    user_id: PydanticObjectId
        Identity value of the user created this document
    name: Optional[StrictStr]
        Name of the child class document model object (value is None when name has not been set)
    created_at: Optional[datetime]
        Record creation datetime when the document get stored at the persistent
    updated_at: Optional[datetime]
        Record update datetime when the document get updated at the persistent
    block_modifications_by: List[ReferenceInfo]
        List of reference information that blocks modifications to the document
    """

    id: PydanticObjectId = Field(
        default_factory=ObjectId, alias="_id", allow_mutation=False, description="Record identifier"
    )
    user_id: Optional[PydanticObjectId] = Field(
        default=None, allow_mutation=False, description="User identifier"
    )
    name: Optional[NameStr] = Field(description="Record name")
    created_at: Optional[datetime] = Field(
        default=None, allow_mutation=False, description="Record creation time"
    )
    updated_at: Optional[datetime] = Field(
        default=None, allow_mutation=False, description="Record last updated time"
    )
    block_modification_by: List[ReferenceInfo] = Field(
        default_factory=list,
        allow_mutation=False,
        description="List of reference information that blocks modifications to the document",
    )
    description: Optional[StrictStr] = Field(default=None, description="Record description")

    @validator("id", pre=True)
    @classmethod
    def validate_id(cls, value: Any) -> Any:
        """
        Base document model id field validator. If the value of the ID is None, generate a valid ID.

        Parameters
        ----------
        value: Any
            Input value to id field

        Returns
        -------
        Output id value
        """
        if value is None:
            return ObjectId()
        return value

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

    @classmethod
    def _get_remote_attribute_paths(cls, document_dict: Dict[str, Any]) -> List[Path]:
        """
        Get remote attribute paths for a document

        Parameters
        ----------
        document_dict: Dict[str, Any]
            Dict representation of the document

        Returns
        -------
        List[Path]
        """
        _ = document_dict
        return []

    class Settings:
        """
        MongoDB settings
        """

        collection_name: str
        unique_constraints: List[UniqueValuesConstraint]

        indexes: List[Union[IndexModel, List[Tuple[str, str]]]] = [
            IndexModel("user_id"),
            IndexModel("name"),
            IndexModel("created_at"),
            IndexModel("updated_at"),
        ]

        auditable: bool = True


class VersionIdentifier(BaseModel):
    """
    VersionIdentifier model

    name: str
        Version name like `V220917`
    suffix: Optional[int]
        Suffix integer to differentiate multiple version with the same name
    """

    name: str
    suffix: Optional[int] = Field(default=None)

    def to_str(self) -> str:
        """
        Convert the VersionIdentifier object into string

        Returns
        -------
        version string
        """
        if self.suffix:
            return f"{self.name}_{self.suffix}"
        return self.name

    @classmethod
    def from_str(cls, version: str) -> VersionIdentifier:
        """
        Convert the version string into VersionIdentifier object

        Parameters
        ----------
        version: str
            Version string

        Returns
        -------
        VersionIdentifier object
        """
        version_identifier = cls(name=version)
        if "_" in version:
            name, suffix = version.split("_", 1)
            version_identifier = cls(name=name, suffix=suffix)
        return version_identifier


class FeatureByteCatalogBaseDocumentModel(FeatureByteBaseDocumentModel):
    """
    FeatureByte Catalog-specific BaseDocumentModel
    """

    catalog_id: PydanticObjectId

    @root_validator(pre=True)
    @classmethod
    def _validate_catalog_id(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        catalog_id = values.get("catalog_id")
        if catalog_id is None:
            values["catalog_id"] = DEFAULT_CATALOG_ID
        return values

    class Settings(FeatureByteBaseDocumentModel.Settings):
        """
        MongoDB settings
        """

        indexes = FeatureByteBaseDocumentModel.Settings.indexes + [
            IndexModel("catalog_id"),
        ]


class User(FeatureByteBaseModel):
    """
    Skeleton user class to provide static user
    """

    id: Optional[PydanticObjectId] = Field(default=None)
