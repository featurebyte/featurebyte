"""
FeatureByte specific BaseModel
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar, Union

from bson import ObjectId
from bson.errors import InvalidId
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictStr,
    StringConstraints,
    field_validator,
    model_validator,
)
from pydantic_core import core_schema
from pymongo.operations import IndexModel
from typing_extensions import Annotated

from featurebyte.common.model_util import get_version
from featurebyte.enum import StrEnum

Model = TypeVar("Model", bound="FeatureByteBaseModel")

DEFAULT_CATALOG_ID = ObjectId("23eda344d0313fb925f7883a")
ACTIVE_CATALOG_ID: Optional[ObjectId] = None
CAMEL_CASE_TO_SNAKE_CASE_PATTERN = re.compile("((?!^)(?<!_)[A-Z][a-z]+|(?<=[a-z0-9])[A-Z])")


# SQL object name is limited to 255 characters:
# - Snowflake: https://docs.snowflake.com/en/sql-reference/identifiers
# - DataBricks: https://docs.databricks.com/en/sql/language-manual/sql-ref-names.html
# Give 255 characters as the maximum length for the name, we preserve 25 characters
# for featurebyte internal use, like
# - version name: _VYYMMDD_{suffix}
# - component name: _part{suffix}
# - catalog prefix: cat{suffix}_
NameStr = Annotated[str, StringConstraints(min_length=0, max_length=230)]


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
    global ACTIVE_CATALOG_ID
    ACTIVE_CATALOG_ID = catalog_id


class _ObjectIdPydanticAnnotation:
    # Based on https://docs.pydantic.dev/latest/usage/types/custom/#handling-third-party-types.

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        _source_type: Any,
        _handler: Callable[[Any], core_schema.CoreSchema],
    ) -> core_schema.CoreSchema:
        def validate_from_str(input_value: str) -> ObjectId:
            try:
                return ObjectId(input_value)
            except InvalidId as exc:
                raise ValueError(f"Invalid ObjectId: {input_value}") from exc

        return core_schema.union_schema(
            [
                # check if it's an instance first before doing any further work
                core_schema.is_instance_schema(ObjectId),
                core_schema.no_info_plain_validator_function(validate_from_str),
            ],
            serialization=core_schema.to_string_ser_schema(),
        )


PydanticObjectId = Annotated[ObjectId, _ObjectIdPydanticAnnotation]


class FeatureByteBaseModel(BaseModel):
    """
    FeatureByte specific BaseModel
    """

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
        output: dict[str, Any] = self.model_dump(mode="json", by_alias=True, **kwargs)
        return output

    # pydantic model configuration
    model_config = ConfigDict(
        # with `validate_assignment` flag enabled, pydantic model runs validation check during attribute assignment.
        validate_assignment=True,
        # with `use_enum_values` flag enabled, pydantic model converts the enum attribute to the enum's value when
        # storing the value in the model (`<model>.<enum_attribute>` should return enum's value rather than enum type).
        use_enum_values=True,
        # whether arbitrary types are allowed for field types.
        arbitrary_types_allowed=True,
        # Protected_namespaces warnings are disabled to avoid warnings.
        # If we create a field_name with an actual conflict with pydantic's internal fields, we will get an error
        # @See: https://docs.pydantic.dev/latest/api/config/#pydantic.config.ConfigDict.protected_namespaces
        protected_namespaces=(),
    )


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
    resolution_signature: Optional[UniqueConstraintResolutionSignature] = Field(default=None)
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
        default_factory=ObjectId, alias="_id", frozen=True, description="Record identifier"
    )
    user_id: Optional[PydanticObjectId] = Field(
        default=None, frozen=True, description="User identifier"
    )
    name: Optional[NameStr] = Field(default=None, description="Record name")
    created_at: Optional[datetime] = Field(
        default=None, frozen=True, description="Record creation time"
    )
    updated_at: Optional[datetime] = Field(
        default=None, frozen=True, description="Record last updated time"
    )
    block_modification_by: List[ReferenceInfo] = Field(
        default_factory=list,
        frozen=True,
        description="List of reference information that blocks modifications to the document",
    )
    description: Optional[StrictStr] = Field(default=None, description="Record description")
    is_deleted: bool = Field(
        default=False,
        description="Flag to indicate if the record is deleted.",
    )

    @field_validator("id", mode="before")
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

    @classmethod
    def create(cls) -> "VersionIdentifier":
        """
        Create a new VersionIdentifier object

        Returns
        -------
        VersionIdentifier object
        """
        return cls(name=get_version())

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

    @model_validator(mode="before")
    @classmethod
    def _validate_catalog_id(cls, values: Any) -> Any:
        if isinstance(values, BaseModel):
            values = values.model_dump(by_alias=True)

        catalog_id = values.get("catalog_id")
        if catalog_id is None:
            values["catalog_id"] = DEFAULT_CATALOG_ID
        return values

    @property
    def remote_storage_paths(self) -> list[Path]:
        """
        Get remote storage paths

        Returns
        -------
        list[Path]
        """
        return self._get_remote_attribute_paths(self.model_dump(by_alias=True))

    @property
    def warehouse_tables(self) -> list[Any]:
        """
        Get warehouse table details

        Returns
        -------
        list[str]
        """
        return []

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
