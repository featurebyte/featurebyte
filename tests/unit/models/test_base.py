"""
Tests FeatureByteBaseModel
"""

import json
from typing import List

import pytest
from bson.objectid import ObjectId
from pydantic import StrictStr, ValidationError

from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
    PydanticObjectId,
    ReferenceInfo,
    VersionIdentifier,
)


def test_featurebyte_base_model__error_message():
    """
    Test FeatureBaseModel error message
    """

    class Basket(FeatureByteBaseModel):
        """Basket class"""

        items: List[StrictStr]

    with pytest.raises(ValidationError) as exc:
        Basket(items="hello")
    assert "1 validation error for Basket\nitems\n  Input should be a valid list " in str(exc.value)

    with pytest.raises(ValidationError) as exc:
        Basket(items=[1])
    assert "1 validation error for Basket\nitems.0\n  Input should be a valid string " in str(
        exc.value
    )


def test_featurebyte_base_model__pydantic_model_type_error_message():
    """
    Test FeatureBaseModel error message
    """

    class Items(FeatureByteBaseModel):
        """Items class"""

        data: List[StrictStr]

    class Basket(FeatureByteBaseModel):
        """Basket class"""

        items: Items

    with pytest.raises(ValidationError) as exc:
        Basket(items="hello")

    expected_msg = "1 validation error for Basket\nitems\n  Input should be a valid dictionary or instance of Items "
    assert expected_msg in str(exc.value)


class TestClass(FeatureByteBaseModel):
    """Test class for FeatureByteBaseModel"""

    items: List[PydanticObjectId]


def test_base_model__id_validation():
    """Test base model id field validation logic"""
    model = FeatureByteBaseDocumentModel()
    assert model.id is not None
    model = FeatureByteBaseDocumentModel(_id=None)
    assert model.id is not None
    id_value = ObjectId()

    model = FeatureByteBaseDocumentModel(_id=id_value)
    assert model.id == id_value
    model = FeatureByteBaseDocumentModel(_id=str(id_value))
    assert model.id == id_value

    # test PydanticObjectId in a list
    model = TestClass(items=[ObjectId(), ObjectId()])
    assert model.model_dump()["items"] == model.items
    assert json.loads(model.model_dump_json())["items"] == [str(item) for item in model.items]
    assert TestClass.model_validate_json(model.model_dump_json(by_alias=True)) == model

    model = TestClass(items=[str(ObjectId()), str(ObjectId())])
    assert model.model_dump()["items"] == model.items
    assert json.loads(model.model_dump_json())["items"] == [str(item) for item in model.items]
    assert TestClass.model_validate_json(model.model_dump_json(by_alias=True)) == model


@pytest.mark.parametrize(
    "version_str,version_identifier_dict",
    [
        ("V210304", {"name": "V210304", "suffix": None}),
        ("V210304_1", {"name": "V210304", "suffix": 1}),
    ],
)
def test_version_identifier(version_str, version_identifier_dict):
    """Test version identifier conversion"""
    version_identifier = VersionIdentifier(**version_identifier_dict)
    assert VersionIdentifier.from_str(version_str) == version_identifier
    assert version_identifier.to_str() == version_str


@pytest.mark.parametrize(
    "asset_name,expected_collection_name",
    [
        ("SCDTable", "scd_table"),
        ("FeatureList", "feature_list"),
        ("ApprovalRequest", "approval_request"),
    ],
)
def test_reference_info(asset_name, expected_collection_name):
    """Test reference info collection name property"""
    ref_info = ReferenceInfo(asset_name=asset_name, document_id=ObjectId())
    assert ref_info.collection_name == expected_collection_name


def test_base_model__name_validation():
    """Test base model id field validation logic"""
    model = FeatureByteBaseDocumentModel()
    assert model.id is not None
    model = FeatureByteBaseDocumentModel(name="test")
    assert model.id is not None
    with pytest.raises(ValidationError) as exc:
        FeatureByteBaseDocumentModel(name="t" * 256)
    expected_error = "1 validation error for FeatureByteBaseDocumentModel\nname\n  String should have at most 230 characters "
    assert expected_error in str(exc.value)
