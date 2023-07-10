"""
Tests FeatureByteBaseModel
"""
from typing import List

import pytest
from bson.objectid import ObjectId
from pydantic import StrictStr, ValidationError

from featurebyte.models.base import (
    FeatureByteBaseDocumentModel,
    FeatureByteBaseModel,
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
    assert "value is not a valid list (type=type_error.list)" in str(exc.value)

    with pytest.raises(ValidationError) as exc:
        Basket(items=[1])
    assert "str type expected (type=type_error.str)" in str(exc.value)


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
    expected_msg = (
        "value is not a valid Items type (type=type_error.featurebytetype; object_type=Items)"
    )
    assert expected_msg in str(exc.value)


def test_base_model__id_validation():
    """Test base model id field validation logic"""
    model = FeatureByteBaseDocumentModel()
    assert model.id is not None
    model = FeatureByteBaseDocumentModel(_id=None)
    assert model.id is not None
    id_value = ObjectId()
    model = FeatureByteBaseDocumentModel(_id=id_value)
    assert model.id == id_value


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
