"""
Tests FeatureByteBaseModel
"""
from typing import List

import pytest
from bson.objectid import ObjectId
from pydantic import StrictStr, ValidationError

from featurebyte.models.base import FeatureByteBaseDocumentModel, FeatureByteBaseModel


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
