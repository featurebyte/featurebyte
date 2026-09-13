"""
Test exposure namespace schema
"""

from bson import ObjectId

from featurebyte.enum import DBVarType
from featurebyte.schema.exposure_namespace import ExposureNamespaceCreate


def test_exposure_namespace_create():
    """
    Test exposure namespace create schema
    """
    exposure_namespace_create = ExposureNamespaceCreate(
        name="exposure_namespace",
        dtype=DBVarType.FLOAT,
        entity_ids=[ObjectId()],
        window="7d",
    )
    assert exposure_namespace_create.name == "exposure_namespace"
    assert exposure_namespace_create.dtype == DBVarType.FLOAT
    assert exposure_namespace_create.window == "7d"


def test_exposure_namespace_create_without_window():
    """
    Test exposure namespace create schema without window
    """
    exposure_namespace_create = ExposureNamespaceCreate(
        name="exposure_namespace",
        dtype=DBVarType.INT,
        entity_ids=[ObjectId()],
    )
    assert exposure_namespace_create.name == "exposure_namespace"
    assert exposure_namespace_create.dtype == DBVarType.INT
    assert exposure_namespace_create.window is None


def test_exposure_namespace_create_with_defaults():
    """
    Test exposure namespace create schema with default values
    """
    entity_id = ObjectId()
    exposure_namespace_create = ExposureNamespaceCreate(
        name="my_exposure",
        dtype=DBVarType.FLOAT,
        entity_ids=[entity_id],
    )
    assert exposure_namespace_create.exposure_ids == []
    assert exposure_namespace_create.default_exposure_id is None
    assert exposure_namespace_create.default_version_mode == "AUTO"
