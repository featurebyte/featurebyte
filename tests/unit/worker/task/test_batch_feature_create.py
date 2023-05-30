"""
Test batch feature creation task
"""
import textwrap

import pytest

from featurebyte import RecordRetrievalException
from featurebyte.api.catalog import Catalog, Entity
from featurebyte.models.base import activate_catalog
from featurebyte.worker.task.batch_feature_create import execute_sdk_code


@pytest.fixture(name="test_catalog")
def fixture_catalog(snowflake_feature_store):
    """Fixture for catalog"""
    catalog = Catalog(name="test_catalog", default_feature_store_ids=[snowflake_feature_store.id])
    catalog.save()
    return catalog


@pytest.mark.asyncio
async def test_execute_sdk_code(test_catalog):
    """Test execute sdk code"""
    # save the current active catalog
    current_active_catalog = Catalog.get_active()

    # check that the entity doesn't exist
    with pytest.raises(RecordRetrievalException):
        Entity.get(name="test_entity")

    # prepare the SDK code to create & save an entity
    sdk_code = textwrap.dedent(
        """
    from featurebyte import Catalog, Entity

    Entity.create(name="test_entity", serving_names=["test_entity_serving_name"])
    """
    ).strip()

    # execute the SDK code & check the entity's catalog id
    await execute_sdk_code(catalog_id=test_catalog.id, code=sdk_code)
    entity = Entity.get(name="test_entity")
    assert entity.serving_names == ["test_entity_serving_name"]
    assert entity.catalog_id == test_catalog.id

    # restore the active catalog
    activate_catalog(catalog_id=current_active_catalog.id)
    catalog = Catalog.get_active()
    assert catalog.id == current_active_catalog.id
