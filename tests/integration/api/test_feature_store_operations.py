"""
Feature store operations test class
"""
from typing import Any, Optional

from unittest.mock import patch

import pytest

from featurebyte import FeatureStore, SourceType
from featurebyte.exception import DuplicatedRecordException, FeatureStoreSchemaCollisionError
from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.node.schema import DatabaseDetails
from featurebyte.service.session_validator import SessionValidatorService


@pytest.mark.skip(reason="skipping while we rollback the default state")
@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_feature_store_create__no_writes_on_error(
    mongo_persistent, feature_store_details, get_cred
):
    """
    Test that nothing is written to mongo if we error halfway through
    while interacting with snowflake
    """

    def new_validate_throw_error(
        self,
        feature_store_name: str,
        session_type: SourceType,
        details: DatabaseDetails,
        get_credential: Any,
        users_feature_store_id: Optional[PydanticObjectId],
    ):
        raise FeatureStoreSchemaCollisionError

    feature_store_name_to_create = "snowflake_featurestore"
    with patch.object(
        SessionValidatorService,
        "validate_feature_store_id_not_used_in_warehouse",
        new_validate_throw_error,
    ):
        # Note that this comes back as a `DuplicatedRecordException`, even though we throw a
        # `FeatureStoreSchemaCollisionError` above. This is because the HTTP middleware will convert the
        # `FeatureStoreSchemaCollisionError` to a CONFLICT HTTP error type, which then gets converted into a
        # `DuplicatedRecordException` error type in the controller.
        with pytest.raises(DuplicatedRecordException):
            # Create feature store
            _ = FeatureStore.create(
                name=feature_store_name_to_create,
                source_type=SourceType.SNOWFLAKE,
                details=feature_store_details,
            )

    # Assert that there's nothing in mongo. This confirms that the transactions never happens since we raise
    # an exception in `validate_feature_store_id_not_used_in_warehouse`, which happens after we try to
    # create the document.
    persistent, _ = mongo_persistent
    _, count = await persistent.find(collection_name="feature_store", query_filter={})
    assert count == 0
