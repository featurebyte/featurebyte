"""Test feature list batch feature create task payload schema"""
import pytest
from bson import ObjectId
from pydantic import ValidationError

from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.feature import BatchFeatureItem
from featurebyte.schema.feature_list import (
    FeatureListCreateWithBatchFeatureCreation,
    FeatureListCreateWithBatchFeatureCreationPayload,
)


@pytest.mark.parametrize(
    "payload_class",
    [
        FeatureListCreateWithBatchFeatureCreation,
        FeatureListCreateWithBatchFeatureCreationPayload,
    ],
)
def test_feature_list_batch_feature_creation(payload_class):
    """Test feature list batch feature creation schema"""
    feat_number = 501
    tabular_source = TabularSource(
        feature_store_id=ObjectId(),
        table_details=TableDetails(
            database_name=None,
            schema_name=None,
            table_name="test_table",
        ),
    )
    features = [
        BatchFeatureItem(
            id=ObjectId(),
            name=f"feat_{idx}",
            node_name="",
            tabular_source=tabular_source,
        )
        for idx in range(feat_number)
    ]

    # Test skip_batch_feature_creation is False
    with pytest.raises(ValidationError) as exc:
        payload_class(
            name="test",
            conflict_resolution_strategy="raise",
            features=features,
            skip_batch_feature_creation=False,
            graph=QueryGraphModel(),
        )

    expected_error = (
        "features count must be less than or equal to 500 "
        "if skip_batch_feature_creation is not set to True."
    )
    assert expected_error in str(exc.value)

    # Test skip_batch_feature_creation is True
    payload = payload_class(
        name="test",
        conflict_resolution_strategy="raise",
        features=features,
        skip_batch_feature_creation=True,
        graph=QueryGraphModel(),
    )
    assert len(payload.features) == feat_number
