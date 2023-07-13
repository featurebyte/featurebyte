"""
Test target module
"""
import pytest

from tests.unit.api.base_feature_or_target_test import FeatureOrTargetBaseTestSuite, TestItemType


class TestTargetTestSuite(FeatureOrTargetBaseTestSuite):
    """Test suite for target"""

    item_type = TestItemType.TARGET
    expected_item_definition = """
    # Generated by SDK version: 0.3.0
    from bson import ObjectId
    from featurebyte import EventTable

    event_table = EventTable.get_by_id(ObjectId("{table_id}"))
    event_view = event_table.get_view(
        view_mode="manual",
        drop_column_names=["created_at"],
        column_cleaning_operations=[],
    )
    target = event_view.groupby(
        by_keys=["cust_id"], category=None
    ).forward_aggregate(
        value_column="col_float",
        method="sum",
        window="1d",
        target_name="float_target",
    )
    feat = target["float_target"]
    output = feat
    """
    expected_saved_item_definition = """
    # Generated by SDK version: 0.3.0
    from bson import ObjectId
    from featurebyte import EventTable


    # event_table name: "sf_event_table"
    event_table = EventTable.get_by_id(ObjectId("{table_id}"))
    event_view = event_table.get_view(
        view_mode="manual",
        drop_column_names=["created_at"],
        column_cleaning_operations=[],
    )
    target = event_view.groupby(
        by_keys=["cust_id"], category=None
    ).forward_aggregate(
        value_column="col_float",
        method="sum",
        window="1d",
        target_name="float_target",
    )
    feat = target["float_target"]
    output = feat
    output.save(_id=ObjectId("{item_id}"))
    """

    def test_invalid_operations_with_feature(
        self, float_target, float_feature, snowflake_event_view_with_entity
    ):
        """
        Test invalid operations with feature
        """
        # Test binary series operation
        with pytest.raises(TypeError) as exc_info:
            _ = float_target + float_feature
        assert "Operation between Target and Feature is not supported" in str(exc_info)

        arbitrary_mask = float_target > 20
        # Test series assignment fails when other series is a feature
        with pytest.raises(TypeError) as exc_info:
            float_target[arbitrary_mask] = float_feature
        assert "Operation between Target and Feature is not supported" in str(exc_info)

        # Test series assignment fails when other series is a view column
        with pytest.raises(TypeError) as exc_info:
            float_target[arbitrary_mask] = snowflake_event_view_with_entity["col_int"]
        assert "Operation between Target and EventViewColumn is not supported" in str(exc_info)

        # Assigning a target to a target is ok
        new_target = float_target + 1
        float_target[arbitrary_mask] = new_target
