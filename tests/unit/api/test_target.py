"""
Test target module
"""

import pandas as pd
import pytest

import featurebyte
from featurebyte.api.target import Target
from featurebyte.api.target_namespace import TargetNamespace
from featurebyte.enum import DBVarType
from tests.unit.api.base_feature_or_target_test import FeatureOrTargetBaseTestSuite, TestItemType
from tests.util.helper import fb_assert_frame_equal


class TestTargetTestSuite(FeatureOrTargetBaseTestSuite):
    """Test suite for target"""

    item_type = TestItemType.TARGET
    expected_item_definition = (
        f"""
    # Generated by SDK version: {featurebyte.get_version()}"""
        + """
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
        skip_fill_na=True,
    )
    output = target
    """
    )
    expected_saved_item_definition = (
        f"""
    # Generated by SDK version: {featurebyte.get_version()}"""
        + """
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
        skip_fill_na=True,
    )
    output = target
    output.save(_id=ObjectId("{item_id}"))
    """
    )

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

    def test_window(self, float_target):
        """
        Test window is present
        """
        float_target.save()
        assert float_target.window == "1d"

    def test_list_includes_namespace(self, float_target, cust_id_entity):
        """
        Test list includes namespace targets.
        """
        # Persist a target from an aggregation
        float_target.save()
        # Persist a target that has no recipe.
        TargetNamespace.create(
            name="target_without_recipe",
            primary_entity=[cust_id_entity.name],
            dtype=DBVarType.FLOAT,
            window="7d",
        )

        # List out the targets
        lists = Target.list()

        # Verify that there are 2 targets in the list.
        expected_df = pd.DataFrame(
            {
                "name": ["float_target", "target_without_recipe"],
                "dtype": ["FLOAT", "FLOAT"],
                "entities": [["customer"], ["customer"]],
            }
        )
        actual_df = lists[["name", "dtype", "entities"]]
        fb_assert_frame_equal(actual_df, expected_df, sort_by_columns=["name"])

    def test_target_delete(self, float_target):
        """
        Test target delete
        """
        float_target.save()
        assert float_target.saved

        namespace = float_target.target_namespace
        assert namespace.saved

        float_target.delete()
        assert not float_target.saved

        # check namespace is not deleted
        assert namespace.saved
