"""
Feature or Target base test suite
"""
import textwrap

import pytest

from featurebyte.common.model_util import get_version
from featurebyte.enum import StrEnum
from featurebyte.exception import RecordRetrievalException


class TestItemType(StrEnum):
    """
    Test item type
    """

    FEATURE = "Feature"
    TARGET = "Target"


class FeatureOrTargetBaseTestSuite:
    """
    FeatureOrTargetBaseTestSuite contains common tests for api feature or target objects.
    """

    item_type: TestItemType = None
    expected_item_definition = None
    expected_saved_item_definition = None

    @pytest.fixture(name="item_under_test")
    def get_item_under_test_fixture(self, float_feature, float_target):
        """Retrieves fixture for item under test."""
        item_map = {
            TestItemType.FEATURE: float_feature,
            TestItemType.TARGET: float_target,
        }
        if self.item_type not in item_map:
            pytest.fail(
                f"Invalid item type: {self.item_type}. Please update the test suite to support this item type"
            )
        return item_map[self.item_type]

    @pytest.fixture(name="saved_item_under_test")
    def get_saved_item_under_test_fixture(self, item_under_test):
        """Retrieves fixture for saved item under test."""
        item_under_test.save()
        return item_under_test

    @staticmethod
    def get_expected_definition(definition_template, table, item):
        """Get expected definition"""
        expected_definition = textwrap.dedent(
            definition_template.format(table_id=table.id, item_id=item.id)
        )
        return expected_definition.strip()

    def test_item_properties(self, item_under_test, snowflake_event_table, cust_id_entity, catalog):
        """Test item properties"""
        with pytest.raises(RecordRetrievalException) as exc:
            _ = item_under_test.version
        expected_error_message = (
            f'{self.item_type} (id: "{item_under_test.id}") not found. '
            f"Please save the {self.item_type} object first."
        )
        assert expected_error_message in str(exc.value)
        assert item_under_test.catalog_id == catalog.id
        assert item_under_test.entity_ids == [cust_id_entity.id]
        assert item_under_test.table_ids == [snowflake_event_table.id]
        assert item_under_test.definition.strip() == self.get_expected_definition(
            self.expected_item_definition, snowflake_event_table, item_under_test
        )

    def test_saved_item_properties(
        self, saved_item_under_test, snowflake_event_table, cust_id_entity, catalog
    ):
        """Test saved item properties"""
        assert saved_item_under_test.version.startswith(get_version())
        assert saved_item_under_test.catalog_id == catalog.id
        assert saved_item_under_test.entity_ids == [cust_id_entity.id]
        assert saved_item_under_test.table_ids == [snowflake_event_table.id]
        assert saved_item_under_test.definition.strip() == self.get_expected_definition(
            self.expected_saved_item_definition, snowflake_event_table, saved_item_under_test
        )

    def test_info(self, saved_item_under_test):
        """Test info"""
        # TODO: Add more assertions to check the info
        info = saved_item_under_test.info()
        assert info["entities"] == [
            {"name": "customer", "serving_names": ["cust_id"], "catalog_name": "catalog"}
        ]

    def test_update_version_description(self, saved_item_under_test):
        """Test update version description"""
        assert saved_item_under_test.description is None
        saved_item_under_test.update_version_description("new description")
        assert saved_item_under_test.description == "new description"
        assert saved_item_under_test.info()["description"] == "new description"
        saved_item_under_test.update_version_description(None)
        assert saved_item_under_test.description is None
        assert saved_item_under_test.info()["description"] is None

    def test_update_description(self, saved_item_under_test):
        """Test update description"""
        assert saved_item_under_test.info()["namespace_description"] is None
        saved_item_under_test.update_description("new description")
        assert saved_item_under_test.info()["namespace_description"] == "new description"
        saved_item_under_test.update_description(None)
        assert saved_item_under_test.info()["namespace_description"] is None
