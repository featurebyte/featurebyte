"""
Test production ready validator
"""
import textwrap

import pytest
from bson import ObjectId

from featurebyte import Feature, FeatureJobSetting, MissingValueImputation
from featurebyte.exception import DocumentUpdateError
from featurebyte.models import FeatureModel
from featurebyte.models.feature_store import TableStatus
from featurebyte.service.validator.production_ready_validator import ProductionReadyValidator


@pytest.fixture(name="production_ready_validator")
def production_ready_validator_fixture(version_service, feature_service, table_service):
    """
    Get production ready validator
    """
    return ProductionReadyValidator(
        table_service=table_service,
        version_service=version_service,
        feature_service=feature_service,
    )


@pytest.fixture(name="source_version_creator")
def source_version_creator_fixture(version_service, feature_service):
    """
    Fixture that is a constructor to get source versions.
    """

    async def get_source_version(feature_name):
        docs = await feature_service.list_documents_as_dict(
            query_filter={
                "name": feature_name,
            }
        )
        data = docs["data"]
        assert len(data) == 1
        feature = data[0]
        source_feature = await version_service.create_new_feature_version_using_source_settings(
            feature["_id"]
        )
        if source_feature is None:
            return None
        return source_feature.node, source_feature.graph

    return get_source_version


def format_exception_string_for_comparison(exception_str: str) -> str:
    """
    Formats exception string for comparison.
    """
    stripped = textwrap.dedent(exception_str).strip()
    new_lines_removed = stripped.replace("\n", "")
    return new_lines_removed.replace(" ", "")


@pytest.mark.asyncio
async def test_validate(
    production_ready_validator,
    snowflake_event_table_with_entity,
    feature_group_feature_job_setting,
):
    """
    Test the validate method returns an error when there are differences.
    """
    # Generate a feature
    snowflake_event_table_with_entity.update_default_feature_job_setting(
        feature_job_setting=feature_group_feature_job_setting
    )
    snowflake_event_table_with_entity["col_int"].update_critical_data_info(
        cleaning_operations=[
            MissingValueImputation(imputed_value=2.5),
        ]
    )

    event_view = snowflake_event_table_with_entity.get_view()
    updated_feature_job_setting = feature_group_feature_job_setting
    assert feature_group_feature_job_setting.blind_spot == "10m"
    new_feature_job_settings_dict = updated_feature_job_setting.json_dict()
    new_feature_job_settings_dict["blind_spot"] = "3m"  # set a new value
    updated_feature_job_setting = FeatureJobSetting(**new_feature_job_settings_dict)
    feature = event_view.groupby("cust_id").aggregate_over(
        value_column="col_int",
        method="sum",
        windows=["90m"],
        feature_job_setting=updated_feature_job_setting,
        feature_names=["sum_90m"],
    )["sum_90m"]
    feature.save()

    # Update cleaning operations after feature has been created
    snowflake_event_table_with_entity["col_int"].update_critical_data_info(
        cleaning_operations=[
            MissingValueImputation(imputed_value=0.0),
        ]
    )

    # Verify that validate does not throw an error if ignore_guardrails is True
    await production_ready_validator.validate(
        promoted_feature=feature.cached_model, ignore_guardrails=True
    )

    # Verify that validates throws an error without ignore_guardrails
    with pytest.raises(DocumentUpdateError) as exc:
        await production_ready_validator.validate(promoted_feature=feature.cached_model)
    exception_str = format_exception_string_for_comparison(str(exc.value))
    expected_exception_str = format_exception_string_for_comparison(
        """
        Discrepancies found between the promoted feature version you are trying to promote to PRODUCTION_READY,
        and the input table.
        {
            'feature_job_setting': {
                'data_source': FeatureJobSetting(blind_spot='600s', frequency='1800s', time_modulo_frequency='300s'),
                'promoted_feature': FeatureJobSetting(blind_spot='180s', frequency='1800s', time_modulo_frequency='300s')
            },
            'cleaning_operations': {
                'data_source': [ColumnCleaningOperation(column_name='col_int',
                    cleaning_operations=[MissingValueImputation(imputed_value=0.0)]
                )],
                'promoted_feature': [ColumnCleaningOperation(column_name='col_int',
                    cleaning_operations=[MissingValueImputation(imputed_value=2.5)]
                )]
            }
        }
        Please fix these issues first before trying to promote your feature to PRODUCTION_READY.
        """
    )
    assert exception_str == expected_exception_str


@pytest.mark.asyncio
async def test_assert_no_other_production_ready_feature__exists(
    production_ready_validator, production_ready_feature
):
    """
    Test that assert throws an error if there are other production ready features with the same name.

    """
    with pytest.raises(DocumentUpdateError) as exc:
        another_feature = FeatureModel(**{**production_ready_feature.dict(), "_id": ObjectId()})
        await production_ready_validator._assert_no_other_production_ready_feature(another_feature)

    expected_msg = (
        'Found another feature version that is already PRODUCTION_READY. Please deprecate the feature "sum_30m" '
        f"with ID {production_ready_feature.id} first before promoting the promoted version as there can only be "
        f"one feature version that is production ready at any point in time. We are unable to promote the feature "
        f"with ID {another_feature.id} right now."
    )
    assert expected_msg in str(exc.value)


@pytest.mark.asyncio
async def test_assert_no_other_production_ready_feature__no_error_if_promoted_feature_is_currently_prod_ready(
    production_ready_validator, production_ready_feature
):
    """
    Test that assert does not throw an error if the feature that is already production ready is the same as the
    feature that is _being_ promoted to production ready.
    """
    await production_ready_validator._assert_no_other_production_ready_feature(
        production_ready_feature
    )


@pytest.mark.asyncio
async def test_assert_no_deprecated_tables(
    production_ready_validator, event_table_service, table_status_service, feature
):
    """Test assert_no_deprecated_tables throws an error if there are deprecated tables."""
    # check table status & check that the assertion passes
    table_id = feature.table_ids[0]
    table = await event_table_service.get_document(document_id=table_id)
    assert table.status == TableStatus.PUBLIC_DRAFT
    await production_ready_validator._assert_no_deprecated_table(promoted_feature=feature)

    # update table status to deprecated & check that the assertion fails
    await table_status_service.update_status(
        service=event_table_service,
        document_id=table_id,
        status=TableStatus.DEPRECATED,
    )
    table = await event_table_service.get_document(document_id=table_id)
    assert table.status == TableStatus.DEPRECATED

    with pytest.raises(DocumentUpdateError) as exc:
        await production_ready_validator._assert_no_deprecated_table(promoted_feature=feature)

    expected_msg = (
        'Found a deprecated table "sf_event_table" that is used by the feature "sum_30m". '
        "We are unable to promote the feature to PRODUCTION_READY right now."
    )
    assert expected_msg in str(exc.value)


@pytest.mark.asyncio
async def test_get_feature_job_setting_diffs__settings_differ(
    production_ready_validator,
    snowflake_event_table_with_entity,
    feature_group_feature_job_setting,
    source_version_creator,
    feature_service,
):
    """
    Test _check_feature_job_setting_match returns a dictionary when the settings differ
    """
    # update event table w/ a feature job setting
    snowflake_event_table_with_entity.update_default_feature_job_setting(
        feature_group_feature_job_setting
    )

    # create a feature with a different feature job setting from the event table
    event_view = snowflake_event_table_with_entity.get_view()
    updated_feature_job_setting = feature_group_feature_job_setting.copy()
    assert updated_feature_job_setting.blind_spot == "10m"
    new_feature_job_settings_dict = updated_feature_job_setting.json_dict()
    new_feature_job_settings_dict["blind_spot"] = "5m"  # set a new value
    updated_feature_job_setting = FeatureJobSetting(**new_feature_job_settings_dict)
    feature_group = event_view.groupby("cust_id").aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["60m"],
        feature_job_setting=updated_feature_job_setting,
        feature_names=["sum_60m"],
    )
    feature = feature_group["sum_60m"]
    feature.save()

    source_node, source_feature_version_graph = await source_version_creator(feature.name)

    # check if the settings match
    feature_docs = await feature_service.list_documents_as_dict(query_filter={"name": feature.name})
    feature_data = feature_docs["data"]
    assert len(feature_data) == 1
    feature_document = feature_data[0]
    refetch_feature = Feature(**feature_document)
    pruned_graph, _ = refetch_feature.extract_pruned_graph_and_node()
    differences = await production_ready_validator._get_feature_job_setting_diffs_table_source_vs_promoted_feature(
        source_node, source_feature_version_graph, pruned_graph
    )

    # assert that there are differences
    assert differences == {
        "data_source": FeatureJobSetting(
            frequency="1800s", time_modulo_frequency="300s", blind_spot="600s"
        ),
        "promoted_feature": FeatureJobSetting(
            frequency="1800s", time_modulo_frequency="300s", blind_spot="300s"
        ),
    }


@pytest.mark.asyncio
async def test_validate__no_diff_in_feature_should_return_none(
    snowflake_event_table_with_entity,
    production_ready_validator,
    feature_group_feature_job_setting,
):
    """
    Test validate - no diff returns None
    """
    # Create a feature that has same feature job setting and cleaning operations as it's table source
    snowflake_event_table_with_entity.update_default_feature_job_setting(
        feature_job_setting=feature_group_feature_job_setting
    )

    event_view = snowflake_event_table_with_entity.get_view()
    feature = event_view.groupby("cust_id").aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["60m"],
        feature_names=["sum_60m"],
    )["sum_60m"]
    feature.save()

    # Validate
    response = await production_ready_validator.validate(feature.cached_model)
    assert response is None


def test_raise_error_if_diffs_present():
    """
    Test raise error if diffs present
    """
    # Nothing expected to happen
    ProductionReadyValidator._raise_error_if_diffs_present({}, {})

    # Feature job setting errors found, but no cleaning ops errors
    some_error_diff = {"random key": "random stuff"}
    with pytest.raises(DocumentUpdateError) as exc:
        ProductionReadyValidator._raise_error_if_diffs_present(some_error_diff, {})
    str_exc = str(exc.value)
    assert "Discrepancies found" in str_exc
    assert "feature_job_setting" in str_exc
    assert "cleaning_operations" not in str_exc

    # Cleaning ops errors found, but no feature job setting errors
    with pytest.raises(DocumentUpdateError) as exc:
        ProductionReadyValidator._raise_error_if_diffs_present({}, some_error_diff)
    str_exc = str(exc.value)
    assert "Discrepancies found" in str_exc
    assert "feature_job_setting" not in str_exc
    assert "cleaning_operations" in str_exc

    # Both feature job setting, and cleaning ops errors found
    with pytest.raises(DocumentUpdateError) as exc:
        ProductionReadyValidator._raise_error_if_diffs_present(some_error_diff, some_error_diff)
    str_exc = str(exc.value)
    assert "Discrepancies found" in str_exc
    assert "feature_job_setting" in str_exc
    assert "cleaning_operations" in str_exc
