"""
Test production ready validator
"""

import pytest

from featurebyte import DimensionView, EventView, Feature, FeatureJobSetting, MissingValueImputation
from featurebyte.service.validator.production_ready_validator import ProductionReadyValidator


@pytest.fixture(name="production_ready_validator")
def production_ready_validator_fixture(feature_namespace_service, app_container, version_service):
    """
    Get production ready validator
    """
    return ProductionReadyValidator(
        feature_namespace_service=feature_namespace_service,
        data_service=app_container.tabular_data_service,
        version_service=version_service,
    )


@pytest.fixture(name="source_version_creator")
def source_version_creator_fixture(version_service):
    """
    Fixture that is a constructor to get source versions.
    """

    async def get_source_version(feature_name):
        feature = Feature.get(feature_name)
        source_feature = await version_service.create_new_feature_version_using_source_settings(
            feature.id
        )
        return source_feature.node, source_feature.graph

    return get_source_version


@pytest.mark.asyncio
async def test_validate(
    production_ready_validator,
    snowflake_feature_store,
    snowflake_event_data_with_entity,
    feature_group_feature_job_setting,
):
    """
    Test the validate method returns an error when there are differences.
    """
    # Generate a feature
    snowflake_feature_store.save()
    snowflake_event_data_with_entity.save()
    snowflake_event_data_with_entity.update_default_feature_job_setting(
        feature_job_setting=FeatureJobSetting(**feature_group_feature_job_setting)
    )
    snowflake_event_data_with_entity["col_int"].update_critical_data_info(
        cleaning_operations=[
            MissingValueImputation(imputed_value=2.0),
        ]
    )

    event_view = EventView.from_event_data(snowflake_event_data_with_entity)
    updated_feature_job_setting = feature_group_feature_job_setting
    assert feature_group_feature_job_setting["blind_spot"] == "10m"
    updated_feature_job_setting["blind_spot"] = "5m"  # set a new value
    feature = event_view.groupby("cust_id").aggregate_over(
        value_column="col_int",
        method="sum",
        windows=["30m", "2h", "1d"],
        feature_job_setting=updated_feature_job_setting,
        feature_names=["sum_30m", "sum_2h", "sum_1d"],
    )["sum_30m"]
    feature.save()

    # Update cleaning operations after feature has been created
    snowflake_event_data_with_entity["col_int"].update_critical_data_info(
        cleaning_operations=[
            MissingValueImputation(imputed_value=0.0),
        ]
    )

    # Verify that validate does not throw an error if ignore_guardrails is True
    await production_ready_validator.validate(
        "sum_30m", feature.node, feature.graph, ignore_guardrails=True
    )

    # Verify that validates throws an error without ignore_guardrails
    with pytest.raises(ValueError) as exc:
        await production_ready_validator.validate("sum_30m", feature.node, feature.graph)
    exception_str = str(exc.value)
    assert "Discrepancies found between the current feature version" in exception_str
    assert "feature_job_setting" in exception_str
    assert "cleaning_operations" in exception_str


@pytest.mark.asyncio
async def test_assert_no_other_production_ready_feature__does_not_exist(production_ready_validator):
    """
    Test that there are no other production ready features.

    sum_30m is the name of the production_ready_feature used in the next test.
    """
    await production_ready_validator._assert_no_other_production_ready_feature("sum_30m")


@pytest.mark.asyncio
async def test_assert_no_other_production_ready_feature__exists(
    production_ready_validator, production_ready_feature
):
    """
    Test that assert throws an error if there are other production ready features with the same name.

    """
    with pytest.raises(ValueError) as exc:
        await production_ready_validator._assert_no_other_production_ready_feature(
            production_ready_feature.name
        )
    assert "Found another feature version that is already" in str(exc)


@pytest.mark.asyncio
async def test_get_feature_job_setting_diffs__settings_differ(
    production_ready_validator,
    snowflake_feature_store,
    snowflake_event_data_with_entity,
    feature_group_feature_job_setting,
    source_version_creator,
):
    """
    Test _check_feature_job_setting_match returns a dictionary when the settings differ
    """
    # update event data w/ a feature job setting
    snowflake_feature_store.save()
    snowflake_event_data_with_entity.save()
    snowflake_event_data_with_entity.update_default_feature_job_setting(
        FeatureJobSetting(**feature_group_feature_job_setting)
    )

    # create a feature with a different feature job setting from the event data
    event_view = EventView.from_event_data(snowflake_event_data_with_entity)
    updated_feature_job_setting = feature_group_feature_job_setting
    assert feature_group_feature_job_setting["blind_spot"] == "10m"
    updated_feature_job_setting["blind_spot"] = "5m"  # set a new value
    feature_group = event_view.groupby("cust_id").aggregate_over(
        value_column="col_float",
        method="sum",
        windows=["30m", "2h", "1d"],
        feature_job_setting=updated_feature_job_setting,
        feature_names=["sum_30m", "sum_2h", "sum_1d"],
    )
    feature = feature_group["sum_30m"]
    feature.save()

    source_node, source_feature_version_graph = await source_version_creator(feature.name)

    # check if the settings match
    refetch_feature = Feature.get(feature.name)
    differences = await production_ready_validator._get_feature_job_setting_diffs_source_vs_curr(
        source_node, source_feature_version_graph, refetch_feature.graph
    )

    # assert that there are differences
    assert differences == {
        "default": FeatureJobSetting(
            frequency="1800s", time_modulo_frequency="300s", blind_spot="600s"
        ),
        "feature": FeatureJobSetting(
            frequency="1800s", time_modulo_frequency="300s", blind_spot="300s"
        ),
    }


@pytest.mark.asyncio
async def test_get_feature_version_of_source__no_diff(
    production_ready_feature, production_ready_validator
):
    """
    Test _get_feature_version_of_source - no diff returns None
    """
    response = await production_ready_validator._get_feature_version_of_source(
        production_ready_feature.name
    )
    assert response is None


@pytest.fixture(name="feature_from_dimension_data")
def feature_from_dimension_data_fixture(cust_id_entity, snowflake_dimension_data):
    """Feature from dimension data"""
    snowflake_dimension_data["col_int"].as_entity(cust_id_entity.name)
    dimension_view = DimensionView.from_dimension_data(snowflake_dimension_data)
    return dimension_view["col_float"].as_feature("FloatFeature")  # pylint: disable=no-member


def test_raise_error_if_diffs_present():
    """
    Test raise error if diffs present
    """
    # Nothing expected to happen
    ProductionReadyValidator._raise_error_if_diffs_present({}, {})

    # Feature job setting errors found, but no cleaning ops errors
    some_error_diff = {"random key": "random stuff"}
    with pytest.raises(ValueError) as exc:
        ProductionReadyValidator._raise_error_if_diffs_present(some_error_diff, {})
    str_exc = str(exc.value)
    assert "Discrepancies found" in str_exc
    assert "feature_job_setting" in str_exc
    assert "cleaning_operations" not in str_exc

    # Cleaning ops errors found, but no feature job setting errors
    with pytest.raises(ValueError) as exc:
        ProductionReadyValidator._raise_error_if_diffs_present({}, some_error_diff)
    str_exc = str(exc.value)
    assert "Discrepancies found" in str_exc
    assert "feature_job_setting" not in str_exc
    assert "cleaning_operations" in str_exc

    # Both feature job setting, and cleaning ops errors found
    with pytest.raises(ValueError) as exc:
        ProductionReadyValidator._raise_error_if_diffs_present(some_error_diff, some_error_diff)
    str_exc = str(exc.value)
    assert "Discrepancies found" in str_exc
    assert "feature_job_setting" in str_exc
    assert "cleaning_operations" in str_exc
