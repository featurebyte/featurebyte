"""Unit tests for SDK code generation"""
import pytest

from featurebyte.core.timedelta import to_timedelta
from featurebyte.enum import AggFunc
from featurebyte.exception import RecordUpdateException
from featurebyte.query_graph.node.cleaning_operation import MissingValueImputation
from tests.util.helper import check_sdk_code_generation


def test_sdk_code_generation__complex_arithmetic_expression(saved_event_table, update_fixtures):
    """Check SDK code generation for complex arithmetic expression"""
    event_view = saved_event_table.get_view()
    col_a, col_b = event_view["col_int"], event_view["col_float"]
    output = (
        (1 - col_a) * (col_b - 1) / (col_a + col_b)
        + 1 / (col_a - col_b)
        + col_a % 10
        - col_b.pow(2)
        + col_a
    )
    check_sdk_code_generation(
        output,
        to_use_saved_data=True,
        to_format=True,
        fixture_path="tests/fixtures/sdk_code/complex_arithmetic_expression.py",
        update_fixtures=update_fixtures,
        table_id=saved_event_table.id,
    )


def test_sdk_code_generation__complex_relational_expression(saved_event_table, update_fixtures):
    """SDK code generation for complex relational expression"""
    event_view = saved_event_table.get_view()
    col_a = event_view["col_int"]
    output = (
        (col_a > 1) & (col_a < 10) | (col_a == 1) | (col_a != 10) | (col_a >= 1) | (col_a <= 10)
    )
    check_sdk_code_generation(
        output,
        to_use_saved_data=True,
        to_format=True,
        fixture_path="tests/fixtures/sdk_code/complex_relational_expression.py",
        update_fixtures=update_fixtures,
        table_id=saved_event_table.id,
    )


def test_sdk_code_generation__complex_math_expression(saved_event_table, update_fixtures):
    """SDK code generation for complex math expression"""
    event_view = saved_event_table.get_view()
    col_a, col_b = event_view["col_int"], event_view["col_float"]
    output = (
        (col_a > 10).astype(int)
        - (~(col_a > 10)).astype(int)
        - col_b.abs().sqrt().ceil()
        + col_a.floor() * col_b.log() / col_a.exp()
        + col_a.isnull().astype(float)
    )
    check_sdk_code_generation(
        output,
        to_use_saved_data=True,
        to_format=True,
        fixture_path="tests/fixtures/sdk_code/complex_math_expression.py",
        update_fixtures=update_fixtures,
        table_id=saved_event_table.id,
    )


def test_sdk_code_generation__complex_date_related_operations(saved_event_table, update_fixtures):
    """SDK code generation for complex date related operations"""
    event_view = saved_event_table.get_view()
    col_a = event_view["event_timestamp"]
    col_b = to_timedelta(event_view["col_int"], unit="hour")
    # create complex date property related operations
    output_date_prop = (
        col_a.dt.year
        + col_a.dt.quarter
        - col_a.dt.month * col_a.dt.week / col_a.dt.day % col_a.dt.day_of_week
        + col_a.dt.hour
        - col_a.dt.minute * col_a.dt.second
    )
    # create complex timedelta related operations
    output_timedelta = (
        col_b.dt.day * col_b.dt.hour
        - col_b.dt.minute / col_b.dt.second * col_b.dt.millisecond
        + col_b.dt.microsecond
    )
    # create complex date related operations
    output_date = (col_a + col_b).dt.second + (col_a - col_a).dt.minute
    output = output_date_prop + output_timedelta + output_date
    check_sdk_code_generation(
        output,
        to_use_saved_data=True,
        to_format=True,
        fixture_path="tests/fixtures/sdk_code/complex_date_related_operations.py",
        update_fixtures=update_fixtures,
        table_id=saved_event_table.id,
    )


def test_sdk_code_generation__complex_string_related_operations(saved_event_table, update_fixtures):
    """SDK code generation for complex string related operations"""
    to_use_saved_data, to_format = True, True
    event_view = saved_event_table.get_view()
    col_a = event_view["col_text"]
    output = (
        col_a.str.len().astype(str)
        + col_a.str.strip(to_strip=" ")
        + col_a.str.replace(pat=" ", repl="_")
        + col_a.str.pad(width=10, side="left", fillchar="_")
        + col_a.str.upper()
        + col_a.str.lower()
        + col_a.str.contains(pat=" ", case=True).astype(str)
        + col_a.str.slice(start=0, stop=10)
    )
    check_sdk_code_generation(
        output,
        to_use_saved_data=to_use_saved_data,
        to_format=to_format,
        fixture_path="tests/fixtures/sdk_code/complex_string_related_operations.py",
        update_fixtures=update_fixtures,
        table_id=saved_event_table.id,
    )


def test_sdk_code_generation__complex_feature(
    saved_event_table, saved_item_table, transaction_entity, update_fixtures
):
    """SDK code generation for complex feature"""
    saved_item_table.event_id_col.as_entity(transaction_entity.name)

    event_view = saved_event_table.get_view()
    item_view = saved_item_table.get_view(event_suffix="_event_view")

    # construct an item view feature referencing an event view column and join back to event view
    item_view = item_view.join_event_table_attributes(["col_float"])
    feat_item_sum = item_view.groupby("event_id_col").aggregate(
        value_column="col_float",
        method=AggFunc.SUM,
        feature_name="non_time_sum_feature",
    )
    event_view = event_view.add_feature(feat_item_sum.name, feat_item_sum, "cust_id")

    # use the newly created column to construct a new time-aware feature
    feat_event_sum = event_view.groupby("cust_id").aggregate_over(
        value_column=feat_item_sum.name,
        method="sum",
        windows=["24h"],
        feature_names=["sum_a_24h"],
    )["sum_a_24h"]
    feat_event_count = event_view.groupby("cust_id", category="col_int").aggregate_over(
        method="count",
        windows=["24h"],
        feature_names=["count_a_24h_per_col_int"],
    )["count_a_24h_per_col_int"]
    output = (
        feat_event_sum
        + feat_event_count.cd.entropy() * feat_event_count.cd.most_frequent().str.len()
        - feat_event_count.cd.unique_count(include_missing=False)
        / feat_event_count.cd.unique_count(include_missing=True)
    )
    check_sdk_code_generation(
        output,
        to_use_saved_data=True,
        to_format=True,
        fixture_path="tests/fixtures/sdk_code/complex_event_item_feature.py.jinja2",
        update_fixtures=update_fixtures,
        table_id=saved_event_table.id,
        item_table_id=saved_item_table.id,
    )

    # check main input nodes
    primary_input_nodes = output.graph.get_primary_input_nodes(node_name=output.node_name)
    assert [node.parameters.id for node in primary_input_nodes] == [saved_event_table.id]

    primary_input_nodes = feat_item_sum.graph.get_primary_input_nodes(
        node_name=feat_item_sum.node_name
    )
    assert [node.parameters.id for node in primary_input_nodes] == [saved_item_table.id]

    temp = feat_item_sum + output
    primary_input_nodes = temp.graph.get_primary_input_nodes(node_name=temp.node_name)
    assert [node.parameters.id for node in primary_input_nodes] == [
        saved_item_table.id,
        saved_event_table.id,
    ]


def test_sdk_code_generation__multi_table_feature(
    saved_event_table, saved_item_table, transaction_entity, cust_id_entity, update_fixtures
):
    """Test SDK code generation for multi-table feature"""
    # add critical data info to the table
    saved_event_table.col_char.update_critical_data_info(
        cleaning_operations=[MissingValueImputation(imputed_value="missing")]
    )
    for col in ["col_float", "col_int", "cust_id"]:
        saved_event_table[col].update_critical_data_info(
            cleaning_operations=[MissingValueImputation(imputed_value=0.0)]
        )
    for col in ["event_id_col", "item_id_col", "item_amount"]:
        saved_item_table[col].update_critical_data_info(
            cleaning_operations=[MissingValueImputation(imputed_value=0.0)]
        )

    # tag entities
    saved_event_table.cust_id.as_entity(cust_id_entity.name)
    saved_item_table.event_id_col.as_entity(transaction_entity.name)

    # create views
    event_suffix = "_event_table"
    event_view = saved_event_table.get_view()
    item_view = saved_item_table.get_view(event_suffix=event_suffix)

    # create feature
    item_view = item_view.join_event_table_attributes(
        ["col_float", "col_char", "col_boolean"], event_suffix=event_suffix
    )
    item_view["percent"] = item_view["item_amount"] / item_view[f"col_float{event_suffix}"]
    max_percent = item_view.groupby("event_id_col").aggregate(
        value_column="percent",
        method=AggFunc.MAX,
        feature_name="max_percent",
    )

    event_view = event_view.add_feature(max_percent.name, max_percent, "cust_id")
    output = event_view.groupby("cust_id").aggregate_over(
        value_column=max_percent.name,
        method=AggFunc.MAX,
        windows=["30d"],
        feature_names=["max_percent_over_30d"],
    )["max_percent_over_30d"]

    # save feature so that the graph is pruned
    output.save()

    check_sdk_code_generation(
        output,
        to_use_saved_data=True,
        to_format=True,
        fixture_path="tests/fixtures/sdk_code/complex_multi_table_feature.py.jinja2",
        update_fixtures=update_fixtures,
        table_id=saved_event_table.id,
        item_table_id=saved_item_table.id,
    )

    # check update readiness to production ready won't fail due to production ready guardrail
    output.update_readiness(readiness="PRODUCTION_READY")

    # check update readiness to production ready should fail due to critical data info guardrail
    # (though the feature is already production ready)
    saved_event_table.col_float.update_critical_data_info(cleaning_operations=[])
    assert output.readiness == "PRODUCTION_READY"
    with pytest.raises(RecordUpdateException) as exc:
        output.update_readiness(readiness="PRODUCTION_READY")

    expected_error = (
        "Discrepancies found between the promoted feature version you are trying to promote to PRODUCTION_READY, "
        # data source does not have col_float cleaning operations
        "and the input table.\n{'cleaning_operations': {'data_source': ["
        "ColumnCleaningOperation(column_name='col_int', "
        "cleaning_operations=[MissingValueImputation(imputed_value=0.0, type=missing)]), "
        "ColumnCleaningOperation(column_name='cust_id', "
        "cleaning_operations=[MissingValueImputation(imputed_value=0.0, type=missing)])], "
        # promoted feature has additional col_float cleaning operations
        "'promoted_feature': ["
        "ColumnCleaningOperation(column_name='col_int', "
        "cleaning_operations=[MissingValueImputation(imputed_value=0.0, type=missing)]), "
        "ColumnCleaningOperation(column_name='col_float', "
        "cleaning_operations=[MissingValueImputation(imputed_value=0.0, type=missing)]), "
        "ColumnCleaningOperation(column_name='cust_id', "
        "cleaning_operations=[MissingValueImputation(imputed_value=0.0, type=missing)])]}}\n"
        "Please fix these issues first before trying to promote your feature to PRODUCTION_READY."
    )
    assert expected_error in str(exc.value)

    # check main input nodes
    primary_input_nodes = output.graph.get_primary_input_nodes(node_name=output.node_name)
    assert [node.parameters.id for node in primary_input_nodes] == [saved_event_table.id]

    primary_input_nodes = max_percent.graph.get_primary_input_nodes(node_name=max_percent.node_name)
    assert [node.parameters.id for node in primary_input_nodes] == [saved_item_table.id]

    temp = output + max_percent
    primary_input_nodes = temp.graph.get_primary_input_nodes(node_name=temp.node_name)
    assert [node.parameters.id for node in primary_input_nodes] == [
        saved_event_table.id,
        saved_item_table.id,
    ]


def test_sdk_code_generation__item_view_cosine_similarity_feature(
    saved_event_table, saved_item_table, transaction_entity, cust_id_entity, update_fixtures
):
    """Test SDK code generation for item view groupby feature"""
    # tag entities
    saved_item_table.event_id_col.as_entity(transaction_entity.name)

    # create views
    item_view = saved_item_table.get_view(event_suffix="_event_table")

    grouped = item_view.groupby("cust_id_event_table", category="item_id_col").aggregate_over(
        value_column="item_amount",
        method=AggFunc.SUM,
        windows=["30d"],
        feature_names=["sum_item_amount_over_30d"],
    )
    feat = grouped["sum_item_amount_over_30d"]

    grouped_1 = item_view.groupby("cust_id_event_table", category="item_id_col").aggregate_over(
        value_column="item_amount",
        method=AggFunc.SUM,
        windows=["90d"],
        feature_names=["sum_item_amount_over_90d"],
    )
    feat_1 = grouped_1["sum_item_amount_over_90d"]
    output = feat.cd.cosine_similarity(other=feat_1)
    output.name = "sum_item_amount_over_30d_cosine_similarity_sum_item_amount_over_90d"

    # save feature so that the graph is pruned
    output.save()

    check_sdk_code_generation(
        output,
        to_use_saved_data=True,
        to_format=True,
        fixture_path="tests/fixtures/sdk_code/feature_item_cosine_similarity.py.jinja2",
        update_fixtures=update_fixtures,
        table_id=saved_event_table.id,
        item_table_id=saved_item_table.id,
    )

    # check update readiness to production ready won't fail due to production ready guardrail
    output.update_readiness(readiness="PRODUCTION_READY")

    # check the main input nodes
    primary_input_nodes = output.graph.get_primary_input_nodes(node_name=output.node_name)
    assert [node.parameters.id for node in primary_input_nodes] == [saved_item_table.id]
