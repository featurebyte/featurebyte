"""
Test the date nodes in the on-demand view code generation.
"""

import numpy as np
import pandas as pd
import pytest

from featurebyte.query_graph.node.date import (
    DateAddNode,
    DateDifferenceNode,
    DatetimeExtractNode,
    TimeDeltaExtractNode,
    TimeDeltaNode,
)
from featurebyte.query_graph.node.metadata.sdk_code import (
    CodeGenerator,
    VariableNameGenerator,
    VariableNameStr,
)
from featurebyte.typing import DatetimeSupportedPropertyType, TimedeltaSupportedUnitType
from tests.unit.query_graph.util import evaluate_and_compare_odfv_and_udf_results


def test_date_add(odfv_config, udf_config, node_code_gen_output_factory):
    """Test DateAddNode derive_on_demand_view_code"""
    node = DateAddNode(name="node_name", parameters={"value": None})
    node_inputs = [VariableNameStr("feat1"), VariableNameStr("feat2")]
    node_inputs = [node_code_gen_output_factory(node_input) for node_input in node_inputs]

    odfv_stats, odfv_expr = node.derive_on_demand_view_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=odfv_config,
    )
    assert odfv_stats == []
    assert odfv_expr == "pd.to_datetime(feat1) + pd.to_timedelta(feat2)"

    udf_stats, udf_expr = node.derive_user_defined_function_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=udf_config,
    )
    assert odfv_stats == []
    assert udf_expr == "pd.to_datetime(feat1) + pd.to_timedelta(feat2)"

    feat1 = pd.Series(pd.date_range("2020-10-01", freq="d", periods=10).to_list() + [np.nan])
    feat2 = pd.Series([np.nan] + list(pd.Timedelta(seconds=1) * np.random.randint(0, 100, 10)))

    # check the expression can be evaluated & matches expected
    evaluate_and_compare_odfv_and_udf_results(
        input_map={"feat1": feat1, "feat2": feat2},
        odfv_expr=odfv_expr,
        udf_expr=udf_expr,
        odfv_stats=odfv_stats,
        udf_stats=udf_stats,
    )


def test_date_difference(odfv_config, udf_config, node_code_gen_output_factory):
    """Test DateDifferenceNode derive_on_demand_view_code"""
    node = DateDifferenceNode(name="node_name", parameters={"value": None})
    node_inputs = [VariableNameStr("feat1"), VariableNameStr("feat2")]
    node_inputs = [node_code_gen_output_factory(node_input) for node_input in node_inputs]

    odfv_stats, odfv_expr = node.derive_on_demand_view_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=odfv_config,
    )
    assert odfv_stats == []
    assert odfv_expr == "pd.to_datetime(feat1) - pd.to_datetime(feat2)"

    udf_stats, udf_expr = node.derive_user_defined_function_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=udf_config,
    )
    assert odfv_stats == []
    assert udf_expr == "pd.to_datetime(feat1) - pd.to_datetime(feat2)"

    feat1 = pd.Series(pd.date_range("2020-10-01", freq="d", periods=10).to_list() + [np.nan])
    feat2 = pd.Series([np.nan] + pd.date_range("2020-10-01", freq="d", periods=10).to_list())

    # check the expression can be evaluated & matches expected
    evaluate_and_compare_odfv_and_udf_results(
        input_map={"feat1": feat1, "feat2": feat2},
        odfv_expr=odfv_expr,
        udf_expr=udf_expr,
        odfv_stats=odfv_stats,
        udf_stats=udf_stats,
    )


@pytest.mark.parametrize(
    "dt_property", [property for property in DatetimeSupportedPropertyType.__args__]
)
def test_datetime_extract(odfv_config, udf_config, dt_property, node_code_gen_output_factory):
    """Test DatetimeExtractNode derive_on_demand_view_code"""
    # single input with no timezone offset
    node = DatetimeExtractNode(
        name="node_name", parameters={"property": dt_property, "timezone_offset": None}
    )
    node_inputs = [VariableNameStr("feat")]
    node_inputs = [node_code_gen_output_factory(node_input) for node_input in node_inputs]

    odfv_stats, odfv_expr = node.derive_on_demand_view_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=odfv_config,
    )
    assert odfv_stats == []
    expected_odfv_expr = f"pd.to_datetime(feat).dt.{dt_property}"
    if dt_property == "week":
        expected_odfv_expr = "pd.to_datetime(feat).dt.isocalendar().week"
    assert odfv_expr == expected_odfv_expr

    udf_stats, udf_expr = node.derive_user_defined_function_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=udf_config,
    )
    assert udf_stats == []
    assert udf_expr == f"pd.to_datetime(feat).{dt_property}"

    feat = pd.Series(pd.date_range("2020-10-01", freq="d", periods=10).to_list() + [np.nan])
    evaluate_and_compare_odfv_and_udf_results(
        input_map={"feat": feat},
        odfv_expr=odfv_expr,
        udf_expr=udf_expr,
        odfv_stats=odfv_stats,
        udf_stats=udf_stats,
    )

    # second input as timezone offset
    node_inputs = [VariableNameStr("feat"), VariableNameStr("feat1")]
    node_inputs = [node_code_gen_output_factory(node_input) for node_input in node_inputs]
    odfv_stats, odfv_expr = node.derive_on_demand_view_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=odfv_config,
    )
    assert odfv_stats == [("feat_dt", "pd.to_datetime(feat) + pd.to_timedelta(feat1)")]
    expected_odfv_expr = f"pd.to_datetime(feat_dt).dt.{dt_property}"
    if dt_property == "week":
        expected_odfv_expr = "pd.to_datetime(feat_dt).dt.isocalendar().week"
    assert odfv_expr == expected_odfv_expr

    udf_stats, udf_expr = node.derive_user_defined_function_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=udf_config,
    )
    assert udf_stats == [("feat", "pd.to_datetime(feat) + pd.to_timedelta(feat1)")]
    assert udf_expr == f"pd.to_datetime(feat).{dt_property}"

    # offset as a timedelta
    node = DatetimeExtractNode(
        name="node_name", parameters={"property": dt_property, "timezone_offset": "+06:00"}
    )
    odfv_stats, odfv_expr = node.derive_on_demand_view_code(
        node_inputs=node_inputs[:1],
        var_name_generator=VariableNameGenerator(),
        config=odfv_config,
    )
    code_gen = CodeGenerator(statements=odfv_stats + [(VariableNameStr("output"), odfv_expr)])
    codes = code_gen.generate().strip()
    dt_prop = "isocalendar().week" if dt_property == "week" else dt_property
    assert codes == (
        'tz_offset = pd.to_timedelta("+06:00:00")\n'
        "feat_dt = pd.to_datetime(feat) + tz_offset\n"
        f"output = pd.to_datetime(feat_dt).dt.{dt_prop}"
    )

    udf_stats, udf_expr = node.derive_user_defined_function_code(
        node_inputs=node_inputs[:1],
        var_name_generator=VariableNameGenerator(),
        config=udf_config,
    )
    code_gen = CodeGenerator(statements=udf_stats + [(VariableNameStr("output"), udf_expr)])
    codes = code_gen.generate().strip()
    assert codes == (
        'tz_offset = pd.to_timedelta("+06:00:00")\n'
        "feat = pd.to_datetime(feat) + tz_offset\n"
        f"output = pd.to_datetime(feat).{dt_property}"
    )


@pytest.mark.parametrize(
    "td_property,expected_odfv_expr,expected_udf_expr,expected_output",
    [
        (
            "day",
            "pd.to_timedelta(feat).dt.total_seconds() // 86400",
            "pd.to_timedelta(feat).total_seconds() // 86400",
            [0, 0, 5],
        ),  # 86400 = 24 * 60 * 60
        (
            "hour",
            "pd.to_timedelta(feat).dt.total_seconds() // 3600",
            "pd.to_timedelta(feat).total_seconds() // 3600",
            [0, 3, 120],
        ),  # 3600 = 60 * 60
        (
            "minute",
            "pd.to_timedelta(feat).dt.total_seconds() // 60",
            "pd.to_timedelta(feat).total_seconds() // 60",
            [1, 180, 7219],
        ),
        (
            "second",
            "pd.to_timedelta(feat).dt.total_seconds()",
            "pd.to_timedelta(feat).total_seconds()",
            [60.123, 10800.000456, 433149],
        ),
        (
            "millisecond",
            "1e3 * pd.to_timedelta(feat).dt.total_seconds()",
            "1e3 * pd.to_timedelta(feat).total_seconds()",
            [60123, 10800000.456, 433149000],
        ),
        (
            "microsecond",
            "1e6 * pd.to_timedelta(feat).dt.total_seconds()",
            "1e6 * pd.to_timedelta(feat).total_seconds()",
            [60123000, 10800000456, 433149000000],
        ),
    ],
)
def test_time_delta_extract(
    odfv_config,
    udf_config,
    td_property,
    expected_odfv_expr,
    expected_udf_expr,
    expected_output,
    node_code_gen_output_factory,
):
    """Test TimeDeltaExtractNode derive_on_demand_view_code"""
    # single input with no timezone offset
    node = TimeDeltaExtractNode(name="node_name", parameters={"property": td_property})
    node_inputs = [VariableNameStr("feat")]
    node_inputs = [node_code_gen_output_factory(node_input) for node_input in node_inputs]

    odfv_stats, odfv_expr = node.derive_on_demand_view_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=odfv_config,
    )
    assert odfv_stats == []
    assert odfv_expr == expected_odfv_expr

    udf_stats, udf_expr = node.derive_user_defined_function_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=udf_config,
    )
    assert udf_stats == []
    assert udf_expr == f"np.nan if pd.isna(feat) else {expected_udf_expr}"

    timedelta_vals = [
        np.nan,
        pd.Timedelta(minutes=1) + pd.Timedelta(seconds=0.123),
        pd.Timedelta(hours=3) + pd.Timedelta(seconds=0.000456),
        pd.Timedelta(days=5) + pd.Timedelta(minutes=6) + pd.Timedelta(seconds=789),
    ]
    feat = pd.Series(timedelta_vals)

    # check the expression can be evaluated & matches expected
    evaluate_and_compare_odfv_and_udf_results(
        input_map={"feat": feat},
        odfv_expr=odfv_expr,
        udf_expr=udf_expr,
        odfv_stats=odfv_stats,
        udf_stats=udf_stats,
        expected_output=pd.Series([np.nan] + expected_output),
    )


@pytest.mark.parametrize("unit", [unit for unit in TimedeltaSupportedUnitType.__args__])
def test_time_delta(odfv_config, udf_config, unit, node_code_gen_output_factory):
    """Test TimeDeltaNode derive_on_demand_view_code"""
    node = TimeDeltaNode(name="node_name", parameters={"unit": unit})
    node_inputs = [VariableNameStr("val")]
    node_inputs = [node_code_gen_output_factory(node_input) for node_input in node_inputs]

    odfv_stats, odfv_expr = node.derive_on_demand_view_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=odfv_config,
    )
    code_gen = CodeGenerator(statements=odfv_stats + [(VariableNameStr("output"), odfv_expr)])
    codes = code_gen.generate().strip()
    assert codes == f'feat = pd.to_timedelta(val, unit="{unit}")\n' "output = feat"

    udf_stats, udf_expr = node.derive_user_defined_function_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=udf_config,
    )
    code_gen = CodeGenerator(statements=udf_stats + [(VariableNameStr("output"), udf_expr)])
    codes = code_gen.generate().strip()
    assert codes == f'feat = pd.to_timedelta(val, unit="{unit}")\n' "output = feat"
