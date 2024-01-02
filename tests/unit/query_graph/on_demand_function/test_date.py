"""
Test the date nodes in the on-demand view code generation.
"""
import numpy as np
import pandas as pd
import pytest

from featurebyte.common.typing import DatetimeSupportedPropertyType, TimedeltaSupportedUnitType
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
from tests.unit.query_graph.util import evaluate_and_compare_odfv_and_udf_results


def test_date_add(odfv_config, udf_config):
    """Test DateAddNode derive_on_demand_view_code"""
    node = DateAddNode(name="node_name", parameters={"value": None})
    node_inputs = [VariableNameStr("feat1"), VariableNameStr("feat2")]

    odfv_stats, odfv_expr = node.derive_on_demand_view_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=odfv_config,
    )
    assert odfv_stats == []
    assert odfv_expr == "feat1 + feat2"

    udf_stats, udf_expr = node.derive_user_defined_function_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=udf_config,
    )
    assert odfv_stats == []
    assert udf_expr == "feat1 + feat2"

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


def test_date_difference(odfv_config, udf_config):
    """Test DateDifferenceNode derive_on_demand_view_code"""
    node = DateDifferenceNode(name="node_name", parameters={"value": None})
    node_inputs = [VariableNameStr("feat1"), VariableNameStr("feat2")]

    odfv_stats, odfv_expr = node.derive_on_demand_view_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=odfv_config,
    )
    assert odfv_stats == []
    assert odfv_expr == "feat1 - feat2"

    udf_stats, udf_expr = node.derive_user_defined_function_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=udf_config,
    )
    assert odfv_stats == []
    assert udf_expr == "feat1 - feat2"

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
def test_datetime_extract(odfv_config, udf_config, dt_property):
    """Test DatetimeExtractNode derive_on_demand_view_code"""
    # single input with no timezone offset
    node = DatetimeExtractNode(
        name="node_name", parameters={"property": dt_property, "timezone_offset": None}
    )
    node_inputs = [VariableNameStr("feat")]

    odfv_stats, odfv_expr = node.derive_on_demand_view_code(
        node_inputs=[VariableNameStr("feat")],
        var_name_generator=VariableNameGenerator(),
        config=odfv_config,
    )
    assert odfv_stats == []
    assert odfv_expr == f"feat.dt.{dt_property}"

    udf_stats, udf_expr = node.derive_user_defined_function_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=udf_config,
    )
    assert udf_stats == []
    assert udf_expr == f"feat.{dt_property}"

    feat = pd.Series(pd.date_range("2020-10-01", freq="d", periods=10).to_list() + [np.nan])
    evaluate_and_compare_odfv_and_udf_results(
        input_map={"feat": feat},
        odfv_expr=odfv_expr,
        udf_expr=udf_expr,
        odfv_stats=odfv_stats,
        udf_stats=udf_stats,
    )

    # second input as timezone offset
    odfv_stats, odfv_expr = node.derive_on_demand_view_code(
        node_inputs=[VariableNameStr("feat"), VariableNameStr("feat1")],
        var_name_generator=VariableNameGenerator(),
        config=odfv_config,
    )
    assert odfv_stats == [("feat_dt", "feat + feat1")]
    assert odfv_expr == f"feat_dt.dt.{dt_property}"

    udf_stats, udf_expr = node.derive_user_defined_function_code(
        node_inputs=[VariableNameStr("feat"), VariableNameStr("feat1")],
        var_name_generator=VariableNameGenerator(),
        config=udf_config,
    )
    assert udf_stats == [("feat", "feat + feat1")]
    assert udf_expr == f"feat.{dt_property}"

    # offset as a timedelta
    node = DatetimeExtractNode(
        name="node_name", parameters={"property": dt_property, "timezone_offset": "+06:00"}
    )
    odfv_stats, odfv_expr = node.derive_on_demand_view_code(
        node_inputs=[VariableNameStr("feat")],
        var_name_generator=VariableNameGenerator(),
        config=odfv_config,
    )
    code_gen = CodeGenerator(statements=odfv_stats + [(VariableNameStr("output"), odfv_expr)])
    codes = code_gen.generate().strip()
    assert codes == (
        'tz_offset = pd.to_timedelta("+06:00:00")\n'
        "feat_dt = feat + tz_offset\n"
        f"output = feat_dt.dt.{dt_property}"
    )

    udf_stats, udf_expr = node.derive_user_defined_function_code(
        node_inputs=[VariableNameStr("feat")],
        var_name_generator=VariableNameGenerator(),
        config=udf_config,
    )
    code_gen = CodeGenerator(statements=udf_stats + [(VariableNameStr("output"), udf_expr)])
    codes = code_gen.generate().strip()
    assert codes == (
        'tz_offset = pd.to_timedelta("+06:00:00")\n'
        "feat = feat + tz_offset\n"
        f"output = feat.{dt_property}"
    )


@pytest.mark.parametrize(
    "td_property,expected_odfv_expr,expected_udf_expr",
    [
        ("day", "feat.dt.seconds // 86400", "feat.seconds // 86400"),  # 86400 = 24 * 60 * 60
        ("hour", "feat.dt.seconds // 3600", "feat.seconds // 3600"),  # 3600 = 60 * 60
        ("minute", "feat.dt.seconds // 60", "feat.seconds // 60"),
        ("second", "feat.dt.seconds", "feat.seconds"),
        ("millisecond", "feat.dt.microseconds // 1000", "feat.microseconds // 1000"),
        ("microsecond", "feat.dt.microseconds", "feat.microseconds"),
    ],
)
def test_time_delta_extract(
    odfv_config, udf_config, td_property, expected_odfv_expr, expected_udf_expr
):
    """Test TimeDeltaExtractNode derive_on_demand_view_code"""
    # single input with no timezone offset
    node = TimeDeltaExtractNode(name="node_name", parameters={"property": td_property})
    node_inputs = [VariableNameStr("feat")]

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

    feat = pd.Series([np.nan] + list(pd.Timedelta(seconds=1) * np.random.randint(0, 100, 10)))

    # check the expression can be evaluated & matches expected
    evaluate_and_compare_odfv_and_udf_results(
        input_map={"feat": feat},
        odfv_expr=odfv_expr,
        udf_expr=udf_expr,
        odfv_stats=odfv_stats,
        udf_stats=udf_stats,
    )


@pytest.mark.parametrize("unit", [unit for unit in TimedeltaSupportedUnitType.__args__])
def test_time_delta(odfv_config, udf_config, unit):
    """Test TimeDeltaNode derive_on_demand_view_code"""
    node = TimeDeltaNode(name="node_name", parameters={"unit": unit})
    node_inputs = [VariableNameStr("val")]

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