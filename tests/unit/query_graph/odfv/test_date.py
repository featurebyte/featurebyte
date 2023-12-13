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
from featurebyte.query_graph.node.metadata.config import OnDemandViewCodeGenConfig
from featurebyte.query_graph.node.metadata.sdk_code import (
    CodeGenerator,
    VariableNameGenerator,
    VariableNameStr,
)


@pytest.fixture(name="config")
def fixture_config():
    """Fixture for the config"""
    return OnDemandViewCodeGenConfig(
        input_df_name="df", output_df_name="df", on_demand_function_name="on_demand"
    )


def test_date_add(config):
    """Test DateAddNode derive_on_demand_view_code"""
    node = DateAddNode(name="node_name", parameters={"value": None})
    statements, expr = node.derive_on_demand_view_code(
        node_inputs=[VariableNameStr("feat1"), VariableNameStr("feat2")],
        var_name_generator=VariableNameGenerator(),
        config=config,
    )
    assert statements == []
    feat1 = pd.date_range("2020-10-01", freq="d", periods=10).to_series()
    feat2 = pd.Series(pd.Timedelta(seconds=1) * np.random.randint(0, 100, 10))
    _ = feat1, feat2

    # check the expression can be evaluated & matches expected
    eval(expr)
    assert expr == "feat1 + feat2"


def test_date_difference(config):
    """Test DateDifferenceNode derive_on_demand_view_code"""
    node = DateDifferenceNode(name="node_name", parameters={"value": None})
    statements, expr = node.derive_on_demand_view_code(
        node_inputs=[VariableNameStr("feat1"), VariableNameStr("feat2")],
        var_name_generator=VariableNameGenerator(),
        config=config,
    )
    assert statements == []
    feat1 = pd.date_range("2020-10-01", freq="d", periods=10).to_series()
    feat2 = feat1
    _ = feat1, feat2

    # check the expression can be evaluated & matches expected
    eval(expr)
    assert expr == "feat1 - feat2"


@pytest.mark.parametrize(
    "property", [property for property in DatetimeSupportedPropertyType.__args__]
)
def test_datetime_extract(config, property):
    """Test DatetimeExtractNode derive_on_demand_view_code"""
    # single input with no timezone offset
    node = DatetimeExtractNode(
        name="node_name", parameters={"property": property, "timezone_offset": None}
    )
    statements, expr = node.derive_on_demand_view_code(
        node_inputs=[VariableNameStr("feat")],
        var_name_generator=VariableNameGenerator(),
        config=config,
    )
    assert statements == []
    feat = pd.date_range("2020-10-01", freq="d", periods=10).to_series()
    _ = feat

    # check the expression can be evaluated & matches expected
    eval(expr)
    assert expr == f"feat.dt.{property}"

    # second input as timezone offset
    statements, expr = node.derive_on_demand_view_code(
        node_inputs=[VariableNameStr("feat"), VariableNameStr("feat1")],
        var_name_generator=VariableNameGenerator(),
        config=config,
    )
    assert statements == [("feat_dt", "feat + feat1")]
    assert expr == f"feat_dt.dt.{property}"

    # offset as a timedelta
    node = DatetimeExtractNode(
        name="node_name", parameters={"property": property, "timezone_offset": "+06:00"}
    )
    statements, expr = node.derive_on_demand_view_code(
        node_inputs=[VariableNameStr("feat")],
        var_name_generator=VariableNameGenerator(),
        config=config,
    )
    code_gen = CodeGenerator(statements=statements + [(VariableNameStr("output"), expr)])
    codes = code_gen.generate().strip()
    assert codes == (
        'tz_offset = pd.to_timedelta("+06:00:00")\n'
        "feat_dt = feat + tz_offset\n"
        f"output = feat_dt.dt.{property}"
    )


@pytest.mark.parametrize(
    "property,expected_expr",
    [
        ("day", "feat.dt.seconds // 86400"),  # 86400 = 24 * 60 * 60
        ("hour", "feat.dt.seconds // 3600"),  # 3600 = 60 * 60
        ("minute", "feat.dt.seconds // 60"),
        ("second", "feat.dt.seconds"),
        ("millisecond", "feat.dt.microseconds // 1000"),
        ("microsecond", "feat.dt.microseconds"),
    ],
)
def test_time_delta_extract(config, property, expected_expr):
    """Test TimeDeltaExtractNode derive_on_demand_view_code"""
    # single input with no timezone offset
    node = TimeDeltaExtractNode(name="node_name", parameters={"property": property})
    statements, expr = node.derive_on_demand_view_code(
        node_inputs=[VariableNameStr("feat")],
        var_name_generator=VariableNameGenerator(),
        config=config,
    )
    assert statements == []
    feat = pd.Series(pd.Timedelta(seconds=1) * np.random.randint(0, 100, 10))
    _ = feat

    # check the expression can be evaluated & matches expected
    eval(expr)
    assert expr == expected_expr


@pytest.mark.parametrize("unit", [unit for unit in TimedeltaSupportedUnitType.__args__])
def test_time_delta(config, unit):
    """Test TimeDeltaNode derive_on_demand_view_code"""
    node = TimeDeltaNode(name="node_name", parameters={"unit": unit})
    statements, expr = node.derive_on_demand_view_code(
        node_inputs=[VariableNameStr("val")],
        var_name_generator=VariableNameGenerator(),
        config=config,
    )
    code_gen = CodeGenerator(statements=statements + [(VariableNameStr("output"), expr)])
    codes = code_gen.generate().strip()
    assert codes == (f'feat = pd.to_timedelta(val, unit="{unit}")\n' "output = feat")
