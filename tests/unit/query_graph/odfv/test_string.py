"""
Test the string nodes in the on-demand view code generation.
"""
import pandas as pd
import pytest

from featurebyte.query_graph.node.metadata.config import OnDemandViewCodeGenConfig
from featurebyte.query_graph.node.metadata.sdk_code import VariableNameGenerator, VariableNameStr
from featurebyte.query_graph.node.string import (
    ConcatNode,
    LengthNode,
    PadNode,
    ReplaceNode,
    StringCaseNode,
    StringContainsNode,
    SubStringNode,
    TrimNode,
)

NODE_PARAMS = {"name": "node_name"}


@pytest.mark.parametrize(
    "node, expected_expr, expected_statements",
    [
        (LengthNode(**NODE_PARAMS), "feat.str.len()", []),
        (
            TrimNode(**NODE_PARAMS, parameters={"side": "left", "character": "*"}),
            'feat.str.lstrip(to_strip="*")',
            [],
        ),
        (
            TrimNode(**NODE_PARAMS, parameters={"side": "right", "character": None}),
            "feat.str.rstrip(to_strip=None)",
            [],
        ),
        (
            TrimNode(**NODE_PARAMS, parameters={"side": "both", "character": None}),
            "feat.str.strip(to_strip=None)",
            [],
        ),
        (
            ReplaceNode(**NODE_PARAMS, parameters={"pattern": "a", "replacement": "b"}),
            'feat.str.replace(pat="a", repl="b")',
            [],
        ),
        (
            PadNode(**NODE_PARAMS, parameters={"side": "left", "length": 3, "pad": "0"}),
            "feat",
            [("feat", 'feat.str.pad(width=3, side="left", fillchar="0")')],
        ),
        (StringCaseNode(**NODE_PARAMS, parameters={"case": "upper"}), "feat.str.upper()", []),
        (StringCaseNode(**NODE_PARAMS, parameters={"case": "lower"}), "feat.str.lower()", []),
        (
            StringContainsNode(**NODE_PARAMS, parameters={"pattern": "foo", "case": True}),
            'feat.str.contains(pat="foo", case=True)',
            [],
        ),
        (
            SubStringNode(**NODE_PARAMS, parameters={"start": 1, "length": 2}),
            "feat.str.slice(start=1, stop=3)",
            [],
        ),
        (ConcatNode(**NODE_PARAMS, parameters={"value": "foo"}), 'feat + "foo"', []),
    ],
)
def test_derive_on_demand_view_code(node, expected_expr, expected_statements):
    """Test derive_on_demand_view_code"""
    config = OnDemandViewCodeGenConfig(
        input_df_name="df", output_df_name="df", on_demand_function_name="on_demand"
    )
    statements, expr = node.derive_on_demand_view_code(
        node_inputs=[VariableNameStr("feat")],
        var_name_generator=VariableNameGenerator(),
        config=config,
    )
    assert statements == expected_statements
    feat = pd.Series(["foo", "bar", "baz"])
    _ = feat

    # check the expression can be evaluated & matches expected
    eval(expr)
    assert expr == expected_expr
