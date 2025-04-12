"""
Test the string nodes in the on-demand view code generation.
"""

import numpy as np
import pandas as pd
import pytest

from featurebyte.query_graph.node.metadata.sdk_code import (
    NodeCodeGenOutput,
    VariableNameGenerator,
    VariableNameStr,
)
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
from tests.unit.query_graph.util import evaluate_and_compare_odfv_and_udf_results

NODE_PARAMS = {"name": "node_name"}


@pytest.mark.parametrize(
    "node, expected_odfv_expr, expected_udf_expr, expected_values",
    [
        (
            LengthNode(**NODE_PARAMS),
            "feat.astype(object).str.len().astype(float)",
            "len(feat)",
            [0, 3, 4, 3, np.nan],
        ),
        (
            TrimNode(**NODE_PARAMS, parameters={"side": "left", "character": "f"}),
            'feat.astype(object).str.lstrip(to_strip="f")',
            'feat.lstrip("f")',
            ["", "oo", "bar ", "bAz", np.nan],
        ),
        (
            TrimNode(**NODE_PARAMS, parameters={"side": "right", "character": None}),
            "feat.astype(object).str.rstrip(to_strip=None)",
            "feat.rstrip(None)",
            ["", "foo", "bar", "bAz", np.nan],
        ),
        (
            TrimNode(**NODE_PARAMS, parameters={"side": "both", "character": None}),
            "feat.astype(object).str.strip(to_strip=None)",
            "feat.strip(None)",
            ["", "foo", "bar", "bAz", np.nan],
        ),
        (
            ReplaceNode(**NODE_PARAMS, parameters={"pattern": "a", "replacement": "b"}),
            'feat.astype(object).str.replace(pat="a", repl="b")',
            'feat.replace("a", "b")',
            ["", "foo", "bbr ", "bAz", np.nan],
        ),
        (
            PadNode(**NODE_PARAMS, parameters={"side": "left", "length": 3, "pad": "0"}),
            'feat.astype(object).str.pad(width=3, side="left", fillchar="0")',
            'pad_string(input_string=feat, side="left", length=3, pad="0")',
            ["000", "foo", "bar ", "bAz", np.nan],
        ),
        (
            StringCaseNode(**NODE_PARAMS, parameters={"case": "upper"}),
            "feat.astype(object).str.upper()",
            "feat.upper()",
            ["", "FOO", "BAR ", "BAZ", np.nan],
        ),
        (
            StringCaseNode(**NODE_PARAMS, parameters={"case": "lower"}),
            "feat.astype(object).str.lower()",
            "feat.lower()",
            ["", "foo", "bar ", "baz", np.nan],
        ),
        (
            StringContainsNode(**NODE_PARAMS, parameters={"pattern": "A", "case": True}),
            'feat.astype(object).str.contains(pat="A", case=True)',
            '"A" in feat',
            [False, False, False, True, np.nan],
        ),
        (
            StringContainsNode(**NODE_PARAMS, parameters={"pattern": "A", "case": False}),
            'feat.astype(object).str.contains(pat="A", case=False)',
            '"A".lower() in feat.lower()',
            [False, False, True, True, np.nan],
        ),
        (
            SubStringNode(**NODE_PARAMS, parameters={"start": 1, "length": 2}),
            "feat.astype(object).str.slice(start=1, stop=3)",
            "feat[1:3]",
            ["", "oo", "ar", "Az", np.nan],
        ),
        (
            ConcatNode(**NODE_PARAMS, parameters={"value": "foo"}),
            'pd.Series(np.where(pd.isna(feat), np.nan, feat.astype(object) + "foo"), index=feat.index)',
            'feat + "foo"',
            ["foo", "foofoo", "bar foo", "bAzfoo", np.nan],
        ),
    ],
)
def test_derive_on_demand_view_code(
    node, odfv_config, udf_config, expected_odfv_expr, expected_udf_expr, expected_values
):
    """Test derive_on_demand_view_code"""
    node_inputs = [VariableNameStr("feat")]
    node_inputs = [NodeCodeGenOutput(var_name_or_expr=node_input) for node_input in node_inputs]

    odfv_stats, odfv_expr = node.derive_on_demand_view_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=odfv_config,
    )

    udf_stats, udf_expr = node.derive_user_defined_function_code(
        node_inputs=node_inputs,
        var_name_generator=VariableNameGenerator(),
        config=udf_config,
    )

    if not isinstance(node, ConcatNode):
        expected_odfv_expr = (
            f"pd.Series(np.where(pd.isna(feat), np.nan, {expected_odfv_expr}), index=feat.index)"
        )
    expected_udf_expr = f"np.nan if pd.isna(feat) else {expected_udf_expr}"

    assert odfv_stats == []
    assert odfv_expr == expected_odfv_expr

    if not isinstance(node, PadNode):
        assert udf_stats == []
    assert udf_expr == expected_udf_expr

    # check the expression can be evaluated & matches expected
    feat = pd.Series(["", "foo", "bar ", "bAz", np.nan])
    evaluate_and_compare_odfv_and_udf_results(
        input_map={"feat": feat},
        odfv_expr=odfv_expr,
        udf_expr=udf_expr,
        odfv_stats=odfv_stats,
        udf_stats=udf_stats,
        expected_output=pd.Series(expected_values),
    )
