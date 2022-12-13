"""
Tests for featurebyte.query_graph.graph_node.critical_data_info
"""
import pytest
from pydantic.error_wrappers import ValidationError

from featurebyte.query_graph.graph_node.critical_data_info import CriticalDataInfo


@pytest.mark.parametrize(
    "imputations",
    [
        # no imputation
        [],
        # single imputation
        [{"type": "missing", "imputed_value": 0}],
        [{"type": "disguised", "disguised_values": [-999, 999], "imputed_value": 0}],
        [{"type": "not_in", "expected_values": [1, 2, 3, 4], "imputed_value": 0}],
        [{"type": "less_than", "end_point": 0, "imputed_value": 0}],
        [{"type": "is_string", "imputed_value": None}],
        # multiple imputations
        [
            {"type": "missing", "imputed_value": 0},
            {"type": "disguised", "disguised_values": [-999, -99], "imputed_value": None},
        ],
        [
            {"type": "missing", "imputed_value": 0},
            {"type": "disguised", "disguised_values": [-999, -99], "imputed_value": None},
            {"type": "less_than", "end_point": 0, "imputed_value": 0},
        ],
    ],
)
def test_critical_data_info__valid_imputations(imputations):
    """Test multiple imputations (valid)"""
    cdi = CriticalDataInfo(imputations=imputations)
    assert cdi.dict() == {"imputations": imputations}


@pytest.mark.parametrize(
    "imputations,conflicts",
    [
        (
            [
                # invalid as -999 imputed to None first, then None is imputed to 0 (double imputation)
                {"type": "disguised", "disguised_values": [-999, -99], "imputed_value": None},
                {"type": "missing", "imputed_value": 0},
            ],
            ["DisguisedValueImputation", "MissingValueImputation"],
        ),
        (
            [
                # invalid as 0 is imputed to None first, then None is imputed to 0 (double imputation)
                {"type": "not_in", "expected_values": [-999, -99], "imputed_value": None},
                {"type": "missing", "imputed_value": 0},
            ],
            ["UnexpectedValueImputation", "MissingValueImputation"],
        ),
        (
            [
                # invalid as None is imputed to 0 first, then 0 is imputed to 1 (double imputation)
                {"type": "missing", "imputed_value": 0},
                {"type": "disguised", "disguised_values": [-999, -99], "imputed_value": None},
                {"type": "less_than_or_equal", "end_point": 0, "imputed_value": 1},
            ],
            ["MissingValueImputation", "ValueBeyondEndpointImputation"],
        ),
    ],
)
def test_critical_data_info__invalid_imputations(imputations, conflicts):
    """Test invalid imputations exception raised properly"""
    with pytest.raises(ValidationError) as exc:
        CriticalDataInfo(imputations=imputations)
    error_msg = str(exc.value.json())
    expected_msg = "Please revise the imputations so that no value could be imputed twice."
    assert expected_msg in error_msg
    for conflict_imputation in conflicts:
        assert conflict_imputation in error_msg
