"""
Test models#treatment module.
"""

import pytest

from featurebyte.models.treatment import (
    Propensity,
    PropensityGranularity,
    PropensityKnowledge,
)


def test_p_global_only_meaningful_when_granularity_global():
    with pytest.raises(
        ValueError,
        match="p_global is only meaningful when granularity is 'global'",
    ):
        Propensity(
            granularity=PropensityGranularity.UNIT,
            knowledge=PropensityKnowledge.ESTIMATED,
            p_global=0.5,
        )


def test_design_known_global_requires_p_global():
    with pytest.raises(
        ValueError,
        match="p_global must be provided when granularity is 'global' and knowledge is 'design-known'",
    ):
        Propensity(
            granularity=PropensityGranularity.GLOBAL,
            knowledge=PropensityKnowledge.DESIGN_KNOWN,
            p_global=None,
        )


def test_global_estimated_allows_missing_p_global():
    # No error: p_global can be None when knowledge is ESTIMATED
    obj = Propensity(
        granularity=PropensityGranularity.GLOBAL,
        knowledge=PropensityKnowledge.ESTIMATED,
        p_global=None,
    )
    assert obj.p_global is None


@pytest.mark.parametrize("bad_value", [-0.1, 1.1])
def test_p_global_float_must_be_between_0_and_1(bad_value):
    with pytest.raises(
        ValueError,
        match="p_global float must be between 0 and 1",
    ):
        Propensity(
            granularity=PropensityGranularity.GLOBAL,
            knowledge=PropensityKnowledge.DESIGN_KNOWN,
            p_global=bad_value,
        )


def test_p_global_float_valid():
    obj = Propensity(
        granularity=PropensityGranularity.GLOBAL,
        knowledge=PropensityKnowledge.DESIGN_KNOWN,
        p_global=0.7,
    )
    assert obj.p_global == 0.7


def test_p_global_dict_not_empty():
    with pytest.raises(
        ValueError,
        match="p_global dict must not be empty",
    ):
        Propensity(
            granularity=PropensityGranularity.GLOBAL,
            knowledge=PropensityKnowledge.DESIGN_KNOWN,
            p_global={},
        )


def test_p_global_dict_values_in_range():
    with pytest.raises(
        ValueError,
        match=r"p_global\[A] must be between 0 and 1",
    ):
        Propensity(
            granularity=PropensityGranularity.GLOBAL,
            knowledge=PropensityKnowledge.DESIGN_KNOWN,
            p_global={"A": 1.2, "B": -0.2},
        )


def test_p_global_dict_probabilities_must_sum_to_one():
    with pytest.raises(
        ValueError,
        match="Sum of p_global probabilities must be 1.0",
    ):
        Propensity(
            granularity=PropensityGranularity.GLOBAL,
            knowledge=PropensityKnowledge.DESIGN_KNOWN,
            p_global={"A": 0.4, "B": 0.4},
        )


def test_p_global_dict_valid_configuration():
    obj = Propensity(
        granularity=PropensityGranularity.GLOBAL,
        knowledge=PropensityKnowledge.DESIGN_KNOWN,
        p_global={"A": 0.3, "B": 0.7},
    )
    assert obj.p_global == {"A": 0.3, "B": 0.7}
