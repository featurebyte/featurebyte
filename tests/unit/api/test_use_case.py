"""
Unit test for UseCase class
"""
import pytest

from featurebyte import UseCase


@pytest.fixture(name="use_case")
def use_case_fixture(catalog, float_target):
    """
    UseCase fixture
    """
    _ = catalog
    float_target.save()

    use_case = UseCase(
        name="test_use_case",
        target=float_target,
        description="test_use_case description",
    )
    previous_id = use_case.id
    assert use_case.saved is False
    use_case.save()
    assert use_case.saved is True
    assert use_case.id == previous_id
    yield use_case


def test_create_use_case(catalog, float_target):
    """
    Test UseCase.create method
    """
    _ = catalog

    float_target.save()

    use_case = UseCase.create(
        name="test_use_case", target=float_target, description="test_use_case description"
    )

    retrieved_use_case = UseCase.get_by_id(use_case.id)
    assert retrieved_use_case.name == "test_use_case"
    assert retrieved_use_case.target.id == float_target.id
    assert retrieved_use_case.description == "test_use_case description"


def test_add_observation_table(use_case, target_table):
    """
    Test UseCase.add_observation_table method
    """

    use_case.add_observation_table(target_table.id)
    retrieved_use_case = UseCase.get_by_id(use_case.id)
    assert retrieved_use_case.name == "test_use_case"
    assert retrieved_use_case.description == "test_use_case description"
    obs_table_list = retrieved_use_case.list_observation_tables()
    assert len(obs_table_list) == 1
    assert target_table.id == obs_table_list[0].id
