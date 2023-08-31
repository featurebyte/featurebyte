"""
Unit test for UseCase class
"""
import pytest

from featurebyte import UseCase


@pytest.fixture(name="use_case")
def use_case_fixture(catalog, float_target, context):
    """
    UseCase fixture
    """
    _ = catalog
    float_target.save()

    use_case = UseCase(
        name="test_use_case",
        target_id=float_target.id,
        context_id=context.id,
        description="test_use_case description",
    )
    previous_id = use_case.id
    assert use_case.saved is False
    use_case.save()
    assert use_case.saved is True
    assert use_case.id == previous_id
    yield use_case


def test_create_use_case(catalog, float_target, context):
    """
    Test UseCase.create method
    """
    _ = catalog

    if not context.saved:
        context.save()

    if not float_target.saved:
        float_target.save()

    use_case = UseCase.create(
        name="test_use_case",
        target_name=float_target.name,
        context_name=context.name,
        description="test_use_case description",
    )

    # Test get use case and verify attributes
    retrieved_use_case = UseCase.get_by_id(use_case.id)
    assert retrieved_use_case.name == "test_use_case"
    assert retrieved_use_case.target_id == float_target.id
    assert retrieved_use_case.context_id == context.id
    assert retrieved_use_case.description == "test_use_case description"
    assert retrieved_use_case.target.name == float_target.name
    assert retrieved_use_case.context.name == context.name

    # Test list use cases
    use_case_df = UseCase.list()
    assert len(use_case_df) == 1
    assert use_case_df.iloc[0]["id"] == str(use_case.id)
    assert use_case_df.iloc[0]["name"] == use_case.name


def test_add_observation_table(use_case, target_table):
    """
    Test UseCase.add_observation_table method
    """

    use_case.add_observation_table(target_table)
    retrieved_use_case = UseCase.get_by_id(use_case.id)
    assert retrieved_use_case.name == "test_use_case"
    assert retrieved_use_case.description == "test_use_case description"

    obs_table_df = retrieved_use_case.list_observation_tables()
    assert len(obs_table_df) == 1
    assert obs_table_df.iloc[0]["id"] == str(target_table.id)
    assert obs_table_df.iloc[0]["name"] == "my_target_table"


def test_update_default_preview_table(use_case, target_table):
    """
    Test UseCase.update_default_preview_table method
    """

    use_case.update_default_preview_table(target_table)

    use_case_df = UseCase.list()
    assert len(use_case_df) == 1
    assert use_case_df.iloc[0]["id"] == str(use_case.id)
    assert use_case_df.iloc[0]["name"] == use_case.name
    assert use_case_df.iloc[0]["default_preview_table_name"] == "my_target_table"

    retrieved_use_case = UseCase.get_by_id(use_case.id)
    obs_table_df = retrieved_use_case.list_observation_tables()
    assert len(obs_table_df) == 1
    assert obs_table_df.iloc[0]["id"] == str(target_table.id)
    assert obs_table_df.iloc[0]["name"] == "my_target_table"


def test_update_default_eda_table(use_case, target_table):
    """
    Test UseCase.update_default_eda_table method
    """

    use_case.update_default_eda_table(target_table)

    use_case_df = UseCase.list()
    assert len(use_case_df) == 1
    assert use_case_df.iloc[0]["id"] == str(use_case.id)
    assert use_case_df.iloc[0]["name"] == use_case.name
    assert use_case_df.iloc[0]["default_eda_table_name"] == "my_target_table"

    retrieved_use_case = UseCase.get(use_case.name)
    obs_table_df = retrieved_use_case.list_observation_tables()
    assert len(obs_table_df) == 1
    assert obs_table_df.iloc[0]["id"] == str(target_table.id)
    assert obs_table_df.iloc[0]["name"] == "my_target_table"
