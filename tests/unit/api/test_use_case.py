"""
Unit test for UseCase class
"""
from featurebyte import ObservationTable, UseCase


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
        name="test_use_case_1",
        target_name=float_target.name,
        context_name=context.name,
        description="test_use_case_1 description",
    )

    # Test get use case and verify attributes
    retrieved_use_case = UseCase.get_by_id(use_case.id)
    assert retrieved_use_case.name == "test_use_case_1"
    assert retrieved_use_case.target_id == float_target.id
    assert retrieved_use_case.context_id == context.id
    assert retrieved_use_case.description == "test_use_case_1 description"
    assert retrieved_use_case.target.name == float_target.name
    assert retrieved_use_case.context.name == context.name

    # Test list use cases
    use_case_df = UseCase.list()
    assert len(use_case_df) == 1
    assert use_case_df.iloc[0]["id"] == str(use_case.id)
    assert use_case_df.iloc[0]["name"] == use_case.name


def test_add_and_remove_observation_table(use_case, target_table):
    """
    Test UseCase.add_observation_table and UseCase.remove_observation_table method
    """

    use_case.add_observation_table(target_table.name)
    retrieved_use_case = UseCase.get_by_id(use_case.id)
    assert retrieved_use_case.name == "test_use_case"
    assert retrieved_use_case.description == "test_use_case description"

    obs_table_df = retrieved_use_case.list_observation_tables()
    assert len(obs_table_df) == 1
    assert obs_table_df.iloc[0]["id"] == str(target_table.id)
    assert obs_table_df.iloc[0]["name"] == "my_target_table"

    retrieved_use_case.remove_observation_table(target_table.name)
    obs_table_df = retrieved_use_case.list_observation_tables()
    assert len(obs_table_df) == 0


def test_update_default_preview_table(use_case, target_table):
    """
    Test UseCase.update_default_preview_table method
    """

    use_case.update_default_preview_table(target_table.name)

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

    use_case.update_default_eda_table(target_table.name)

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


def test_list_deployments(use_case, target_table, deployment):
    """
    Test UseCase.list_deployments( method
    """

    retrieved_use_case = UseCase.get(use_case.name)
    deployments = retrieved_use_case.list_deployments()
    assert len(deployments) == 1
    assert deployments.iloc[0]["name"] == deployment.name


def test_info(use_case, target_table, cust_id_entity):
    """
    Test UseCase.info method
    """

    use_case.update_default_eda_table(target_table.name)
    use_case.update_default_preview_table(target_table.name)

    use_case_info = use_case.info()
    assert use_case_info["name"] == use_case.name
    assert use_case_info["description"] == use_case.description
    assert use_case_info["default_eda_table"] == target_table.name
    assert use_case_info["default_preview_table"] == target_table.name
    assert use_case_info["primary_entities"] == [
        {
            "name": cust_id_entity.name,
            "serving_names": cust_id_entity.serving_names,
            "catalog_name": "catalog",
        }
    ]


def test_observation_table_with_multiple_use_cases(use_case, target_table, float_target, context):
    """
    Test UseCase.add_observation_table method
    """

    use_case.add_observation_table(target_table.name)
    retrieved_use_case = UseCase.get_by_id(use_case.id)
    obs_table_df = retrieved_use_case.list_observation_tables()
    assert len(obs_table_df) == 1
    assert obs_table_df.iloc[0]["id"] == str(target_table.id)
    assert obs_table_df.iloc[0]["name"] == "my_target_table"

    use_case_2 = UseCase.create(
        name="test_use_case_2",
        target_name=float_target.name,
        context_name=context.name,
        description="test_use_case_2 description",
    )
    use_case_2.add_observation_table(target_table.name)
    retrieved_use_case_2 = UseCase.get_by_id(use_case_2.id)
    obs_table_df_2 = retrieved_use_case_2.list_observation_tables()
    assert len(obs_table_df_2) == 1
    assert obs_table_df_2.iloc[0]["id"] == str(target_table.id)
    assert obs_table_df_2.iloc[0]["name"] == "my_target_table"

    retrieved_obs_table = ObservationTable.get_by_id(target_table.id)
    assert len(retrieved_obs_table.use_case_ids) == 2
    assert set(retrieved_obs_table.use_case_ids) == {retrieved_use_case.id, retrieved_use_case_2.id}
