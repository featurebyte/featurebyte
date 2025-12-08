"""
Unit test for UseCase class
"""

from featurebyte import Context, ObservationTable, Propensity, TargetNamespace, Treatment, UseCase
from featurebyte.enum import DBVarType, TargetType
from featurebyte.models.use_case import UseCaseType


def test_create_use_case_with_descriptive_target(catalog, cust_id_entity, context):
    """
    Test create use case with descriptive target
    """
    if not context.saved:
        context.save()

    target_name = "target_name"
    namespace = TargetNamespace.create(
        name=target_name,
        window="28d",
        primary_entity=[cust_id_entity.name],
        dtype=DBVarType.FLOAT,
        target_type=TargetType.REGRESSION,
    )
    assert namespace.name == target_name
    use_case = UseCase.create(
        name="test_use_case_1",
        target_name=target_name,
        context_name=context.name,
        description="test_use_case_1 description",
    )
    assert use_case.target_namespace_id == namespace.id
    assert use_case.target is None


def test_create_use_case(catalog, float_target, context):
    """
    Test UseCase.create method
    """
    _ = catalog

    if not context.saved:
        context.save()

    if not float_target.saved:
        float_target.save()
        float_target.update_target_type(TargetType.REGRESSION)

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


def test_create_use_case_with_treatment(catalog, cust_id_entity):
    """
    Test create use case with descriptive target
    """
    entity_ids = [cust_id_entity.id]
    entity_names = [cust_id_entity.name]

    observational_treatment = Treatment(
        scale="binary",
        source="observational",
        design="business-rule",
        time="static",
        time_structure="none",
        interference="none",
        treatment_values=[0, 1],
        control_value=0,
        propensity=Propensity(
            granularity="unit",
            knowledge="estimated",
        ),
    )

    context = Context.create(
        name="test_context_with_treatment",
        primary_entity=entity_names,
        description="test_description",
        treatment=observational_treatment,
    )

    target_name = "target_name"
    namespace = TargetNamespace.create(
        name=target_name,
        window="28d",
        primary_entity=[cust_id_entity.name],
        dtype=DBVarType.FLOAT,
        target_type=TargetType.REGRESSION,
    )
    assert namespace.name == target_name
    use_case = UseCase.create(
        name="test_use_case_1",
        target_name=target_name,
        context_name=context.name,
        description="test_use_case_1 description",
    )
    assert use_case.target_namespace_id == namespace.id
    assert use_case.target is None
    assert use_case.use_case_type == UseCaseType.CAUSAL


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

    # test remove default preview table
    retrieved_use_case.remove_default_preview_table()
    assert retrieved_use_case.default_preview_table is None

    retrieved_use_case = UseCase.get_by_id(use_case.id)
    assert retrieved_use_case.default_preview_table is None


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

    # test remove default preview table
    retrieved_use_case.remove_default_eda_table()
    assert retrieved_use_case.default_eda_table is None

    retrieved_use_case = UseCase.get_by_id(use_case.id)
    assert retrieved_use_case.default_eda_table is None


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
    assert use_case_info["use_case_type"] == UseCaseType.PREDICTIVE


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
