"""
Integration tests for user-provided column features.
"""

import numpy as np
import pandas as pd
from bson import ObjectId

import featurebyte as fb
from featurebyte import Feature, FeatureList
from featurebyte.enum import DBVarType, FeatureType
from featurebyte.models.context import UserProvidedColumn
from featurebyte.query_graph.node.schema import DummyTableDetails


def make_unique(name):
    """Make name unique by appending ObjectId"""
    return f"{name}_{str(ObjectId())}"


def test_user_provided_feature_save_and_list(event_table, user_entity):
    """
    Test that a user-provided feature can be created from a Context, saved,
    and that listing correctly filters by context.
    """
    event_view = event_table.get_view()

    # Create a regular warehouse-backed feature
    feat_name = make_unique("amount_sum_7d")
    regular_feature = event_view.groupby("ÜSER ID").aggregate_over(
        value_column="ÀMOUNT",
        method="sum",
        windows=["7d"],
        feature_names=[feat_name],
    )[feat_name]
    regular_feature.save()

    # Create a Context with user-provided columns
    context_name = make_unique("loan_context")
    context = fb.Context.create(
        name=context_name,
        primary_entity=[user_entity.name],
        user_provided_columns=[
            UserProvidedColumn(
                name="annual_income",
                dtype=DBVarType.FLOAT,
                feature_type=FeatureType.NUMERIC,
                description="Annual income",
            ),
            UserProvidedColumn(
                name="credit_score", dtype=DBVarType.INT, feature_type=FeatureType.NUMERIC
            ),
        ],
    )

    # Create a user-provided feature from the context
    income_feature = context.get_user_provided_feature("annual_income")
    assert income_feature.name == "annual_income"
    assert income_feature.dtype == DBVarType.FLOAT
    assert income_feature.context_id == context.id
    assert income_feature.tabular_source.table_details == DummyTableDetails()

    # Save the user-provided feature
    income_feature.name = "annual_income_feature"
    income_feature.save()

    # Verify listing without context excludes the user-provided feature
    features_no_context = Feature.list(include_id=True)
    feature_names = features_no_context["name"].tolist()
    assert feat_name in feature_names
    assert "annual_income_feature" not in feature_names

    # Verify listing with context includes both regular and user-provided features
    features_with_context = Feature.list(include_id=True, context=context_name)
    feature_names_with_ctx = features_with_context["name"].tolist()
    assert feat_name in feature_names_with_ctx
    assert "annual_income_feature" in feature_names_with_ctx

    # Test preview user provided feature
    preview_df = income_feature.preview(
        pd.DataFrame([
            {"POINT_IN_TIME": "2001-11-15 10:00:00", "üser id": 1, "annual_income": 100},
            {"POINT_IN_TIME": "2001-11-15 10:00:00", "üser id": 2, "annual_income": 200},
        ])
    )
    assert preview_df["annual_income_feature"].tolist() == preview_df["annual_income"].tolist()


def test_user_provided_feature_in_feature_list(event_table, user_entity):
    """
    Test that a user-provided feature can be combined with regular features
    in a FeatureList, and that listing correctly filters by context.
    """
    event_view = event_table.get_view()

    # Create a Context with user-provided columns
    context_name = make_unique("scoring_context")
    context = fb.Context.create(
        name=context_name,
        primary_entity=[user_entity.name],
        user_provided_columns=[
            UserProvidedColumn(
                name="risk_score", dtype=DBVarType.FLOAT, feature_type=FeatureType.NUMERIC
            ),
        ],
    )

    # Create a regular feature
    reg_feat_name = make_unique("amount_avg_7d")
    regular_feature = event_view.groupby("ÜSER ID").aggregate_over(
        value_column="ÀMOUNT",
        method="avg",
        windows=["7d"],
        feature_names=[reg_feat_name],
    )[reg_feat_name]
    regular_feature.save()

    # Create a user-provided feature
    risk_feature = context.get_user_provided_feature("risk_score")
    risk_feature.name = "risk_score_feature"
    risk_feature.save()

    # Create a feature that uses both
    feature_a = risk_feature * regular_feature
    feature_a.name = "feature_a"
    feature_a.save()

    # Combine into a FeatureList and save
    fl_name = make_unique("scoring_features")
    feature_list = FeatureList([regular_feature, risk_feature, feature_a], name=fl_name)
    feature_list.save()

    # Verify FeatureList listing without context excludes context-specific lists
    fl_no_context = FeatureList.list(include_id=True)
    fl_names = fl_no_context["name"].tolist()
    assert fl_name not in fl_names

    # Verify FeatureList listing with context includes it
    fl_with_context = FeatureList.list(include_id=True, context=context_name)
    fl_names_with_ctx = fl_with_context["name"].tolist()
    assert fl_name in fl_names_with_ctx

    # Feature list preview
    features_df = feature_list.compute_historical_features(
        observation_set=pd.DataFrame([
            {"POINT_IN_TIME": "2001-11-15 10:00:00", "üser id": 1, "risk_score": 3},
            {"POINT_IN_TIME": "2001-11-15 10:00:00", "üser id": 2, "risk_score": 7},
            {"POINT_IN_TIME": "2001-11-15 10:00:00", "üser id": 3, "risk_score": 1},
        ])
    )
    assert features_df.shape == (3, 6)
    assert np.allclose(
        features_df["feature_a"], features_df["risk_score_feature"] * features_df[reg_feat_name]
    )


def test_user_provided_features_different_contexts(user_entity):
    """
    Test that features from different contexts with the same column name
    can coexist without conflict.
    """
    # Create two contexts with the same column name
    context_a_name = make_unique("context_a")
    context_a = fb.Context.create(
        name=context_a_name,
        primary_entity=[user_entity.name],
        user_provided_columns=[
            UserProvidedColumn(
                name="score", dtype=DBVarType.FLOAT, feature_type=FeatureType.NUMERIC
            ),
        ],
    )

    context_b_name = make_unique("context_b")
    context_b = fb.Context.create(
        name=context_b_name,
        primary_entity=[user_entity.name],
        user_provided_columns=[
            UserProvidedColumn(name="score", dtype=DBVarType.INT, feature_type=FeatureType.NUMERIC),
        ],
    )

    # Create features from both contexts with the same name
    feature_a = context_a.get_user_provided_feature("score")
    feature_b = context_b.get_user_provided_feature("score")

    # Both should be saveable without conflict
    feature_a.save()
    feature_b.save()

    # Listing with context_a shows feature_a but not feature_b
    features_a = Feature.list(include_id=True, context=context_a_name)
    score_features_a = features_a[features_a["name"] == "score"]
    assert len(score_features_a) == 1

    features_b = Feature.list(include_id=True, context=context_b_name)
    score_features_b = features_b[features_b["name"] == "score"]
    assert len(score_features_b) == 1

    # Neither shows up without context
    features_none = Feature.list(include_id=True)
    score_features_none = features_none[features_none["name"] == "score"]
    assert len(score_features_none) == 0
