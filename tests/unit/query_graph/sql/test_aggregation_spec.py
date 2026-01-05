"""Test aggregation spec functionality."""

from sqlglot.expressions import select

from featurebyte.query_graph.sql.specs import AggregationSource, AggregationSpec, AggregationType


class AggregationSpecForTesting(AggregationSpec):
    """Concrete implementation of AggregationSpec for testing."""

    def __init__(self, *args, test_args=None, **kwargs):
        self.test_args = test_args or []
        super().__init__(*args, **kwargs)

    @property
    def agg_result_name(self):
        return self.construct_agg_result_name(*self.test_args)

    @property
    def aggregation_type(self):
        return AggregationType.WINDOW

    def get_source_hash_parameters(self):
        return {"test": "params"}

    @classmethod
    def construct_specs(cls, *args, **kwargs):
        return []


def create_test_aggregation_source(query_node_name="test_node"):
    """Create a test aggregation source."""
    return AggregationSource(expr=select("*").from_("test_table"), query_node_name=query_node_name)


def test_construct_agg_result_name_long_clipped():
    """Test that long aggregation result names are clipped with hash."""
    # Create aggregation source with very long name
    long_node_name = "extremely_long_query_node_name_that_makes_everything_much_longer_than_usual"
    aggregation_source = create_test_aggregation_source(long_node_name)

    # Use many long serving names
    long_serving_names = [
        "very_long_serving_name_that_goes_on_and_on",
        "another_extremely_long_serving_name_with_lots_of_characters",
        "yet_another_super_long_serving_name_that_makes_everything_longer",
    ]

    # Long arguments that will make the name exceed 200 chars
    long_args = [
        "window_parameter_with_very_long_name_w86400",
        "aggregation_function_with_extremely_long_name_sum",
        "aggregation_id_that_is_also_very_long_and_descriptive",
        "additional_parameter_that_makes_name_even_longer",
    ]

    spec = AggregationSpecForTesting(
        node_name="test_node",
        feature_name="test_feature",
        entity_ids=None,
        serving_names=long_serving_names,
        serving_names_mapping=None,
        agg_result_name_include_serving_names=True,
        aggregation_source=aggregation_source,
        is_deployment_sql=False,
        test_args=long_args,
    )

    result = spec.agg_result_name

    # Should be clipped to exactly 200 characters
    assert len(result) == 200

    # Should end with 64-character hash
    assert result[-64:].isalnum()  # SHA256 hex digest
    assert result[-65] == "_"  # Separator before hash


def test_construct_agg_result_name_deterministic():
    """Test that name clipping is deterministic."""
    long_node_name = "long_node_name" * 20  # Very long name
    aggregation_source = create_test_aggregation_source(long_node_name)

    long_args = ["long_arg"] * 15

    spec = AggregationSpecForTesting(
        node_name="test_node",
        feature_name="test_feature",
        entity_ids=None,
        serving_names=["long_serving_name"] * 10,
        serving_names_mapping=None,
        agg_result_name_include_serving_names=True,
        aggregation_source=aggregation_source,
        is_deployment_sql=False,
        test_args=long_args,
    )

    # Generate name twice
    result1 = spec.agg_result_name
    result2 = spec.agg_result_name

    # Should be identical (deterministic)
    assert result1 == result2
    assert len(result1) == 200


def test_construct_agg_result_name_uniqueness():
    """Test that different inputs produce different clipped names."""
    # Create two specs with different configurations
    aggregation_source1 = create_test_aggregation_source("node_name" * 20)
    aggregation_source2 = create_test_aggregation_source("different_node_name" * 20)

    long_args = ["arg"] * 20

    spec1 = AggregationSpecForTesting(
        node_name="test_node",
        feature_name="test_feature",
        entity_ids=None,
        serving_names=["serving1"] * 20,
        serving_names_mapping=None,
        agg_result_name_include_serving_names=True,
        aggregation_source=aggregation_source1,
        is_deployment_sql=False,
        test_args=long_args,
    )

    spec2 = AggregationSpecForTesting(
        node_name="test_node",
        feature_name="test_feature",
        entity_ids=None,
        serving_names=["serving2"] * 20,
        serving_names_mapping=None,
        agg_result_name_include_serving_names=True,
        aggregation_source=aggregation_source2,
        is_deployment_sql=False,
        test_args=long_args,
    )

    result1 = spec1.agg_result_name
    result2 = spec2.agg_result_name

    # Both should be clipped to 200 chars but be different
    assert len(result1) == 200
    assert len(result2) == 200
    assert result1 != result2
