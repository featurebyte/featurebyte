"""
Tests for BatchFeature schema
"""
from featurebyte.schema.feature import BatchFeatureCreate, FeatureCreate


def check_batch_feature_create(features):
    """Check BatchFeature schema"""
    name_to_feat_payload = {}
    for feature in features:
        name_to_feat_payload[feature.name] = FeatureCreate(**feature.json_dict())

    batch_feat_create = BatchFeatureCreate.create(features=list(name_to_feat_payload.values()))
    for feat_create in batch_feat_create.iterate_features():
        # check post-processed feature payload is consistent with input feature payload
        input_feat_create = name_to_feat_payload.pop(feat_create.name)
        assert feat_create.id == input_feat_create.id
        assert feat_create.name == input_feat_create.name
        assert feat_create.tabular_source == input_feat_create.tabular_source

        # check post-processed feature ref is consistent with input feature ref
        input_ref = input_feat_create.graph.node_name_to_ref[input_feat_create.node_name]
        output_ref = feat_create.graph.node_name_to_ref[feat_create.node_name]
        assert input_ref == output_ref

    # check input features are all iterated
    assert len(name_to_feat_payload) == 0


def test_batch_feature(float_feature, non_time_based_feature, feature_group):
    """Test BatchFeature schema"""
    check_batch_feature_create([float_feature, non_time_based_feature])
    check_batch_feature_create(list(feature_group.feature_objects.values()))
    check_batch_feature_create(
        list(feature_group.feature_objects.values()) + [non_time_based_feature]
    )
