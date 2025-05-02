"""Test feature type service"""

from unittest.mock import Mock

import pytest

from featurebyte import AggFunc
from featurebyte.enum import DBVarType, FeatureType
from featurebyte.query_graph.enum import NodeType


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "dtype,expected",
    [
        (DBVarType.BOOL, FeatureType.CATEGORICAL),
        # (DBVarType.CHAR, FeatureType.CATEGORICAL),
        (DBVarType.DATE, FeatureType.OTHERS),
        (DBVarType.FLOAT, FeatureType.NUMERIC),
        # (DBVarType.INT, FeatureType.NUMERIC),
        (DBVarType.TIME, FeatureType.OTHERS),
        (DBVarType.TIMESTAMP, FeatureType.OTHERS),
        (DBVarType.TIMESTAMP_TZ, FeatureType.OTHERS),
        # (DBVarType.VARCHAR, FeatureType.TEXT),
        (DBVarType.ARRAY, FeatureType.OTHERS),
        (DBVarType.DICT, FeatureType.OTHERS),
        (DBVarType.TIMEDELTA, FeatureType.NUMERIC),
        (DBVarType.EMBEDDING, FeatureType.EMBEDDING),
        (DBVarType.FLAT_DICT, FeatureType.OTHERS),
        (DBVarType.TIMESTAMP_TZ_TUPLE, FeatureType.OTHERS),
        (DBVarType.UNKNOWN, FeatureType.OTHERS),
        (DBVarType.BINARY, FeatureType.OTHERS),
        (DBVarType.VOID, FeatureType.OTHERS),
        (DBVarType.MAP, FeatureType.OTHERS),
        (DBVarType.OBJECT, FeatureType.DICT),
        (DBVarType.STRUCT, FeatureType.OTHERS),
    ],
)
async def test_detect_feature_type(app_container, dtype, expected):
    """Test detect feature type"""
    feature_type_service = app_container.feature_type_service
    op_struct = Mock()
    feature_type = await feature_type_service.detect_feature_type_from(dtype, op_struct, {})
    assert feature_type == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "dtype,expected",
    [
        (DBVarType.CHAR, FeatureType.TEXT),
        (DBVarType.VARCHAR, FeatureType.TEXT),
        (DBVarType.INT, FeatureType.NUMERIC),
    ],
)
async def test_detect_feature_type__categorical(app_container, dtype, expected):
    """Test detect feature type - categorical"""
    feature_type_service = app_container.feature_type_service

    # check non-categorical
    op_struct = Mock()
    op_struct.post_aggregation = None
    op_struct.aggregations = []
    feature_type = await feature_type_service.detect_feature_type_from(dtype, op_struct, {})
    assert feature_type == expected

    # check categorical
    # case 1: lookup node type
    aggregation = Mock()
    aggregation.aggregation_type = NodeType.LOOKUP
    op_struct.aggregations = [aggregation]
    feature_type = await feature_type_service.detect_feature_type_from(dtype, op_struct, {})
    assert feature_type == FeatureType.CATEGORICAL

    # case 2: latest aggregation method
    aggregation.aggregation_type = NodeType.GROUPBY
    aggregation.method = AggFunc.LATEST
    feature_type = await feature_type_service.detect_feature_type_from(dtype, op_struct, {})
    assert feature_type == FeatureType.CATEGORICAL

    # case 3: post aggregation with certain transformation
    post_aggregation = Mock()
    post_aggregation.transforms = ["timedelta_extract"]
    op_struct.post_aggregation = post_aggregation
    feature_type = await feature_type_service.detect_feature_type_from(dtype, op_struct, {})
    assert feature_type == FeatureType.CATEGORICAL
