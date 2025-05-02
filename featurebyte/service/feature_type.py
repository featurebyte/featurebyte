"""
Feature type service used to determine the type of the feature
"""

from __future__ import annotations

from functools import cached_property
from typing import Any

from featurebyte import AggFunc
from featurebyte.enum import DBVarType, FeatureType
from featurebyte.models import FeatureModel
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.metadata.operation import GroupOperationStructure
from featurebyte.routes.common.feature_metadata_extractor import FeatureOrTargetMetadataExtractor


class FeatureTypeService:
    """
    Feature type service
    """

    def __init__(
        self,
        feature_or_target_metadata_extractor: FeatureOrTargetMetadataExtractor,
    ):
        self.feature_or_target_metadata_extractor = feature_or_target_metadata_extractor

    @cached_property
    def feature_type_to_valid_dtype(self) -> dict[FeatureType, set[DBVarType]]:
        """
        Mapping of feature type to valid dtypes

        Returns
        -------
        dict[FeatureType, set[DBVarType]]
        """
        return {
            FeatureType.CATEGORICAL: FeatureType.valid_categorical_dtypes(),
            FeatureType.NUMERIC: FeatureType.valid_numeric_dtypes(),
            FeatureType.TEXT: FeatureType.valid_text_dtypes(),
            FeatureType.DICT: FeatureType.valid_dict_dtypes(),
            FeatureType.EMBEDDING: FeatureType.valid_embedding_dtypes(),
        }

    @cached_property
    def dtype_to_valid_feature_type(self) -> dict[DBVarType, set[FeatureType]]:
        """
        Mapping of dtype to valid feature type

        Returns
        -------
        dict[DBVarType, set[FeatureType]]
        """
        dtype_to_feature_type: dict[DBVarType, set[FeatureType]] = {}
        for feature_type, valid_dtypes in self.feature_type_to_valid_dtype.items():
            for dtype in valid_dtypes:
                if dtype not in dtype_to_feature_type:
                    dtype_to_feature_type[dtype] = set()
                dtype_to_feature_type[dtype].add(feature_type)

        # sanity check on more than one feature type for a dtype
        # if adding more than one feature type for a dtype, make sure to update the logic in detect_*_feature_type
        more_than_one = set()
        for dtype, feature_types in dtype_to_feature_type.items():
            if len(feature_types) > 1:
                more_than_one.add(dtype)
        assert more_than_one == {
            DBVarType.FLOAT,
            DBVarType.INT,
            DBVarType.VARCHAR,
            DBVarType.CHAR,
            DBVarType.BOOL,
        }
        return dtype_to_feature_type

    def validate_feature_type(self, feature: FeatureModel, feature_type: FeatureType) -> None:
        """
        Validate the type of the feature against the given feature type

        Parameters
        ----------
        feature: FeatureModel
            Feature model
        feature_type: FeatureType
            Feature type

        Raises
        ------
        ValueError
            If the feature type is not valid for the feature
        """
        if feature_type in self.feature_type_to_valid_dtype:
            valid_dtypes = self.feature_type_to_valid_dtype[feature_type]
            if feature.dtype not in valid_dtypes:
                raise ValueError(
                    f"Feature {feature.name} has dtype {feature.dtype} which is not valid for {feature_type}"
                )

    @staticmethod
    def detect_categorical_by_operation_structure(op_struct: GroupOperationStructure) -> bool:
        """
        Detect the feature type of categorical feature based on operation structure

        Parameters
        ----------
        op_struct: GroupOperationStructure
            Operation structure

        Returns
        -------
        bool
        """
        # if post aggregation not present, check if any aggregation is lookup
        if op_struct.post_aggregation is None:
            for agg in op_struct.aggregations:
                if agg.aggregation_type in {NodeType.LOOKUP, NodeType.LOOKUP_TARGET}:
                    # if lookup is present, treat it as categorical
                    return True
                if agg.method == AggFunc.LATEST:
                    # latest is categorical
                    return True
        else:
            transforms = op_struct.post_aggregation.transforms
            if transforms:
                last_transform = transforms[-1]
                categorical_transforms = [
                    "timedelta_extract",
                    "count_dict_transform(transform_type='most_frequent')",
                    "count_dict_transform(transform_type='key_with_highest_value')",
                    "count_dict_transform(transform_type='key_with_lowest_value')",
                ]
                for cat_transform in categorical_transforms:
                    if cat_transform in last_transform:
                        # check if the last transform is categorical
                        return True
        return False

    async def detect_float_feature_type(
        self, op_struct: GroupOperationStructure, metadata: dict[str, Any]
    ) -> FeatureType:
        """
        Detect the feature type of float feature

        Parameters
        ----------
        op_struct: GroupOperationStructure
            Operation structure
        metadata: dict[str, Any]
            Feature metadata

        Returns
        -------
        FeatureType
        """
        _ = metadata
        _ = self, op_struct, metadata
        # by default, float is considered as numeric
        return FeatureType.NUMERIC

    async def detect_int_feature_type(
        self, op_struct: GroupOperationStructure, metadata: dict[str, Any]
    ) -> FeatureType:
        """
        Detect the feature type of integer feature

        Parameters
        ----------
        op_struct: GroupOperationStructure
            Operation structure
        metadata: dict[str, Any]
            Feature metadata

        Returns
        -------
        FeatureType
        """
        _ = metadata
        if self.detect_categorical_by_operation_structure(op_struct):
            return FeatureType.CATEGORICAL
        return FeatureType.NUMERIC

    async def detect_varchar_feature_type(
        self, op_struct: GroupOperationStructure, metadata: dict[str, Any]
    ) -> FeatureType:
        """
        Detect the feature type of varchar feature

        Parameters
        ----------
        op_struct: GroupOperationStructure
            Operation structure
        metadata: dict[str, Any]
            Feature metadata

        Returns
        -------
        FeatureType
        """
        _ = metadata
        if self.detect_categorical_by_operation_structure(op_struct):
            return FeatureType.CATEGORICAL
        return FeatureType.TEXT

    async def detect_bool_feature_type(
        self, op_struct: GroupOperationStructure, metadata: dict[str, Any]
    ) -> FeatureType:
        """
        Detect the feature type of boolean feature

        Parameters
        ----------
        op_struct: GroupOperationStructure
            Operation structure
        metadata: dict[str, Any]
            Feature metadata

        Returns
        -------
        FeatureType
        """
        _ = self, op_struct, metadata
        # by default, boolean is considered as categorical
        return FeatureType.CATEGORICAL

    async def detect_feature_type_from(
        self, dtype: DBVarType, op_struct: GroupOperationStructure, metadata: dict[str, Any]
    ) -> FeatureType:
        """
        Detect the type of the feature given the feature information

        Parameters
        ----------
        dtype: DBVarType
            Data type
        op_struct: GroupOperationStructure
            Operation structure
        metadata: dict[str, Any]
            Feature metadata

        Returns
        -------
        FeatureType

        Raises
        ------
        ValueError
            If the feature type is not valid for the feature
        """
        if dtype not in self.dtype_to_valid_feature_type:
            return FeatureType.OTHERS

        valid_feature_types = self.dtype_to_valid_feature_type[dtype]
        if len(valid_feature_types) == 1:
            return next(iter(valid_feature_types))

        if dtype == DBVarType.FLOAT:
            return await self.detect_float_feature_type(op_struct=op_struct, metadata=metadata)
        if dtype == DBVarType.INT:
            return await self.detect_int_feature_type(op_struct=op_struct, metadata=metadata)
        if dtype in {DBVarType.VARCHAR, DBVarType.CHAR}:
            return await self.detect_varchar_feature_type(op_struct=op_struct, metadata=metadata)
        if dtype == DBVarType.BOOL:
            return await self.detect_bool_feature_type(op_struct=op_struct, metadata=metadata)

        # This should never happen
        raise ValueError(f"Feature type detection not implemented for {dtype}")

    async def detect_feature_type(self, feature: FeatureModel) -> FeatureType:
        """
        Detect the type of the feature

        Parameters
        ----------
        feature: FeatureModel
            Feature model

        Returns
        -------
        FeatureType
        """
        op_struct, metadata = await self.feature_or_target_metadata_extractor.extract_from_object(
            obj=feature
        )
        feature_type = await self.detect_feature_type_from(
            dtype=feature.dtype, op_struct=op_struct, metadata=metadata
        )
        self.validate_feature_type(feature=feature, feature_type=feature_type)
        return feature_type
