"""
This module contains Feature list store info related models
"""
from typing import Any, Dict, List, Optional, Union

from abc import abstractmethod  # pylint: disable=wrong-import-order

from pydantic import Field, parse_obj_as

from featurebyte.enum import SourceType, SpecialColumnName
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.query_graph.node.metadata.sdk_code import (
    CodeGenerator,
    ExpressionStr,
    VariableNameStr,
)


class BaseStoreInfo(FeatureByteBaseModel):
    """
    Base class for store info
    """

    type: SourceType

    @classmethod
    @abstractmethod
    def create(cls, features: List[FeatureModel], feature_store: FeatureStoreModel) -> "StoreInfo":
        """
        Create store info for a feature list

        Parameters
        ----------
        features: List[FeatureModel]
            List of features
        feature_store: FeatureStoreModel
            Feature store model used by the features & feature list

        Returns
        -------
        StoreInfo
        """


class DataBricksFeatureLookup(FeatureByteBaseModel):
    """
    DataBricks feature lookup
    """

    table_name: str
    lookup_key: List[str]
    timestamp_lookup_key: Optional[str]
    lookback_window: Optional[str]
    feature_names: List[str]
    rename_outputs: Dict[str, str]

    def __repr_name__(self) -> str:
        return "FeatureLookup"


class DataBricksFeatureFunction(FeatureByteBaseModel):
    """
    DataBricks feature function
    """

    udf_name: str
    input_bindings: Dict[str, str]
    output_name: str

    def __repr_name__(self) -> str:
        return "FeatureFunction"


class DataBricksStoreInfo(BaseStoreInfo):
    """
    DataBricks store info
    """

    type: SourceType = Field(default=SourceType.DATABRICKS, const=True)
    databricks_sdk_version: str = Field(default="0.16.3")
    feature_specs: List[Union[DataBricksFeatureLookup, DataBricksFeatureFunction]]
    exclude_columns: List[str]

    @property
    def feature_specs_definition(self) -> str:
        """
        Feature specs definition

        Returns
        -------
        str
        """
        code_gen = CodeGenerator(template="databricks_feature_spec.tpl")
        feature_specs = ExpressionStr(self.feature_specs)
        code_gen.add_statements(
            [
                (VariableNameStr("features"), feature_specs),
                (VariableNameStr("exclude_columns"), ExpressionStr(self.exclude_columns)),
            ]
        )
        codes = code_gen.generate(
            to_format=True,
            remove_unused_variables=False,
            databricks_sdk_version=self.databricks_sdk_version,
        )
        return codes

    @classmethod
    def create(
        cls, features: List[FeatureModel], feature_store: FeatureStoreModel
    ) -> "DataBricksStoreInfo":
        table_name_to_feature_lookup = {}
        feature_functions = []
        exclude_columns = set()
        for feature in features:
            offline_store_info = feature.offline_store_info
            assert offline_store_info is not None, "Feature does not have offline store info"
            if offline_store_info.udf_info:
                input_bindings = {
                    input_arg_name: input_info.column_name
                    for input_arg_name, input_info in offline_store_info.udf_info.input_var_name_to_info.items()
                }
                feature_functions.append(
                    DataBricksFeatureFunction(
                        udf_name=offline_store_info.udf_info.sql_function_name,
                        input_bindings=input_bindings,
                        output_name=feature.name,
                    )
                )

            entity_id_to_serving_name = {
                info.entity_id: info.serving_name for info in offline_store_info.serving_names_info
            }
            for ingest_query in offline_store_info.extract_offline_store_ingest_query_graphs():
                table_name = ingest_query.offline_store_table_name
                if table_name not in table_name_to_feature_lookup:
                    timestamp_lookup_key = None
                    if ingest_query.has_ttl:
                        timestamp_lookup_key = SpecialColumnName.POINT_IN_TIME.value

                    table_name_to_feature_lookup[table_name] = DataBricksFeatureLookup(
                        table_name=ingest_query.offline_store_table_name,
                        lookup_key=[
                            entity_id_to_serving_name[entity_id]
                            for entity_id in ingest_query.primary_entity_ids
                        ],
                        timestamp_lookup_key=timestamp_lookup_key,
                        lookback_window=None,
                        feature_names=[],
                        rename_outputs={},
                    )

                column_name = ingest_query.output_column_name
                feature_lookup = table_name_to_feature_lookup[table_name]
                feature_lookup.feature_names.append(column_name)
                if offline_store_info.udf_info:
                    exclude_columns.add(column_name)
                else:
                    assert feature.name is not None, "Feature does not have a name"
                    feature_lookup.rename_outputs[column_name] = feature.name

        feature_specs: List[Union[DataBricksFeatureLookup, DataBricksFeatureFunction]] = []
        for feature_lookup in table_name_to_feature_lookup.values():
            feature_specs.append(feature_lookup)

        for feature_function in feature_functions:
            feature_specs.append(feature_function)
        return cls(feature_specs=feature_specs, exclude_columns=sorted(exclude_columns))


StoreInfo = DataBricksStoreInfo


def construct_store_info(**kwargs: Any) -> StoreInfo:
    """
    Construct store info based on input keyword arguments

    Parameters
    ----------
    **kwargs: Any
        Keyword arguments used to construct the StoreInfo object

    Returns
    -------
    StoreInfo
    """
    store_info = parse_obj_as(StoreInfo, kwargs)
    return store_info
