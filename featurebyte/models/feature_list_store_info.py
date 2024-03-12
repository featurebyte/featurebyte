"""
This module contains Feature list store info related models
"""
from __future__ import annotations

from typing import Dict, List, Literal, Optional, Tuple, Union
from typing_extensions import Annotated

from pydantic import Field

from featurebyte.enum import DBVarType, SpecialColumnName
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.metadata.sdk_code import (
    CodeGenerator,
    ExpressionStr,
    VariableNameStr,
    get_object_class_from_function_call,
)
from featurebyte.query_graph.node.request import RequestColumnNode
from featurebyte.query_graph.node.schema import (
    ColumnSpec,
    DatabricksDetails,
    DatabricksUnityDetails,
)
from featurebyte.query_graph.sql.entity import DUMMY_ENTITY_COLUMN_NAME

StoreInfoType = Literal["uninitialized", "snowflake", "databricks", "databricks_unity", "spark"]


class BaseStoreInfo(FeatureByteBaseModel):
    """
    Base class for store info
    """

    type: StoreInfoType
    feast_enabled: bool = Field(default=False)

    @classmethod
    def create(
        cls, features: List[FeatureModel], feature_store: FeatureStoreModel
    ) -> BaseStoreInfo:
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
        _ = features, feature_store
        return cls(feast_enabled=True)


class UninitializedStoreInfo(BaseStoreInfo):
    """
    Uninitialized store info
    """

    type: Literal["uninitialized"] = Field("uninitialized", const=True)

    @classmethod
    def create(
        cls, features: List[FeatureModel], feature_store: FeatureStoreModel
    ) -> UninitializedStoreInfo:
        return cls(feast_enabled=False)


class SnowflakeStoreInfo(BaseStoreInfo):
    """
    Snowflake store info
    """

    type: Literal["snowflake"] = Field("snowflake", const=True)


class DataBricksStoreInfo(BaseStoreInfo):
    """
    DataBricks store info
    """

    type: Literal["databricks"] = Field(default="databricks", const=True)


class SparkStoreInfo(BaseStoreInfo):
    """
    Spark store info
    """

    type: Literal["spark"] = Field(default="spark", const=True)


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

    def __repr__(self) -> str:
        repr_str = (
            f"FeatureLookup(\n"
            f"    table_name={repr(self.table_name)},\n"
            f"    lookup_key={repr(self.lookup_key)},\n"
            f"    timestamp_lookup_key={self.timestamp_lookup_key},\n"
            f"    lookback_window={repr(self.lookback_window)},\n"
            f"    feature_names={repr(self.feature_names)},\n"
            f"    rename_outputs={repr(self.rename_outputs)},\n"
            f")"
        )
        return repr_str


class DataBricksFeatureFunction(FeatureByteBaseModel):
    """
    DataBricks feature function
    """

    udf_name: str
    input_bindings: Dict[str, str]
    output_name: str

    def __repr_name__(self) -> str:
        return "FeatureFunction"


class DataBricksUnityStoreInfo(BaseStoreInfo):
    """
    DataBricks store info
    """

    type: Literal["databricks_unity"] = Field(default="databricks_unity", const=True)
    databricks_sdk_version: str = Field(default="0.16.3")
    feature_specs: List[Union[DataBricksFeatureLookup, DataBricksFeatureFunction]]
    base_dataframe_specs: List[ColumnSpec]
    exclude_columns: List[str]
    require_timestamp_lookup_key: bool

    @classmethod
    def to_spark_dtype(cls, dtype: DBVarType) -> str:
        """
        Data type to spark data type

        Parameters
        ----------
        dtype: DBVarType
            Data type to convert

        Returns
        -------
        Dict[str, str]

        Raises
        ------
        ValueError
            If dtype is not supported
        """
        type_map = {
            DBVarType.INT: "LongType",
            DBVarType.FLOAT: "DoubleType",
            DBVarType.VARCHAR: "StringType",
            DBVarType.BOOL: "BooleanType",
            DBVarType.TIMESTAMP: "TimestampType",
        }
        if dtype not in type_map:
            raise ValueError(f"Unsupported dtype: {dtype}")
        return type_map[dtype]

    def _get_base_dataframe_schema_and_import_statement(self) -> Tuple[str, str]:
        schemas = []
        required_imports = {"StructType", "StructField"}
        for column_spec in self.base_dataframe_specs:
            field_type = self.to_spark_dtype(column_spec.dtype)
            schemas.append(
                get_object_class_from_function_call(
                    "StructField",
                    column_spec.name,
                    ExpressionStr(f"{field_type}()"),
                )
            )
            required_imports.add(field_type)

        schema_statement = f"StructType({repr(schemas)})"
        import_classes = ", ".join(sorted(required_imports))
        pyspark_import_statement = f"from pyspark.sql.types import {import_classes}"
        return schema_statement, pyspark_import_statement

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
        (
            base_dataframe_schema,
            pyspark_import_statement,
        ) = self._get_base_dataframe_schema_and_import_statement()
        codes = code_gen.generate(
            to_format=True,
            remove_unused_variables=False,
            databricks_sdk_version=self.databricks_sdk_version,
            pyspark_import_statement=pyspark_import_statement,
            features=feature_specs,
            exclude_columns=self.exclude_columns,
            require_timestamp_lookup_key=self.require_timestamp_lookup_key,
            schema=base_dataframe_schema,
        )
        return codes

    @staticmethod
    def _get_fully_qualified_schema_name(feature_store: FeatureStoreModel) -> str:
        feature_store_details = feature_store.details
        assert isinstance(feature_store_details, (DatabricksDetails, DatabricksUnityDetails))
        catalog_name = feature_store_details.catalog_name
        schema_name = feature_store_details.schema_name
        return f"{catalog_name}.{schema_name}"

    @staticmethod
    def _create_feature_functions(
        features: List[FeatureModel], schema_name: str
    ) -> List[DataBricksFeatureFunction]:
        feature_functions = []
        for feature in features:
            offline_store_info = feature.offline_store_info
            if offline_store_info.udf_info:
                input_bindings = {
                    sql_input_info.sql_input_var_name: sql_input_info.column_name
                    for sql_input_info in offline_store_info.udf_info.sql_inputs_info
                }
                feature_functions.append(
                    DataBricksFeatureFunction(
                        udf_name=f"{schema_name}.{offline_store_info.udf_info.sql_function_name}",
                        input_bindings=input_bindings,
                        output_name=feature.name,
                    )
                )
        return feature_functions

    @staticmethod
    def _create_feature_lookups(
        features: List[FeatureModel], schema_name: str
    ) -> List[DataBricksFeatureLookup]:
        feature_lookups = []
        table_name_to_feature_lookup = {}
        for feature in features:
            offline_store_info = feature.offline_store_info
            entity_id_to_serving_name = {
                info.entity_id: info.serving_name for info in offline_store_info.serving_names_info
            }
            for ingest_query in offline_store_info.extract_offline_store_ingest_query_graphs():
                table_name = ingest_query.offline_store_table_name
                timestamp_lookup_key = VariableNameStr("timestamp_lookup_key")

                if table_name not in table_name_to_feature_lookup:
                    if len(ingest_query.primary_entity_ids) > 0:
                        lookup_key = [
                            entity_id_to_serving_name[entity_id]
                            for entity_id in ingest_query.primary_entity_ids
                        ]
                    else:
                        lookup_key = [DUMMY_ENTITY_COLUMN_NAME]
                    table_name_to_feature_lookup[table_name] = DataBricksFeatureLookup(
                        table_name=f"{schema_name}.{ingest_query.offline_store_table_name}",
                        lookup_key=lookup_key,
                        timestamp_lookup_key=timestamp_lookup_key,
                        lookback_window=None,
                        feature_names=[],
                        rename_outputs={},
                    )

                column_name = ingest_query.output_column_name
                feature_lookup = table_name_to_feature_lookup[table_name]
                feature_lookup.feature_names.append(column_name)
                if not offline_store_info.udf_info:
                    assert feature.name is not None, "Feature does not have a name"
                    feature_lookup.rename_outputs[column_name] = feature.name

        for feature_lookup in table_name_to_feature_lookup.values():
            feature_lookups.append(feature_lookup)
        return feature_lookups

    @classmethod
    def create(
        cls, features: List[FeatureModel], feature_store: FeatureStoreModel
    ) -> DataBricksUnityStoreInfo:
        exclude_columns = {SpecialColumnName.POINT_IN_TIME.value}
        entity_id_to_column_spec = {}
        request_column_name_to_dtype = {}
        for feature in features:
            offline_store_info = feature.offline_store_info
            entity_id_to_serving_name = {
                info.entity_id: info.serving_name for info in offline_store_info.serving_names_info
            }
            for ingest_query in offline_store_info.extract_offline_store_ingest_query_graphs():
                if offline_store_info.udf_info:
                    exclude_columns.add(ingest_query.output_column_name)

                for i, entity_id in enumerate(ingest_query.primary_entity_ids):
                    if entity_id not in entity_id_to_column_spec:
                        entity_id_to_column_spec[entity_id] = ColumnSpec(
                            name=entity_id_to_serving_name[entity_id],
                            dtype=ingest_query.primary_entity_dtypes[i],
                        )

            if offline_store_info.is_decomposed:
                for node in offline_store_info.graph.iterate_nodes(
                    target_node=offline_store_info.graph.get_node_by_name(
                        offline_store_info.node_name
                    ),
                    node_type=NodeType.REQUEST_COLUMN,
                ):
                    assert isinstance(node, RequestColumnNode)
                    node_params = node.parameters
                    if node_params.column_name not in request_column_name_to_dtype:
                        request_column_name_to_dtype[node_params.column_name] = node_params.dtype

        fully_qualified_schema_name = cls._get_fully_qualified_schema_name(feature_store)
        feature_specs: List[
            Union[DataBricksFeatureLookup, DataBricksFeatureFunction]
        ] = cls._create_feature_lookups(
            features, fully_qualified_schema_name
        ) + cls._create_feature_functions(
            features, fully_qualified_schema_name
        )

        base_dataframe_specs = [
            ColumnSpec(name="[TARGET_COLUMN]", dtype=DBVarType.FLOAT),
        ]
        has_point_in_time = False
        for entity_id in sorted(entity_id_to_column_spec.keys()):
            base_dataframe_specs.append(entity_id_to_column_spec[entity_id])
            exclude_columns.add(entity_id_to_column_spec[entity_id].name)
        for column_name in sorted(request_column_name_to_dtype.keys()):
            base_dataframe_specs.append(
                ColumnSpec(name=column_name, dtype=request_column_name_to_dtype[column_name])
            )
            if column_name == SpecialColumnName.POINT_IN_TIME:
                has_point_in_time = True

        if not has_point_in_time:
            base_dataframe_specs.append(
                ColumnSpec(name=SpecialColumnName.POINT_IN_TIME.value, dtype=DBVarType.TIMESTAMP)
            )

        return cls(
            feast_enabled=True,
            feature_specs=feature_specs,
            base_dataframe_specs=base_dataframe_specs,
            exclude_columns=sorted(exclude_columns),
            require_timestamp_lookup_key=True,
        )


StoreInfo = Annotated[
    Union[
        UninitializedStoreInfo,
        SnowflakeStoreInfo,
        DataBricksStoreInfo,
        DataBricksUnityStoreInfo,
        SparkStoreInfo,
    ],
    Field(discriminator="type"),
]
