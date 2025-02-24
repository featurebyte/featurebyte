"""
This module contains unary operation node classes
"""

# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import ClassVar, List, Type, Union

from typing_extensions import Literal

from featurebyte.enum import DBVarType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model.dtype import DBVarTypeInfo, DBVarTypeMetadata
from featurebyte.query_graph.model.timestamp_schema import TimestampSchema
from featurebyte.query_graph.node.base import (
    BaseSeriesOutputWithSingleOperandNode,
)
from featurebyte.query_graph.node.metadata.operation import OperationStructure
from featurebyte.query_graph.node.metadata.sdk_code import ExpressionStr, VariableNameStr


class NotNode(BaseSeriesOutputWithSingleOperandNode):
    """NotNode class"""

    type: Literal[NodeType.NOT] = NodeType.NOT

    # class variable
    _derive_sdk_code_return_var_name_expression_type: ClassVar[
        Union[Type[VariableNameStr], Type[ExpressionStr]]
    ] = ExpressionStr

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=DBVarType.BOOL)

    def generate_expression(self, operand: str) -> str:
        return f"~{operand}"

    def generate_odfv_expression(self, operand: str) -> str:
        return f"{operand}.map(lambda x: not x if pd.notnull(x) else x)"

    def generate_udf_expression(self, operand: str) -> str:
        return f"not {operand}"


class AbsoluteNode(BaseSeriesOutputWithSingleOperandNode):
    """AbsoluteNode class"""

    type: Literal[NodeType.ABS] = NodeType.ABS

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return inputs[0].series_output_dtype_info

    def generate_expression(self, operand: str) -> str:
        return f"{operand}.abs()"

    def generate_udf_expression(self, operand: str) -> str:
        return f"np.abs({operand})"


class SquareRootNode(BaseSeriesOutputWithSingleOperandNode):
    """SquareRootNode class"""

    type: Literal[NodeType.SQRT] = NodeType.SQRT

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=DBVarType.FLOAT)

    def generate_expression(self, operand: str) -> str:
        return f"{operand}.sqrt()"

    def generate_odfv_expression(self, operand: str) -> str:
        return f"np.sqrt({operand})"

    def generate_udf_expression(self, operand: str) -> str:
        return f"np.sqrt({operand})"


class FloorNode(BaseSeriesOutputWithSingleOperandNode):
    """FloorNode class"""

    type: Literal[NodeType.FLOOR] = NodeType.FLOOR

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=DBVarType.INT)

    def generate_expression(self, operand: str) -> str:
        return f"{operand}.floor()"

    def generate_odfv_expression(self, operand: str) -> str:
        return f"np.floor({operand})"

    def generate_udf_expression(self, operand: str) -> str:
        return f"np.floor({operand})"


class CeilNode(BaseSeriesOutputWithSingleOperandNode):
    """CeilNode class"""

    type: Literal[NodeType.CEIL] = NodeType.CEIL

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=DBVarType.INT)

    def generate_expression(self, operand: str) -> str:
        return f"{operand}.ceil()"

    def generate_odfv_expression(self, operand: str) -> str:
        return f"np.ceil({operand})"

    def generate_udf_expression(self, operand: str) -> str:
        return f"np.ceil({operand})"


class CosNode(BaseSeriesOutputWithSingleOperandNode):
    """CosNode class"""

    type: Literal[NodeType.COS] = NodeType.COS

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=DBVarType.FLOAT)

    def generate_expression(self, operand: str) -> str:
        return f"{operand}.cos()"

    def generate_odfv_expression(self, operand: str) -> str:
        return f"np.cos({operand})"

    def generate_udf_expression(self, operand: str) -> str:
        return f"np.cos({operand})"


class SinNode(BaseSeriesOutputWithSingleOperandNode):
    """SinNode class"""

    type: Literal[NodeType.SIN] = NodeType.SIN

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=DBVarType.FLOAT)

    def generate_expression(self, operand: str) -> str:
        return f"{operand}.sin()"

    def generate_odfv_expression(self, operand: str) -> str:
        return f"np.sin({operand})"

    def generate_udf_expression(self, operand: str) -> str:
        return f"np.sin({operand})"


class TanNode(BaseSeriesOutputWithSingleOperandNode):
    """TanNode class"""

    type: Literal[NodeType.TAN] = NodeType.TAN

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=DBVarType.FLOAT)

    def generate_expression(self, operand: str) -> str:
        return f"{operand}.tan()"

    def generate_odfv_expression(self, operand: str) -> str:
        return f"np.tan({operand})"

    def generate_udf_expression(self, operand: str) -> str:
        return f"np.tan({operand})"


class AcosNode(BaseSeriesOutputWithSingleOperandNode):
    """AcosNode class"""

    type: Literal[NodeType.ACOS] = NodeType.ACOS

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=DBVarType.FLOAT)

    def generate_expression(self, operand: str) -> str:
        return f"{operand}.acos()"

    def generate_odfv_expression(self, operand: str) -> str:
        return f"np.arccos({operand})"

    def generate_udf_expression(self, operand: str) -> str:
        return f"np.arccos({operand})"


class AsinNode(BaseSeriesOutputWithSingleOperandNode):
    """AsinNode class"""

    type: Literal[NodeType.ASIN] = NodeType.ASIN

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=DBVarType.FLOAT)

    def generate_expression(self, operand: str) -> str:
        return f"{operand}.asin()"

    def generate_odfv_expression(self, operand: str) -> str:
        return f"np.arcsin({operand})"

    def generate_udf_expression(self, operand: str) -> str:
        return f"np.arcsin({operand})"


class AtanNode(BaseSeriesOutputWithSingleOperandNode):
    """CeilNode class"""

    type: Literal[NodeType.ATAN] = NodeType.ATAN

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=DBVarType.FLOAT)

    def generate_expression(self, operand: str) -> str:
        return f"{operand}.atan()"

    def generate_odfv_expression(self, operand: str) -> str:
        return f"np.arctan({operand})"

    def generate_udf_expression(self, operand: str) -> str:
        return f"np.arctan({operand})"


class LogNode(BaseSeriesOutputWithSingleOperandNode):
    """LogNode class"""

    type: Literal[NodeType.LOG] = NodeType.LOG

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=DBVarType.FLOAT)

    def generate_expression(self, operand: str) -> str:
        return f"{operand}.log()"

    def generate_odfv_expression(self, operand: str) -> str:
        return f"np.log({operand})"

    def generate_udf_expression(self, operand: str) -> str:
        return f"np.log({operand})"


class ExponentialNode(BaseSeriesOutputWithSingleOperandNode):
    """ExponentialNode class"""

    type: Literal[NodeType.EXP] = NodeType.EXP

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=DBVarType.FLOAT)

    def generate_expression(self, operand: str) -> str:
        return f"{operand}.exp()"

    def generate_odfv_expression(self, operand: str) -> str:
        return f"np.exp({operand})"

    def generate_udf_expression(self, operand: str) -> str:
        return f"np.exp({operand})"


class IsNullNode(BaseSeriesOutputWithSingleOperandNode):
    """IsNullNode class"""

    type: Literal[NodeType.IS_NULL] = NodeType.IS_NULL

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=DBVarType.BOOL)

    def generate_expression(self, operand: str) -> str:
        return f"{operand}.isnull()"

    def generate_udf_expression(self, operand: str) -> str:
        return f"pd.isna({operand})"

    def _generate_udf_expression_with_null_value_handling(self, operand: str) -> str:
        return self.generate_udf_expression(operand=operand)


class CastNode(BaseSeriesOutputWithSingleOperandNode):
    """CastNode class"""

    class Parameters(FeatureByteBaseModel):
        """Parameters"""

        type: Literal["int", "float", "str"]
        from_dtype: DBVarType

    type: Literal[NodeType.CAST] = NodeType.CAST
    parameters: Parameters

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        if self.parameters.type == "int":
            return DBVarTypeInfo(dtype=DBVarType.INT)
        if self.parameters.type == "float":
            return DBVarTypeInfo(dtype=DBVarType.FLOAT)
        if self.parameters.type == "str":
            return DBVarTypeInfo(dtype=DBVarType.VARCHAR)
        return DBVarTypeInfo(dtype=DBVarType.UNKNOWN)  # type: ignore[unreachable]

    def generate_expression(self, operand: str) -> str:
        return f"{operand}.astype({self.parameters.type})"

    def generate_odfv_expression(self, operand: str) -> str:
        expr = f"{operand}.map(lambda x: {self.parameters.type}(x) if pd.notnull(x) else x)"
        if self.parameters.type == "str":
            return f"{expr}.astype(object)"
        return expr

    def generate_udf_expression(self, operand: str) -> str:
        return f"{self.parameters.type}({operand})"


class IsStringNode(BaseSeriesOutputWithSingleOperandNode):
    """IsStringNode class"""

    type: Literal[NodeType.IS_STRING] = NodeType.IS_STRING

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=DBVarType.BOOL)

    def generate_expression(self, operand: str) -> str:
        raise RuntimeError("Not implemented")


class AddTimestampSchemaNodeParameters(FeatureByteBaseModel):
    """Parameters"""

    timestamp_schema: TimestampSchema


class AddTimestampSchemaNode(BaseSeriesOutputWithSingleOperandNode):
    """AddTimestampSchemaNode class"""

    type: Literal[NodeType.ADD_TIMESTAMP_SCHEMA] = NodeType.ADD_TIMESTAMP_SCHEMA
    parameters: AddTimestampSchemaNodeParameters

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        input_dtype_info = inputs[0].series_output_dtype_info
        return DBVarTypeInfo(
            dtype=input_dtype_info.dtype,
            metadata=DBVarTypeMetadata(timestamp_schema=self.parameters.timestamp_schema),
        )

    def generate_expression(self, operand: str) -> str:
        raise RuntimeError("Not implemented")
