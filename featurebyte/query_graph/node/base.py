"""
Base classes required for constructing query graph nodes
"""
from typing import TYPE_CHECKING, Any, List, Type, Union

from pydantic import BaseModel
from pydantic.fields import ModelField

from featurebyte.query_graph.enum import NodeOutputType, NodeType

if TYPE_CHECKING:
    from pydantic.typing import CallableGenerator


class ColumnStr(str):
    """
    ColumnStr validator class
    """

    @classmethod
    def __get_validators__(cls) -> "CallableGenerator":
        yield cls.validate

    @classmethod
    def validate(cls, value: Any) -> "ColumnStr":
        """
        Validate value

        Parameters
        ----------
        value: Any
            Input to the ColumnStr class

        Returns
        -------
        ColumnStr
        """
        return cls(str(value))


class OutColumnStr(ColumnStr):
    """
    OutColumnStr is used to type newly generated column string
    """


class InColumnStr(ColumnStr):
    """
    InColumnStr is used to type required input column string
    """


class BaseNode(BaseModel):
    """
    BaseNode class
    """

    name: str
    type: NodeType
    output_type: NodeOutputType
    parameters: BaseModel

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

        # make sure subclass set certain properties correctly
        assert self.__fields__["type"].field_info.const is True
        assert repr(self.__fields__["type"].type_).startswith("typing.Literal")
        assert self.__fields__["output_type"].type_ is NodeOutputType

    @classmethod
    def _extract_column_str_values(
        cls,
        values: Any,
        column_str_type: Union[Type[InColumnStr], Type[OutColumnStr]],
    ) -> List[str]:
        out = set()
        if isinstance(values, dict):
            for val in values.values():
                if isinstance(val, column_str_type):
                    out.add(str(val))
                if isinstance(val, (dict, list)):
                    out.update(cls._extract_column_str_values(val, column_str_type))
        if isinstance(values, list):
            for val in values:
                if isinstance(val, column_str_type):
                    out.add(str(val))
                if isinstance(val, (dict, list)):
                    out.update(cls._extract_column_str_values(val, column_str_type))
        return list(out)

    def get_required_input_columns(self) -> List[str]:
        """
        Get the required input column names based on this node parameters

        Returns
        -------
        list[str]
        """
        return self._extract_column_str_values(self.parameters.dict(), InColumnStr)

    def get_new_output_columns(self) -> List[str]:
        """
        Get additional column names generated based on this node parameters

        Returns
        -------
        list[str]
        """
        return self._extract_column_str_values(self.parameters.dict(), OutColumnStr)
