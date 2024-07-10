"""
This module contains class used to transform or extract dictionary like object
"""

from __future__ import annotations

from typing import Any, Dict

from pydantic import Field
from typeguard import typechecked

from featurebyte.models.base import FeatureByteBaseModel


class DictProject(FeatureByteBaseModel):
    """
    Dictionary projection class based on given projection rule
    """

    rule: Any = Field(default=None)
    verbose_only: bool = Field(default=False)

    @classmethod
    def _project(cls, projection: Any, input_value: Any) -> Any:
        if input_value is None:
            return None

        if isinstance(input_value, list):
            # apply same rule to each of the item in the value list
            return [cls._project(projection, sub_input) for sub_input in input_value]

        if isinstance(projection, str):
            # select item with key value equals to projection
            return input_value.get(projection)

        if isinstance(projection, list):
            # select a subset of the input dictionary
            return {proj: cls._project(proj, input_value) for proj in projection}

        if isinstance(projection, tuple) and projection:
            # traverse the dictionary
            cur_proj = projection[0]
            remain_proj = projection[1:]
            return cls._project(remain_proj, cls._project(cur_proj, input_value))
        return input_value

    @typechecked
    def project(self, input_value: Dict[str, Any]) -> Any:
        """
        Extract value based on given projection rule applied on the given dictionary

        Parameters
        ----------
        input_value: Dict[str, Any]
            Input dictionary

        Returns
        -------
        Projected value
        """
        return self._project(self.rule, input_value)


class DictTransform(FeatureByteBaseModel):
    """
    Dictionary transform class
    """

    rule: Any = Field(default=None)

    @classmethod
    def _transform(cls, transform_rule: Any, input_value: Any, verbose: bool) -> Dict[str, Any]:
        output = {}
        for key, value in transform_rule.items():
            if isinstance(value, DictProject):
                sub_output = value.project(input_value)
                if not verbose and value.verbose_only:
                    # ignore the field when the projection is verbose only and non-verbose transform is requested
                    continue
            else:
                sub_output = cls._transform(value, input_value, verbose)

            if key == "__root__":
                assert isinstance(sub_output, dict)
                output.update(sub_output)
            else:
                output[key] = sub_output
        return output

    def transform(self, input_value: Dict[str, Any], verbose: bool = True) -> Dict[str, Any]:
        """
        Transform a given dictionary based on given transform rule

        Examples
        --------
        Transform rule is a dictionary which contains key & DictProject object.

            transform_rule = {"a": {"b": DictProject(("e",))}}, input_value = {"e": 100}
            output = {"a": {"b": 100}}

        When the transform rule dictionary key value is __root__, it will combine the projected dictionary
        with the current transform output.

            transform_rule = {"__root__": DictProject((["a", "b"]))}, input_value = {"a": 1, "b": 2, "c": 3}
            output = {"a": 1, "b": 2}

        Parameters
        ----------
        input_value: Dict[str, Any]
            Input dictionary
        verbose: bool
            Whether to generate more detail information

        Returns
        -------
        Transformed output
        """
        return self._transform(self.rule, input_value, verbose)
