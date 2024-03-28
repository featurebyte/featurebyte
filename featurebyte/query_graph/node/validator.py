"""This module constains validator used for model input validation by classes in query_graph directory."""

from typing import Any, List, Optional


def construct_unique_name_validator(field: str) -> Any:
    """
    Construct a unique name validator function which will check the uniqueness of the input list

    Parameters
    ----------
    field: str
        Field name used to check uniqueness

    Returns
    -------
    Any
    """

    def _extract_key(elem: Any) -> Any:
        assert isinstance(field, str)
        return getattr(elem, field)

    def _unique_name_validator(cls: Any, value: Optional[List[Any]]) -> Optional[List[Any]]:
        _ = cls
        if not isinstance(value, list):
            return value

        # check uniqueness
        names = set()
        for elem in value:
            name = _extract_key(elem)
            if name in names:
                raise ValueError(f'Name "{name}" is duplicated (field: {field}).')
            names.add(name)
        return value

    return _unique_name_validator
