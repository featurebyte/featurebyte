"""
Utility functions for docstring templating
"""
from typing import Any, Dict, Optional

import textwrap
from functools import wraps

from featurebyte.common.typing import Func


def substitute_docstring(
    doc_template: str,
    parameters: Optional[str] = None,
    returns: Optional[str] = None,
    raises: Optional[str] = None,
    examples: Optional[str] = None,
    format_kwargs: Optional[Dict[str, str]] = None,
) -> Func:
    """
    Decorator to substitute the docstring of a function

    Parameters
    ----------
    doc_template: str
        Template of the docstring
    parameters: Optional[str]
        Parameters section of the docstring
    returns: Optional[str]
        Returns section of the docstring
    raises: Optional[str]
        Raises section of the docstring
    examples: Optional[str]
        Examples section of the docstring
    format_kwargs: Optional[Dict[str, str]]
        Additional keyword arguments to be passed to the docstring formatter

    Returns
    -------
    Func
    """

    def _section_formatter(section: Optional[str]) -> str:
        if section is None:
            return ""
        return textwrap.dedent(section).strip()

    # prepare new docstring
    new_docstring = doc_template.format(
        parameters=_section_formatter(parameters),
        returns=_section_formatter(returns),
        raises=_section_formatter(raises),
        examples=_section_formatter(examples),
        **(format_kwargs or {}),
    )

    def decorator(func: Func) -> Func:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)

        wrapper.__doc__ = new_docstring
        return wrapper

    return decorator
