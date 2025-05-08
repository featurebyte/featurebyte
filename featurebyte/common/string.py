"""
String utilities.
"""

import re
import unicodedata
from typing import Optional


def sanitize_identifier(string: str, max_length: Optional[int] = 64) -> str:
    """
    Sanitizes a string to make it a valid identifier for database tables and columns.

    Parameters
    ----------
    string: str
        The string to be sanitized.
    max_length: Optional[int]
        The maximum length of the sanitized string. If None, no truncation is performed.

    Returns
    --------
    str
        A sanitized version of the string suitable as a database identifier.
    """
    # Normalize unicode characters if any
    string = unicodedata.normalize("NFKD", string)

    # Remove invalid characters
    sanitized = "".join(char for char in string if char.isalnum() or char == "_")

    # Ensure it starts with an alphabetic character or underscore
    if sanitized and not (sanitized[0].isalpha() or sanitized[0] == "_"):
        sanitized = "_" + sanitized

    # Truncate to a reasonable length
    if max_length:
        sanitized = sanitized[:max_length]

    # Convert to lower case for uniformity
    sanitized = sanitized.lower()

    return sanitized


def sanitize_search_term(search_term: Optional[str]) -> Optional[str]:
    """
    Sanitizes a search term for use in a mongo query.

    Parameters
    ----------
    search_term: Optional[str]
        Search term to be sanitized.

    Returns
    -------
    Optional[str]
        A sanitized version of the search term.
    """
    if not search_term:
        return search_term
    return re.escape(search_term)
