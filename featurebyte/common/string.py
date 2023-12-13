"""
String utilities.
"""


def sanitize_identifier(string: str) -> str:
    """
    Sanitizes a string to make it a valid identifier for databases.

    Parameters
    ----------
    string: str
        The string to be sanitized.

    Returns
    --------
    str
        A sanitized version of the string suitable as a database identifier.
    """
    # Remove invalid characters
    sanitized = "".join(char for char in string if char.isalnum() or char == "_")

    # Ensure it starts with an alphabetic character
    if sanitized and (sanitized[0].isdigit() or sanitized[0] == "_"):
        sanitized = "a" + sanitized

    # Truncate to a reasonable length
    max_length = 64
    sanitized = sanitized[:max_length]

    # Convert to lower case for uniformity
    sanitized = sanitized.lower()

    return sanitized
