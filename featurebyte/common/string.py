"""
String utilities.
"""
import keyword


def sanitize_identifier(string: str) -> str:
    """
    Sanitizes a string to make it a valid Python identifier.

    Parameters
    ----------
    string: str
        String to be sanitized.

    Returns
    -------
    str
        Sanitized string.
    """

    # Remove invalid characters
    sanitized = "".join(char for char in string if char.isalnum() or char == "_")

    # Ensure it doesn't start with a digit
    if sanitized and sanitized[0].isdigit():
        sanitized = "_" + sanitized

    # Check if the string is a reserved keyword
    if keyword.iskeyword(sanitized):
        sanitized = "_" + sanitized

    return sanitized
