"""
Time formatter for converting different time formats to a standard python format.
"""

import re
from typing import Any

from featurebyte.enum import SourceType


def _generic_format_converter(
    format_str: str, token_map: list[tuple[str, str]], to_strip_quote: bool
) -> str:
    """
    A generic format converter that applies a two-phase replacement (tokens -> placeholders,
    then placeholders -> final codes). Also conditionally handles quoted literals by removing quotes
    and un-escaping doubled single quotes (i.e. '' -> ').

    Parameters
    ----------
    format_str : str
        The original format string (e.g. Java, Snowflake or Bigquery).
    token_map : list of (token, replacement)
        A list of tokens and their corresponding Python strftime/strptime replacements.
        Example: [("yyyy", "%Y"), ("MM", "%m"), ...]
    to_strip_quote: bool
        Whether to remove surrounding single quotes from quoted literals.

    Returns
    -------
    str
        The resulting Python strftime/strptime-compatible format string.
    """

    # 1) Sort by descending length to handle multi-character tokens first (e.g. "yyyy" before "yy").
    token_map.sort(key=lambda x: len(x[0]), reverse=True)

    # 2) Build two maps: token -> placeholder, placeholder -> replacement.
    token_to_placeholder = {}
    placeholder_to_final = {}

    for i, (original_token, final_token) in enumerate(token_map):
        placeholder = f"__TOK{i}__"
        token_to_placeholder[original_token] = placeholder
        placeholder_to_final[placeholder] = final_token

    # 3) Replace each token in the original format with its placeholder.
    placeholder_string = format_str
    for original_token, placeholder in token_to_placeholder.items():
        placeholder_string = placeholder_string.replace(original_token, placeholder)

    # 4) Replace each placeholder with its final token.
    python_format = placeholder_string
    for placeholder, final_token in placeholder_to_final.items():
        python_format = python_format.replace(placeholder, final_token)

    # 5) Remove surrounding single quotes from quoted literals and unescape doubled single quotes.
    #    e.g.: 'some literal' -> some literal, '' -> '
    if to_strip_quote:
        pattern = r"'((?:[^']|'')+)'"

        def _unescape_quotes(match: Any) -> Any:
            inner = match.group(1)
            # Unescape doubled single quotes
            return inner.replace("''", "'")

        python_format = re.sub(pattern, _unescape_quotes, python_format)

    return python_format


def convert_java_time_format_to_python(java_format: str) -> str:
    """
    Converts a Java date/time format string to a Python strptime/strftime format

    Parameters
    ----------
    java_format : str
        The Java date/time format string to convert.

    Returns
    -------
    str
        The converted Python date/time format string.
    """

    # Define Java -> Python tokens.
    # Adjust or extend if you have other tokens to support.
    java_tokens = [
        # (java_token, python_token)
        ("yyyy", "%Y"),
        ("yyy", "%Y"),  # Some Java usage
        ("yy", "%y"),
        ("MMMM", "%B"),
        ("MMM", "%b"),
        ("MM", "%m"),
        ("M", "%m"),
        ("dd", "%d"),
        ("d", "%d"),
        ("HH", "%H"),
        ("H", "%H"),
        ("hh", "%I"),
        ("h", "%I"),
        ("mm", "%M"),
        ("m", "%M"),
        ("ss", "%S"),
        ("s", "%S"),
        # Milliseconds => microseconds
        ("SSS", "%f"),
        ("EEEE", "%A"),
        ("EEE", "%a"),
        ("a", "%p"),
        ("Z", "%z"),
    ]
    return _generic_format_converter(java_format, java_tokens, to_strip_quote=True)


def convert_snowflake_time_format_to_python(snowflake_format: str) -> str:
    """
    Converts a Snowflake date/time format string to a Python strptime/strftime format

    Parameters
    ----------
    snowflake_format: str
        The Snowflake date/time format string to convert.

    Returns
    -------
    str
        The converted Python date/time format string.
    """
    # Define Snowflake -> Python tokens.
    # Adjust or extend if you have other tokens to support.
    # Note: For some cases like Day of week, Snowflake uses "DY" for full name & abbreviated name.
    # When mapping to Python, we only map to abbreviated name.
    snowflake_tokens = [
        # Time‑zone offsets (must come before shorter tokens)
        ("TZH:TZM", "%z"),  # e.g. +05:30 or -07:00
        ("TZHTZM", "%z"),  # e.g. +0530 or -0700
        ("TZH", "%z"),  # e.g. +05 or -07 (minutes assumed 00)
        # Time‑zone region name
        ("TZR", "%Z"),  # e.g. America/Chicago
        # Year
        ("YYYY", "%Y"),
        ("YYY", "%Y"),  # fallback
        ("YY", "%y"),
        ("Y", "%y"),  # fallback
        # Month
        ("MMMM", "%B"),
        ("MON", "%b"),
        ("MM", "%m"),
        # Day of month
        ("DD", "%d"),
        # Day of week
        ("DY", "%a"),
        # Hour
        ("HH24", "%H"),
        ("HH12", "%I"),
        ("HH", "%I"),  # if ambiguous usage, interpret as 12-hour
        # Minute
        ("MI", "%M"),
        # Second
        ("SS", "%S"),
        # Fractional seconds: FF.. up to 9 => %f
        ("FF9", "%f"),
        ("FF8", "%f"),
        ("FF7", "%f"),
        ("FF6", "%f"),
        ("FF5", "%f"),
        ("FF4", "%f"),
        ("FF3", "%f"),
        ("FF2", "%f"),
        ("FF1", "%f"),
        ("FF", "%f"),
        # AM/PM
        ("AM", "%p"),
        ("PM", "%p"),
    ]
    return _generic_format_converter(snowflake_format, snowflake_tokens, to_strip_quote=False)


def convert_bigquery_time_format_to_python(bigquery_format: str) -> str:
    """
    Converts a BigQuery date/time format string to a Python strptime/strftime format

    Parameters
    ----------
    bigquery_format: str
        The BigQuery date/time format string to convert.

    Returns
    -------
    str
        The converted Python date/time format string.
    """
    # Define BigQuery -> Python tokens.
    # Note: BigQuery tokens already include a "%" prefix. Some tokens like "%E*S" capture seconds with
    # fractional parts and are mapped to Python's "%S.%f". Adjust as needed for your specific use case.
    bigquery_tokens = [
        ("%E*S", "%S.%f"),  # seconds with fractional seconds
        ("%Y", "%Y"),
        ("%y", "%y"),
        ("%m", "%m"),
        ("%d", "%d"),
        ("%e", "%d"),
        ("%k", "%H"),
        ("%H", "%H"),
        ("%I", "%I"),
        ("%M", "%M"),
        ("%S", "%S"),
        ("%p", "%p"),
        ("%z", "%z"),
        ("%Z", "%Z"),
        ("%a", "%a"),
        ("%A", "%A"),
        ("%b", "%b"),
        ("%B", "%B"),
    ]
    return _generic_format_converter(bigquery_format, bigquery_tokens, to_strip_quote=False)


def convert_time_format(source_type: SourceType, format_string: str) -> str:
    """
    Converts a time format string from a specific source type to a standard Python format.

    Parameters
    ----------
    source_type : SourceType
        The source type (e.g., Java, Snowflake, BigQuery).
    format_string : str
        The original format string.

    Returns
    -------
    str
        The converted Python-compatible format string.

    Raises
    -------
    NotImplementedError
        If the source type is not supported.
    """
    if source_type in SourceType.java_time_format_types():
        return convert_java_time_format_to_python(format_string)
    elif source_type == SourceType.SNOWFLAKE:
        return convert_snowflake_time_format_to_python(format_string)
    elif source_type == SourceType.BIGQUERY:
        return convert_bigquery_time_format_to_python(format_string)
    else:
        raise NotImplementedError(f"Unsupported source type: {source_type}")
