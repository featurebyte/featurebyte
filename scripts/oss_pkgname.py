"""Shared package-name normalization for the OSS due-diligence scripts."""

import re


def normalize(package_name: str) -> str:
    """Normalize a package name per PEP 503, so e.g. 'PyPhen' and 'py-phen' match."""
    return re.sub(r"[-_.]+", "-", package_name).lower()
