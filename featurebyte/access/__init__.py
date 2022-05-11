"""
This module acts as a Facade to manage the access to both Local Database and Github.
Any access to Local Database or Github should go through this module
"""

from .database import LocalSourceDBManager

__all__ = ["LocalSourceDBManager"]
