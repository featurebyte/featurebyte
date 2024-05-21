"""
FeatureByte Autodoc Extension for mkdocs
"""

from __future__ import annotations

from markdown import Markdown
from markdown.extensions import Extension

from featurebyte.common.documentation.autodoc_processor import FBAutoDocProcessor


class FBAutoDocExtension(Extension):
    def extendMarkdown(self, md: Markdown) -> None:
        """
        Add the extension to the Markdown instance.

        Parameters
        ----------
        md: Markdown
            The Markdown instance.
        """
        processor = FBAutoDocProcessor(md.parser, md=md)
        md.registerExtension(self)
        md.parser.blockprocessors.register(processor, "fbautodoc", 110)


def makeExtension() -> FBAutoDocExtension:
    """
    Return extension.

    This is the entry point for the extension.

    Returns
    -------
    FBAutoDocExtension
    """
    return FBAutoDocExtension()
