"""
FeatureByte Autodoc Extension for mkdocs
"""
from __future__ import annotations

from markdown import Markdown
from markdown.extensions import Extension

from featurebyte.common.documentation.autodoc_processor import FBAutoDocProcessor


class FBAutoDocExtension(Extension):
    def extendMarkdown(self, md: Markdown) -> None:
        processor = FBAutoDocProcessor(md.parser, md=md)
        md.registerExtension(self)
        md.parser.blockprocessors.register(processor, "fbautodoc", 110)


def makeExtension():
    return FBAutoDocExtension()
