"""
Utility functions for formatting data in Jupyter notebooks
"""

from __future__ import annotations

import copy
import re
from datetime import datetime
from typing import Any, Dict, Union
from xml.dom import getDOMImplementation
from xml.dom.minidom import Document, Element

import pandas as pd
import pygments
from bson import ObjectId
from pygments.formatters.html import HtmlFormatter
from rich.pretty import pretty_repr


class CodeStr(str):
    """
    Code string content that can be displayed in markdown format
    """

    def _repr_html_(self) -> str:
        lexer = pygments.lexers.get_lexer_by_name("python")
        highlighted_code = pygments.highlight(
            str(self).strip(),
            lexer=lexer,
            formatter=HtmlFormatter(noclasses=True, nobackground=True),
        )
        return (
            '<div style="margin:30px; padding: 20px; border:1px solid #aaa">'
            f"{highlighted_code}</div>"
        )


class InfoDict(Dict[str, Any]):
    """
    Featurebyte asset information dictionary that can be displayed in HTML
    """

    def __init__(self, data: Union[Dict[str, Any], InfoDict]) -> None:
        self.class_name: str = "Unknown"
        if isinstance(data, InfoDict):
            self.class_name = data.class_name
        else:
            self.class_name = data.pop("class_name", "Unknown")

        super().__init__(data)

    def __repr__(self) -> str:
        return pretty_repr(dict(self), expand_all=True, indent_size=2)

    def _repr_html_(self) -> str:
        """
        HTML representation of the info dictionary

        Returns
        -------
        str
        """
        return self.to_html()

    def to_html(self) -> str:
        """
        Get HTML representation of the info dictionary

        Returns
        -------
        str
        """

        def _set_element_style(elem: Element, style: Dict[str, Any]) -> None:
            """
            Set style of dom element

            Parameters
            ----------
            elem: Element
                Element to set style for.
            style: Dict[str, Any]
                Style dictionary
            """
            elem.setAttribute("style", ";".join(f"{key}:{value}" for key, value in style.items()))

        def _populate_html_elem(
            data: Dict[str, Any], doc: Document, elem: Element, html_content: Dict[str, str]
        ) -> None:
            """
            Populate html document with data dict

            Parameters
            ----------
            data: Dict[str, str]
                Data dictionary
            doc: Document
                HTML document
            elem: Element
                HTML element to populate into
            html_content: Dict[str, str]
                Dictionary referencing HTML content in the document
            """
            # create html table
            table = doc.createElement("table")
            _set_element_style(
                table, {"table-layout": "auto", "width": "100%", "padding": 0, "margin": 0}
            )

            # determine max key length for computing column width
            max_key_len = max([len(key) for key in data.keys()] + [0])

            for key, value in data.items():
                # add row to table
                row = doc.createElement("tr")
                table.appendChild(row)

                # populate key column
                key_column = doc.createElement("td")
                _set_element_style(
                    key_column,
                    {
                        "font-weight": "bold",
                        "vertical-align": "top",
                        "width": f"{max_key_len * 8 + 5}px",
                        "over-flow": "overflow-wrap",
                        "word-break": "break-all",
                    },
                )
                key_column.appendChild(doc.createTextNode(key))
                row.appendChild(key_column)

                # populate value column
                if isinstance(value, dict):
                    # process dictionaries recursively
                    value_elem = doc.createElement("div")
                    _set_element_style(value_elem, {"border": "0", "padding": "0", "margin": "0"})
                    _populate_html_elem(
                        data=value, doc=doc, elem=value_elem, html_content=html_content
                    )
                elif isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
                    # list of dictionaries
                    value_elem = doc.createElement("div")
                    _set_element_style(value_elem, {"border": "0", "padding": "0", "margin": "0"})
                    df_key = str(ObjectId())
                    html_content[df_key] = pd.DataFrame(value).to_html()
                    value_elem.appendChild(doc.createTextNode(f"{{{df_key}}}"))
                else:
                    # nicer datetime formatting
                    try:
                        value = datetime.fromisoformat(value).strftime("%Y-%m-%d %H:%M:%S")
                    except (ValueError, TypeError):
                        pass
                    value_elem = doc.createTextNode(str(value))  # type: ignore

                val_column = doc.createElement("td")
                _set_element_style(val_column, {"width": "auto", "text-align": "left"})
                val_column.appendChild(value_elem)
                row.appendChild(val_column)

            elem.appendChild(table)

        # create html document
        impl = getDOMImplementation()
        assert impl
        doc_type = impl.createDocumentType(
            "html",
            "-//W3C//DTD XHTML 1.0 Strict//EN",
            "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd",
        )
        doc = impl.createDocument("http://www.w3.org/1999/xhtml", "html", doc_type)
        doc_elem = doc.documentElement

        # add title for the table
        data = copy.deepcopy(self)
        class_name = re.sub(r"([A-Z]+[a-z]+)", r" \1", self.class_name).strip()
        title_div = doc.createElement("div")
        title_div.appendChild(doc.createTextNode(class_name))
        _set_element_style(
            elem=title_div,
            style={
                "font-weight": "bold",
                "text-align": "center",
                "border-bottom": "1px solid black",
                "width": "100%",
                "padding-top": "20px",
            },
        )
        doc_elem.appendChild(title_div)

        # add info table
        html_content: Dict[str, str] = {}
        _populate_html_elem(data=data, doc=doc, elem=doc_elem, html_content=html_content)
        html = str(doc.toprettyxml())
        return html.format(**html_content)
