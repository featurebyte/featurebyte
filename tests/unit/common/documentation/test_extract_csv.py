"""
Test extract CSV
"""
from typing import List

from dataclasses import dataclass

from featurebyte.common.documentation.extract_csv import _generate_items_to_render
from featurebyte.common.documentation.gen_ref_pages_docs_builder import (
    generate_documentation_for_docs,
    get_doc_groups,
)


@dataclass
class FailureMode:
    """
    Dataclass to capture various failure modes for a given menu item.
    """

    missing_docstring: bool = False
    missing_examples: bool = False

    def is_failure(self):
        """
        Check if any of the failure modes are true.
        """
        return self.missing_docstring or self.missing_examples

    def __repr__(self):
        failures: List[str] = []
        if self.missing_docstring:
            failures.append("docstring")
        if self.missing_examples:
            failures.append("examples")
        return ",".join(failures)


@dataclass
class Failure:
    """
    Dataclass to capture a failure reason for a given menu item.
    """

    menu_item: str
    item_name: str
    failure_mode: FailureMode


# These methods currently don't have examples in the documentation. We should update the documentation to include
# some examples.
methods_without_examples = {
    "featurebyte.table.info",
    "featurebyte.api.base_table.TableApiObject.preview_sql",
    "featurebyte.tablecolumn.preview_sql",
    "featurebyte.entity.info",
    "featurebyte.feature.save",
    "featurebyte.feature.info",
    "featurebyte.feature.get_feature_jobs_status",
    "featurebyte.Feature.dt.tz_offset",
    "featurebyte.featurelist.save",
    "featurebyte.featurelist.get_feature_jobs_status",
    "featurebyte.view.get_join_column",
    "featurebyte.view.preview_sql",
    "featurebyte.viewcolumn.preview_sql",
    "featurebyte.ViewColumn.dt.tz_offset",
    "featurebyte.batchfeaturetable.describe",
    "featurebyte.batchfeaturetable.preview",
    "featurebyte.batchfeaturetable.sample",
    "featurebyte.batchfeaturetable.delete",
    "featurebyte.batchrequesttable.describe",
    "featurebyte.batchrequesttable.preview",
    "featurebyte.batchrequesttable.sample",
    "featurebyte.batchrequesttable.delete",
    "featurebyte.observationtable.describe",
    "featurebyte.observationtable.preview",
    "featurebyte.observationtable.sample",
    "featurebyte.observationtable.delete",
    "featurebyte.historicalfeaturetable.describe",
    "featurebyte.historicalfeaturetable.preview",
    "featurebyte.historicalfeaturetable.sample",
    "featurebyte.historicalfeaturetable.delete",
}


def test_attributes_populated():
    """
    Test that various attributes in the docs are populated.

    If an attribute is missing, you should consider updating the docs to include it.
    """
    doc_groups_to_use = get_doc_groups()
    _, doc_items = generate_documentation_for_docs(doc_groups_to_use)
    all_doc_items_to_generate = _generate_items_to_render(doc_items)
    failures: List[Failure] = []
    for item in all_doc_items_to_generate:
        failure_mode = FailureMode()
        # Perform checks
        if not item.docstring_description:
            failure_mode.missing_docstring = True
        if (
            item.item_type == "method"
            and not item.examples
            and item.class_method_or_attribute not in methods_without_examples
        ):
            # All methods should have an example
            failure_mode.missing_examples = True

        # Add to failures if necessary
        if failure_mode.is_failure():
            failures.append(
                Failure(
                    menu_item=item.menu_item,
                    item_name=item.class_method_or_attribute,
                    failure_mode=failure_mode,
                )
            )
    assert len(failures) == 0, f"Failures: {failures}"
