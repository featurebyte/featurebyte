"""
Production ready validator
"""

from typing import Any, Dict, List

from featurebyte import ColumnCleaningOperation
from featurebyte.exception import DocumentUpdateError, NoChangesInFeatureVersionError
from featurebyte.models.feature import FeatureModel
from featurebyte.models.feature_namespace import FeatureReadiness
from featurebyte.models.feature_store import TableStatus
from featurebyte.query_graph.enum import GraphNodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.nested import BaseViewGraphNodeParameters
from featurebyte.service.feature import FeatureService
from featurebyte.service.table import TableService
from featurebyte.service.version import VersionService


class ProductionReadyValidator:
    """
    Validator for checking whether it is valid to promote a feature to the PRODUCTION_READY status.
    """

    def __init__(
        self,
        table_service: TableService,
        feature_service: FeatureService,
        version_service: VersionService,
    ):
        self.table_service = table_service
        self.feature_service = feature_service
        self.version_service = version_service

    async def validate(
        self,
        promoted_feature: FeatureModel,
        ignore_guardrails: bool = False,
    ) -> None:
        """
        Validate whether it is ok to promote the feature being passed in to PRODUCTION_READY status.

        Parameters
        ----------
        promoted_feature: FeatureModel
            Feature being promoted to PRODUCTION_READY
        ignore_guardrails: bool
            Parameter to determine whether to ignore guardrails
        """
        assert promoted_feature.name is not None
        await self._assert_no_other_production_ready_feature(promoted_feature=promoted_feature)
        await self._assert_no_deprecated_table(promoted_feature=promoted_feature)
        # We will skip these additional checks if the user explicit states that they want to ignore these
        # guardrails.
        if not ignore_guardrails:
            try:
                source_feature = (
                    await self.version_service.create_new_feature_version_using_source_settings(
                        promoted_feature.id
                    )
                )
            except NoChangesInFeatureVersionError:
                # We can return here since there are no changes in the feature version.
                return

            feature_job_setting_diff = (
                await self._get_feature_job_setting_diffs_table_source_vs_promoted_feature(
                    source_feature.node, source_feature.graph, promoted_feature.graph
                )
            )
            cleaning_ops_diff = (
                await self._get_cleaning_operations_diff_table_source_vs_promoted_feature(
                    source_feature.graph, promoted_feature.graph
                )
            )
            ProductionReadyValidator._raise_error_if_diffs_present(
                feature_job_setting_diff, cleaning_ops_diff
            )

    @staticmethod
    def _raise_error_if_diffs_present(
        feature_job_setting_diff: Dict[str, Any], cleaning_ops_diff: Dict[str, Any]
    ) -> None:
        """
        Raise an error if diffs are present.

        Parameters
        ----------
        feature_job_setting_diff: Dict[str, Any]
            feature job setting diffs
        cleaning_ops_diff: Dict[str, Any]
            cleaning operations differences

        Raises
        ------
        DocumentUpdateError
            raised if there are any differences between the promoted feature version and table defaults
        """
        if not feature_job_setting_diff and not cleaning_ops_diff:
            return
        diff_format_dict = {}
        if feature_job_setting_diff:
            diff_format_dict["feature_job_setting"] = feature_job_setting_diff
        if cleaning_ops_diff:
            diff_format_dict["cleaning_operations"] = cleaning_ops_diff
        raise DocumentUpdateError(
            "Discrepancies found between the promoted feature version you are trying to promote to "
            "PRODUCTION_READY, and the input table.\n"
            f"{diff_format_dict}\n"
            "Please fix these issues first before trying to promote your feature to PRODUCTION_READY."
        )

    async def _assert_no_other_production_ready_feature(
        self, promoted_feature: FeatureModel
    ) -> None:
        """
        Check to see if there are any other production ready features.

        Parameters
        ----------
        promoted_feature: FeatureModel
            Feature being promoted to PRODUCTION_READY

        Raises
        ------
        DocumentUpdateError
            raised when there is another feature version with the same name that is production ready
        """
        query_filter = {
            "name": promoted_feature.name,
            "readiness": FeatureReadiness.PRODUCTION_READY.value,
        }
        async for feature in self.feature_service.list_documents_as_dict_iterator(
            query_filter=query_filter, projection={"_id": 1}
        ):
            feature_id = feature["_id"]
            if feature_id != promoted_feature.id:
                raise DocumentUpdateError(
                    f"Found another feature version that is already PRODUCTION_READY. Please deprecate the feature "
                    f'"{promoted_feature.name}" with ID {feature_id} first before promoting the promoted '
                    "version as there can only be one feature version that is production ready at any point in time. "
                    f"We are unable to promote the feature with ID {promoted_feature.id} right now."
                )

    async def _assert_no_deprecated_table(self, promoted_feature: FeatureModel) -> None:
        """
        Check to see if there are any deprecated tables.

        Parameters
        ----------
        promoted_feature: FeatureModel
            Feature being promoted to PRODUCTION_READY

        Raises
        ------
        DocumentUpdateError
            raise when deprecated tables are found
        """
        query_filter = {
            "_id": {"$in": promoted_feature.table_ids},
            "status": TableStatus.DEPRECATED.value,
        }
        async for table in self.table_service.list_documents_iterator(query_filter=query_filter):
            raise DocumentUpdateError(
                f'Found a deprecated table "{table.name}" that is used by the feature "{promoted_feature.name}". '
                "We are unable to promote the feature to PRODUCTION_READY right now."
            )

    @staticmethod
    async def _get_feature_job_setting_diffs_table_source_vs_promoted_feature(
        table_source_node: Node,
        table_source_graph: QueryGraph,
        promoted_feature_graph: QueryGraphModel,
    ) -> Dict[str, Any]:
        """
        Get feature job setting diffs between the feature version created from source table, and the promoted feature
        version that the user is trying to promote to PRODUCTION_READY.

        Parameters
        ----------
        table_source_node: Node
            source node
        table_source_graph: QueryGraph
            source graph
        promoted_feature_graph: QueryGraphModel
            promoted graph

        Returns
        -------
        Dict[str, Any]
            feature job setting diffs
        """
        for current_node, _ in table_source_graph.iterate_group_by_node_and_table_id_pairs(
            target_node=table_source_node
        ):
            # Get corresponding group by node in promoted graph
            promoted_agg_node = promoted_feature_graph.get_node_by_name(current_node.name)
            source_feature_job_setting = current_node.parameters.feature_job_setting
            promoted_feature_job_setting = promoted_agg_node.parameters.feature_job_setting  # type: ignore
            if source_feature_job_setting != promoted_feature_job_setting:
                return {
                    "data_source": source_feature_job_setting,
                    "promoted_feature": promoted_feature_job_setting,
                }
        return {}

    @staticmethod
    def _get_cleaning_operations_from_view_graph_node(
        view_graph_node: Node,
    ) -> List[ColumnCleaningOperation]:
        """
        Get cleaning operations from view graph node.

        Parameters
        ----------
        view_graph_node: Node
            view graph node

        Returns
        -------
        List[ColumnCleaningOperation]
            list of cleaning operations
        """
        parameters = view_graph_node.parameters
        assert isinstance(parameters, BaseViewGraphNodeParameters)
        view_metadata = parameters.metadata
        return view_metadata.column_cleaning_operations

    async def _get_cleaning_operations_diff_table_source_vs_promoted_feature(
        self, table_source_feature_graph: QueryGraph, promoted_feature_graph: QueryGraphModel
    ) -> Dict[str, Any]:
        """
        Get cleaning operation diffs between the feature version created from source table, and the promoted feature
        version that the user is trying to promote to PRODUCTION_READY.

        Parameters
        ----------
        table_source_feature_graph: QueryGraph
            source graph
        promoted_feature_graph: QueryGraphModel
            promoted graph

        Returns
        -------
        Dict[str, Any]
            returns a dictionary with the difference in values in the cleaning operations
        """
        for view_graph_node in table_source_feature_graph.iterate_sorted_graph_nodes(
            graph_node_types=GraphNodeType.view_graph_node_types()
        ):
            # get node from promoted graph
            promoted_view_graph_node = promoted_feature_graph.get_node_by_name(view_graph_node.name)

            # get cleaning operations from source and promoted graph
            source_cleaning_operations = self._get_cleaning_operations_from_view_graph_node(
                view_graph_node
            )
            promoted_cleaning_operations = self._get_cleaning_operations_from_view_graph_node(
                promoted_view_graph_node
            )

            # compare cleaning operations
            if source_cleaning_operations != promoted_cleaning_operations:
                return {
                    "data_source": source_cleaning_operations,
                    "promoted_feature": promoted_cleaning_operations,
                }
        return {}
