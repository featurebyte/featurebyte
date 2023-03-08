"""
Production ready validator
"""
from typing import Any, Dict, List, Optional, Tuple, cast

from featurebyte import ColumnCleaningOperation, FeatureJobSetting
from featurebyte.exception import DocumentError
from featurebyte.models.feature import FeatureReadiness
from featurebyte.query_graph.enum import GraphNodeType, NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import GroupByNode
from featurebyte.query_graph.node.nested import BaseViewGraphNodeParameters
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.tabular_data import DataService
from featurebyte.service.version import VersionService


class ProductionReadyValidator:
    """
    Validator for checking whether it is valid to upgrade a feature to the PRODUCTION_READY status.
    """

    def __init__(
        self,
        feature_namespace_service: FeatureNamespaceService,
        data_service: DataService,
        version_service: VersionService,
        feature_service: FeatureService,
    ):
        self.feature_namespace_service = feature_namespace_service
        self.data_service = data_service
        self.version_service = version_service
        self.feature_service = feature_service

    async def validate(
        self, feature_name: str, graph: QueryGraph, ignore_guardrails: bool = False
    ) -> None:
        """
        Validate.

        Parameters
        ----------
        feature_name: str
            feature name
        graph: QueryGraph
            graph
        ignore_guardrails: bool
            parameter to determine whether to ignore guardrails
        """
        await self._assert_no_other_production_ready_feature(feature_name)
        # We will skip these additional checks if the user explicit states that they want to ignore these
        # guardrails.
        if not ignore_guardrails:
            feature_version = await self._get_feature_version_of_source(feature_name)
            if feature_version is None:
                return
            feature_version_source_node, feature_version_source_graph = feature_version
            feature_job_setting_diff = await self._get_feature_job_setting_diffs_source_vs_curr(
                feature_version_source_node, feature_version_source_graph, graph
            )
            cleaning_ops_diff = await self._get_cleaning_operations_diff_vs_source(
                feature_version_source_graph, graph
            )
            ProductionReadyValidator._raise_error_if_diffs_present(
                feature_job_setting_diff, cleaning_ops_diff
            )

    async def _get_feature_version_of_source(
        self, feature_name: str
    ) -> Optional[Tuple[Node, QueryGraph]]:
        """
        Get the diffs.

        Parameters
        ----------
        feature_name: str
            feature name

        Returns
        -------
        Optional[Tuple[Node, QueryGraph]]
            node and graph of the feature version of the source, or None if there are no changes detected

        Raises
        ------
        DocumentError
            raised if there is an error when trying to create new feature version
        """
        features = await self.feature_service.list_documents(
            query_filter={"name": feature_name},
        )
        feature = features["data"][0]
        try:
            new_feature = (
                await self.version_service.create_new_feature_version_using_source_settings(
                    feature["_id"]
                )
            )
            if new_feature is None:
                return None
            return new_feature.node, new_feature.graph
        except DocumentError as exc:
            if "No change detected on the new feature version" in str(exc):
                return None
            raise exc

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
        ValueError
            raised if there are any differences
        """
        if not feature_job_setting_diff and not cleaning_ops_diff:
            return
        diff_format_dict = {}
        if feature_job_setting_diff:
            diff_format_dict["feature_job_setting"] = feature_job_setting_diff
        if cleaning_ops_diff:
            diff_format_dict["cleaning_operations"] = cleaning_ops_diff
        raise ValueError(
            "Discrepancies found between the current feature version you are trying to promote to "
            "PRODUCTION_READY, and the input data.\n"
            f"{diff_format_dict}\n"
            "Please fix these issues first before trying to promote your feature to PRODUCTION_READY."
        )

    async def _assert_no_other_production_ready_feature(self, feature_name: str) -> None:
        """
        Check to see if there are any other production ready features.

        Parameters
        ----------
        feature_name: str
            the name of the feature we are checking

        Raises
        ------
        ValueError
            raised when there is another feature version with the same name that is production ready
        """
        results = await self.feature_namespace_service.list_documents(
            query_filter={"name": feature_name}
        )
        for feature in results["data"]:
            if feature["readiness"] == FeatureReadiness.PRODUCTION_READY:
                feature_id = feature["_id"]
                raise ValueError(
                    "Found another feature version that is already PRODUCTION_READY. Please "
                    f"deprecate the feature {feature_name} with ID {feature_id} first before upgrading the current "
                    "version as there can only be one feature version that is production ready at any point in time."
                )

    @staticmethod
    def _get_feature_job_setting_from_groupby_node(node: Node) -> FeatureJobSetting:
        """
        Get feature job setting from node.

        Parameters
        ----------
        node: Node
            node

        Returns
        -------
        FeatureJobSetting
            feature job setting
        """
        groupby_node = cast(GroupByNode, node)
        parameters = groupby_node.parameters
        blind_spot_str = f"{parameters.blind_spot}s"
        frequency_str = f"{parameters.frequency}s"
        time_modulo_frequency_str = f"{parameters.time_modulo_frequency}s"
        return FeatureJobSetting(
            blind_spot=blind_spot_str,
            frequency=frequency_str,
            time_modulo_frequency=time_modulo_frequency_str,
        )

    @staticmethod
    async def _get_feature_job_setting_diffs_source_vs_curr(
        source_node: Node, source_graph: QueryGraph, new_graph: QueryGraph
    ) -> Dict[str, Any]:
        """
        Get feature job setting diffs between source and current.

        Parameters
        ----------
        source_node: Node
            source node
        source_graph: QueryGraph
            source graph
        new_graph: QueryGraph
            new graph

        Returns
        -------
        Dict[str, Any]
            feature job setting diffs
        """
        for current_node in source_graph.iterate_nodes(
            target_node=source_node, node_type=NodeType.GROUPBY
        ):
            # Get corresponding group by node in new graph
            new_group_by_node = new_graph.get_node_by_name(current_node.name)
            source_feature_job_setting = (
                ProductionReadyValidator._get_feature_job_setting_from_groupby_node(current_node)
            )
            new_feature_job_setting = (
                ProductionReadyValidator._get_feature_job_setting_from_groupby_node(
                    new_group_by_node
                )
            )
            if source_feature_job_setting != new_feature_job_setting:
                # TODO: how to handle multiple group by nodes failing w/ different FJS?
                # TODO: make sure we handle the error in no FJS
                return {
                    "default": source_feature_job_setting,
                    "feature": new_feature_job_setting,
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

    async def _get_cleaning_operations_diff_vs_source(
        self, source_graph: QueryGraph, new_graph: QueryGraph
    ) -> Dict[str, Any]:
        """
        Get differences between cleaning operations in the graph and the data source.

        Parameters
        ----------
        source_graph: QueryGraph
            source graph
        new_graph: QueryGraph
            new graph

        Returns
        -------
        Dict[str, Any]
            returns a dictionary with the difference in values in the cleaning operations
        """
        for view_graph_node in source_graph.iterate_sorted_graph_nodes(
            graph_node_types=GraphNodeType.view_graph_node_types()
        ):
            # get node from new graph
            new_view_graph_node = new_graph.get_node_by_name(view_graph_node.name)

            # get cleaning operations from source and new graph
            source_cleaning_operations = self._get_cleaning_operations_from_view_graph_node(
                view_graph_node
            )
            new_cleaning_operations = self._get_cleaning_operations_from_view_graph_node(
                new_view_graph_node
            )

            # compare cleaning operations
            if source_cleaning_operations != new_cleaning_operations:
                return {"default": source_cleaning_operations, "feature": new_cleaning_operations}
        return {}
