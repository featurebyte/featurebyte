"""
Production ready validator
"""
from typing import Any, Dict, List, cast

from bson import ObjectId

from featurebyte import ColumnCleaningOperation, FeatureJobSetting
from featurebyte.exception import NoChangesInFeatureVersionError
from featurebyte.models.feature import FeatureReadiness
from featurebyte.query_graph.enum import GraphNodeType, NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import GroupByNode
from featurebyte.query_graph.node.nested import BaseViewGraphNodeParameters
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.version import VersionService


class ProductionReadyValidator:
    """
    Validator for checking whether it is valid to promote a feature to the PRODUCTION_READY status.
    """

    def __init__(
        self,
        version_service: VersionService,
        feature_service: FeatureService,
    ):
        self.version_service = version_service
        self.feature_service = feature_service

    async def validate(
        self,
        promoted_feature_name: str,
        promoted_feature_id: ObjectId,
        promoted_feature_graph: QueryGraphModel,
        ignore_guardrails: bool = False,
    ) -> None:
        """
        Validate whether it is ok to promote the feature being passed in to PRODUCTION_READY status.

        Parameters
        ----------
        promoted_feature_name: str
            feature name of the feature being promoted to PRODUCTION_READY
        promoted_feature_id: ObjectId
            feature id of the feature being promoted to PRODUCTION_READY
        promoted_feature_graph: QueryGraphModel
            feature graph of the feature being promoted to PRODUCTION_READY
        ignore_guardrails: bool
            parameter to determine whether to ignore guardrails
        """
        await self._assert_no_other_production_ready_feature(
            promoted_feature_id, promoted_feature_name
        )
        # We will skip these additional checks if the user explicit states that they want to ignore these
        # guardrails.
        if not ignore_guardrails:
            try:
                source_feature = (
                    await self.version_service.create_new_feature_version_using_source_settings(
                        promoted_feature_id
                    )
                )
            except NoChangesInFeatureVersionError:
                # We can return here since there are no changes in the feature version.
                return
            feature_job_setting_diff = (
                await self._get_feature_job_setting_diffs_data_source_vs_promoted_feature(
                    source_feature.node, source_feature.graph, promoted_feature_graph
                )
            )
            cleaning_ops_diff = (
                await self._get_cleaning_operations_diff_data_source_vs_promoted_feature(
                    source_feature.graph, promoted_feature_graph
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
            "Discrepancies found between the promoted feature version you are trying to promote to "
            "PRODUCTION_READY, and the input data.\n"
            f"{diff_format_dict}\n"
            "Please fix these issues first before trying to promote your feature to PRODUCTION_READY."
        )

    async def _assert_no_other_production_ready_feature(
        self, promoted_feature_id: ObjectId, feature_name: str
    ) -> None:
        """
        Check to see if there are any other production ready features.

        Parameters
        ----------
        promoted_feature_id: ObjectId
            feature id of the feature being promoted to PRODUCTION_READY
        feature_name: str
            the name of the feature we are checking

        Raises
        ------
        ValueError
            raised when there is another feature version with the same name that is production ready
        """
        results = await self.feature_service.list_documents(query_filter={"name": feature_name})
        for feature in results["data"]:
            if feature["readiness"] == FeatureReadiness.PRODUCTION_READY:
                feature_id = feature["_id"]
                # If the version we are promoting is already production ready, we can return and skip this validation.
                if promoted_feature_id == feature_id:
                    return
                raise ValueError(
                    f"Found another feature version that is already PRODUCTION_READY. Please deprecate the feature "
                    f"{feature_name} with ID {feature_id} first before promoting the promoted version as there can "
                    "only be one feature version that is production ready at any point in time. We are unable to "
                    f"promote the feature with ID {promoted_feature_id} right now."
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
    async def _get_feature_job_setting_diffs_data_source_vs_promoted_feature(
        data_source_node: Node,
        data_source_graph: QueryGraph,
        promoted_feature_graph: QueryGraphModel,
    ) -> Dict[str, Any]:
        """
        Get feature job setting diffs between the feature version created from source data, and the promoted feature
        version that the user is trying to promote to PRODUCTION_READY.

        Parameters
        ----------
        data_source_node: Node
            source node
        data_source_graph: QueryGraph
            source graph
        promoted_feature_graph: QueryGraphModel
            promoted graph

        Returns
        -------
        Dict[str, Any]
            feature job setting diffs
        """
        for current_node in data_source_graph.iterate_nodes(
            target_node=data_source_node, node_type=NodeType.GROUPBY
        ):
            # Get corresponding group by node in promoted graph
            promoted_group_by_node = promoted_feature_graph.get_node_by_name(current_node.name)
            source_feature_job_setting = (
                ProductionReadyValidator._get_feature_job_setting_from_groupby_node(current_node)
            )
            promoted_feature_job_setting = (
                ProductionReadyValidator._get_feature_job_setting_from_groupby_node(
                    promoted_group_by_node
                )
            )
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

    async def _get_cleaning_operations_diff_data_source_vs_promoted_feature(
        self, data_source_feature_graph: QueryGraph, promoted_feature_graph: QueryGraphModel
    ) -> Dict[str, Any]:
        """
        Get cleaning operation diffs between the feature version created from source data, and the promoted feature
        version that the user is trying to promote to PRODUCTION_READY.

        Parameters
        ----------
        data_source_feature_graph: QueryGraph
            source graph
        promoted_feature_graph: QueryGraphModel
            promoted graph

        Returns
        -------
        Dict[str, Any]
            returns a dictionary with the difference in values in the cleaning operations
        """
        for view_graph_node in data_source_feature_graph.iterate_sorted_graph_nodes(
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
