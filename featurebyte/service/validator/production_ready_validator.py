"""
Production ready validator
"""
from typing import Any, Dict, Optional, cast

from featurebyte import FeatureJobSetting
from featurebyte.enum import TableDataType
from featurebyte.models import EventDataModel
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature import FeatureReadiness
from featurebyte.query_graph.enum import GraphNodeType, NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.graph_node.base import GraphNode
from featurebyte.query_graph.model.common_table import BaseTableData
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import GroupByNode
from featurebyte.query_graph.node.input import InputNode, ItemDataInputNodeParameters
from featurebyte.query_graph.transform.flattening import GraphFlatteningTransformer
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.tabular_data import DataService


class ProductionReadyValidator:
    """
    Validator for checking whether it is valid to upgrade a feature to the PRODUCTION_READY status.
    """

    def __init__(
        self,
        feature_namespace_service: FeatureNamespaceService,
        data_service: DataService,
    ):
        self.feature_namespace_service = feature_namespace_service
        self.data_service = data_service

    async def validate(
        self, feature_name: str, node: Node, graph: QueryGraph, ignore_guardrails: bool = False
    ) -> None:
        """
        Validate.

        Parameters
        ----------
        feature_name: str
            feature name
        node: Node
            node
        graph: QueryGraph
            graph
        ignore_guardrails: bool
            parameter to determine whether to ignore guardrails
        """
        await self._assert_no_other_production_ready_feature(feature_name)
        # We will skip these additional checks if the user explicit states that they want to ignore these
        # guardrails.
        if not ignore_guardrails:
            feature_job_setting_diff = await self._get_feature_job_setting_diffs(node, graph)
            cleaning_ops_diff = await self._get_cleaning_operations_diffs(node, graph)
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
    def _get_feature_job_setting_from_graph(
        node: Node, graph: QueryGraph
    ) -> Optional[FeatureJobSetting]:
        """
        Get feature job setting from graph.

        Parameters
        ----------
        node: Node
            node
        graph: QueryGraph
            graph

        Returns
        -------
        Optional[FeatureJobSetting]
            feature job setting
        """
        for current_node in graph.iterate_nodes(target_node=node, node_type=NodeType.GROUPBY):
            groupby_node = cast(GroupByNode, current_node)
            parameters = groupby_node.parameters
            blind_spot_str = f"{parameters.blind_spot}s"
            frequency_str = f"{parameters.frequency}s"
            time_modulo_frequency_str = f"{parameters.time_modulo_frequency}s"
            return FeatureJobSetting(
                blind_spot=blind_spot_str,
                frequency=frequency_str,
                time_modulo_frequency=time_modulo_frequency_str,
            )
        return None

    @staticmethod
    def _get_input_event_data_id(node: Node, graph: QueryGraph) -> Optional[PydanticObjectId]:
        """
        Get input event data ID.

        Parameters
        ----------
        node: Node
            node
        graph: QueryGraph
            graph

        Returns
        -------
        Optional[PydanticObjectId]
            event data id if the feature comes from an event and item data, None if not.
        """
        event_id_from_item_data: Optional[PydanticObjectId] = None
        event_id_from_event_data: Optional[PydanticObjectId] = None
        for current_node in graph.iterate_nodes(target_node=node, node_type=NodeType.INPUT):
            input_node = cast(InputNode, current_node)
            if input_node.parameters.type == TableDataType.ITEM_DATA:
                parameters = cast(ItemDataInputNodeParameters, input_node.parameters)
                event_id_from_item_data = parameters.event_data_id
            if input_node.parameters.type == TableDataType.EVENT_DATA:
                event_id_from_event_data = input_node.parameters.id
        if event_id_from_item_data is not None:
            return event_id_from_item_data
        return event_id_from_event_data

    async def _get_feature_job_setting_for_data_source(
        self, event_data_id: PydanticObjectId
    ) -> Optional[FeatureJobSetting]:
        """
        Get feature job setting for a particular event data id.

        Parameters
        ----------
        event_data_id: PydanticObjectId
            event data ID

        Returns
        -------
        Optional[FeatureJobSetting]
            feature job setting if present
        """
        event_data = await self.data_service.get_document(document_id=event_data_id)
        assert isinstance(event_data, EventDataModel)
        return event_data.default_feature_job_setting

    async def _get_feature_job_setting_diffs(self, node: Node, graph: QueryGraph) -> Dict[str, Any]:
        """
        Check that the job setting matches what is present in the data source.

        Parameters
        ----------
        node: Node
            node
        graph: QueryGraph
            graph

        Returns
        -------
        Dict[str, Any]
            returns a dictionary with the difference in values in the FeatureJobSetting
        """
        input_event_data_id = ProductionReadyValidator._get_input_event_data_id(node, graph)
        # If there is no input_event_data_id, this means that the feature is not created from an item or event data.
        # This means we can skip this validation.
        if input_event_data_id is None:
            return {}
        current_feature_job_setting = ProductionReadyValidator._get_feature_job_setting_from_graph(
            node, graph
        )
        if current_feature_job_setting is None:
            return {}
        data_source_feature_job_setting = await self._get_feature_job_setting_for_data_source(
            input_event_data_id
        )
        if data_source_feature_job_setting is None:
            raise ValueError(
                f"no feature job setting found for input data ID {input_event_data_id}"
            )
        if current_feature_job_setting != data_source_feature_job_setting:
            return {
                "default": data_source_feature_job_setting,
                "feature": current_feature_job_setting,
            }
        return {}

    @staticmethod
    def _get_cleaning_node_from_feature_graph(node: Node, graph: QueryGraph) -> Optional[GraphNode]:
        """
        Get feature job setting from graph.

        Parameters
        ----------
        node: Node
            node
        graph: QueryGraph
            graph

        Returns
        -------
        Optional[GraphNode]
            cleaning graph node if it's found, None otherwise
        """
        flattened_graph, node_name_map = GraphFlatteningTransformer(graph=graph).transform(
            skip_flattening_graph_node_types={GraphNodeType.CLEANING}
        )
        new_node = flattened_graph.get_node_by_name(node_name_map[node.name])
        for current_node in flattened_graph.iterate_nodes(
            target_node=new_node, node_type=NodeType.GRAPH
        ):
            graph_node = cast(GraphNode, current_node)
            if graph_node.parameters.type == GraphNodeType.CLEANING:
                return graph_node
        return None

    async def _get_cleaning_node_from_input_data(
        self, node: Node, graph: QueryGraph
    ) -> Optional[GraphNode]:
        """
        Get cleaning node from input data

        Parameters
        ----------
        node: Node
            node
        graph: QueryGraph
            graph

        Returns
        -------
        Optional[GraphNode]
            graph node

        Raises
        ------
        ValueError
            raised if no data input node is found for the graph
        """
        data_input_node = graph.get_input_node(node.name)
        if data_input_node is None:
            raise ValueError("no input data node found")
        # We retrieve the value from the data store to see what has been persisted.
        data_id = data_input_node.parameters.id
        if data_id is None:
            raise ValueError("data input id could not be found")
        input_data = await self.data_service.get_document(data_id)
        assert isinstance(input_data, BaseTableData)
        input_node = input_data.construct_input_node(
            data_input_node.parameters.feature_store_details
        )
        return input_data.construct_cleaning_recipe_node(input_node)

    @staticmethod
    def _diff_cleaning_nodes(
        cleaning_node_default: Optional[GraphNode], cleaning_node_feature: Optional[GraphNode]
    ) -> Dict[str, Any]:
        """
        Diff 2 graph cleaning nodes.

        Parameters
        ----------
        cleaning_node_default: Optional[GraphNode]
            cleaning node a
        cleaning_node_feature: Optional[GraphNode]
            cleaning node b

        Returns
        -------
        Dict[str, Any]
            differences between the 2 nodes
        """
        if cleaning_node_default is None and cleaning_node_feature is None:
            return {}
        if cleaning_node_default is None:
            return {
                "feature": cleaning_node_feature.parameters.graph.dict(),  # type: ignore[union-attr]
            }
        if cleaning_node_feature is None:
            return {
                "default": cleaning_node_default.parameters.graph.dict(),
            }
        cleaning_node_default_graph_dict = cleaning_node_default.parameters.graph.dict()
        cleaning_node_feature_graph_dict = cleaning_node_feature.parameters.graph.dict()
        if cleaning_node_default_graph_dict != cleaning_node_feature_graph_dict:
            return {
                "default": cleaning_node_default_graph_dict,
                "feature": cleaning_node_feature_graph_dict,
            }
        return {}

    async def _get_cleaning_operations_diffs(self, node: Node, graph: QueryGraph) -> Dict[str, Any]:
        """
        Get differences between cleaning operations in the graph and the data source.

        Parameters
        ----------
        node: Node
            node
        graph: QueryGraph
            graph

        Returns
        -------
        Dict[str, Any]
            returns a dictionary with the difference in values in the cleaning operations
        """
        cleaning_node_from_graph = ProductionReadyValidator._get_cleaning_node_from_feature_graph(
            node, graph
        )
        cleaning_node_from_data = await self._get_cleaning_node_from_input_data(node, graph)
        return ProductionReadyValidator._diff_cleaning_nodes(
            cleaning_node_from_data, cleaning_node_from_graph
        )
