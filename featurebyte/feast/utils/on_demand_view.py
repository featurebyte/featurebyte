"""
On demand feature view related classes and functions.
"""
from typing import Any, Dict, List, Optional, Union, cast

from unittest.mock import patch

from feast import FeatureView, Field, RequestSource
from feast.feature_view_projection import FeatureViewProjection
from feast.on_demand_feature_view import OnDemandFeatureView, on_demand_feature_view

from featurebyte.enum import DBVarType, SpecialColumnName
from featurebyte.feast.enum import to_feast_primitive_type
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature import FeatureModel
from featurebyte.models.offline_store_feature_table import OfflineStoreFeatureTableModel
from featurebyte.models.offline_store_ingest_query import OfflineFeatureTableSignature


def create_feast_on_demand_feature_view(
    definition: str,
    function_name: str,
    sources: List[Union[FeatureView, RequestSource, FeatureViewProjection]],
    schema: List[Field],
) -> OnDemandFeatureView:
    """
    Create a Feast OnDemandFeatureView from a function definition.

    Parameters
    ----------
    definition: str
        Definition of the function to be used to create the OnDemandFeatureView
    function_name: str
        Name of the function to be used to create the OnDemandFeatureView
    sources: List[Union[FeatureView, RequestSource]]
        List of FeatureViews and RequestSources to be used as sources for the OnDemandFeatureView
    schema: List[Field]
        List of Fields to be used as schema for the OnDemandFeatureView

    Returns
    -------
    OnDemandFeatureView
        The created OnDemandFeatureView
    """
    # get the function from the definition
    locals_namespace: Dict[str, Any] = {}
    exec(definition, locals_namespace)  # pylint: disable=exec-used  # nosec
    func = locals_namespace[function_name]

    # dill.source.getsource fails to find the source code of the function
    # since we already have the source code, we can just use that instead
    with patch("dill.source.getsource", return_value=definition):
        output = on_demand_feature_view(sources=sources, schema=schema)(func)

    return cast(OnDemandFeatureView, output)


class OnDemandFeatureViewConstructor:
    """
    Class to construct OnDemandFeatureView from a feature's offlineStoreInfo.
    """

    @classmethod
    def create(
        cls,
        table_signature_to_model: Dict[OfflineFeatureTableSignature, OfflineStoreFeatureTableModel],
        feature_model: FeatureModel,
        name_to_feast_feature_view: Dict[str, FeatureView],
        name_to_feast_request_source: Dict[str, RequestSource],
        entity_id_to_serving_name: Dict[PydanticObjectId, str],
    ) -> OnDemandFeatureView:
        """
        Create a Feast OnDemandFeatureView from an offline store info.

        Parameters
        ----------
        table_signature_to_model: Dict[OfflineFeatureTableSignature, OfflineStoreFeatureTableModel]
            Dict of OfflineFeatureTableSignatures to OfflineStoreFeatureTableModels to be used to create the
        feature_model: FeatureModel
            FeatureModel to be used to create the OnDemandFeatureView
        name_to_feast_feature_view: Dict[str, FeatureView]
            Dict of FeatureView names to FeatureViews to be used as sources for the OnDemandFeatureView
        name_to_feast_request_source: Dict[str, RequestSource]
            Dict of RequestSource names to RequestSources to be used as sources for the OnDemandFeatureView
        entity_id_to_serving_name: Dict[PydanticObjectId, str]
            Dict of Entity ids to serving names

        Returns
        -------
        OnDemandFeatureView
            The created OnDemandFeatureView or None if the offline store info does not result in an
            OnDemandFeatureView
        """
        offline_store_info = feature_model.offline_store_info
        assert feature_model.name is not None, "FeatureModel does not have a name"

        sources: List[Union[FeatureView, RequestSource, FeatureViewProjection]] = []
        for ingest_query in offline_store_info.extract_offline_store_ingest_query_graphs():
            table_signature = ingest_query.get_full_table_signature(
                feature_store_id=feature_model.tabular_source.feature_store_id,
                catalog_id=feature_model.catalog_id,
                entity_id_to_serving_name=entity_id_to_serving_name,
            )
            table = table_signature_to_model[table_signature]
            fv_source = name_to_feast_feature_view[table.full_name]
            sources.append(fv_source)

            # add point in time request source
            req_source = name_to_feast_request_source[SpecialColumnName.POINT_IN_TIME.value]
            sources.append(req_source)

        assert offline_store_info.odfv_info is not None, "OfflineStoreInfo does not have ODFV info"
        feature_view = create_feast_on_demand_feature_view(
            definition=offline_store_info.odfv_info.codes,
            function_name=offline_store_info.odfv_info.function_name,
            sources=sources,
            schema=[
                Field(
                    name=feature_model.versioned_name,
                    dtype=to_feast_primitive_type(DBVarType(feature_model.dtype)),
                )
            ],
        )
        return feature_view
