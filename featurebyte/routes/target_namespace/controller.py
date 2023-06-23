"""
Target namespace controller
"""
from bson import ObjectId

from featurebyte.models.target_namespace import TargetNamespaceModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.target_namespace import TargetNamespaceInfo, TargetNamespaceList
from featurebyte.service.target_namespace import TargetNamespaceService


class TargetNamespaceController(
    BaseDocumentController[TargetNamespaceModel, TargetNamespaceService, TargetNamespaceList],
):
    """
    TargetNamespace controller
    """

    paginated_document_class = TargetNamespaceList

    async def get_info(self, document_id: ObjectId, verbose: bool) -> TargetNamespaceInfo:
        """
        Get target namespace info given document_id

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Flag to control verbose level

        Returns
        -------
        TargetNamespaceInfo
        """
        _ = verbose
        target_namespace = await self.service.get_document(document_id=document_id)
        return TargetNamespaceInfo(
            name=target_namespace.name,
            default_version_mode=target_namespace.default_version_mode,
            default_target_id=target_namespace.default_target_id,
        )
