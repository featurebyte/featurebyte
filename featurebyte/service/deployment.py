"""
DeploymentService class
"""
from __future__ import annotations

from featurebyte.models.deployment import DeploymentModel
from featurebyte.schema.deployment import DeploymentUpdate
from featurebyte.service.base_document import BaseDocumentService


class DeploymentService(BaseDocumentService[DeploymentModel, DeploymentModel, DeploymentUpdate]):
    """
    DeploymentService class
    """

    document_class = DeploymentModel
    document_update_class = DeploymentUpdate


class AllDeploymentService(DeploymentService):
    """
    AllDeploymentService class
    """

    @property
    def is_catalog_specific(self) -> bool:
        return False
