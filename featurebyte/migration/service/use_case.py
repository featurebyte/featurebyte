"""
Use Case migration service
"""

from featurebyte.logging import get_logger
from featurebyte.migration.service import migrate
from featurebyte.migration.service.mixin import BaseDocumentServiceT, BaseMongoCollectionMigration
from featurebyte.models.use_case import UseCaseType
from featurebyte.persistent import Persistent
from featurebyte.service.use_case import UseCaseService

logger = get_logger(__name__)


class UseCaseMigrationServiceV23(BaseMongoCollectionMigration):
    """
    UseCaseMigrationService class

    This class is used to migrate the use case records to add use_case_type field.
    """

    # skip audit migration for this migration
    skip_audit_migration = True

    def __init__(
        self,
        persistent: Persistent,
        use_case_service: UseCaseService,
    ):
        super().__init__(persistent)
        self.use_case_service = use_case_service

    @property
    def delegate_service(self) -> BaseDocumentServiceT:
        return self.use_case_service  # type: ignore[return-value]

    @migrate(
        version=23,
        description="Add use_case_type field to use case records for model refactoring",
    )
    async def add_use_case_type_field(self) -> None:
        """Add use_case_type field to use case records"""
        query_filter = await self.delegate_service.construct_list_query_filter()

        # Find records that don't have use_case_type field
        query_filter_missing_type = {**query_filter, "use_case_type": {"$exists": False}}

        total = await self.get_total_record(query_filter=query_filter_missing_type)

        if total == 0:
            logger.info("No records found without use_case_type field")
            return

        logger.info("Found %d records without use_case_type field", total)

        # Update all records to have use_case_type = PREDICTIVE (backward compatibility)
        await self.delegate_service.update_documents(
            query_filter=query_filter_missing_type,
            update={"$set": {"use_case_type": UseCaseType.PREDICTIVE}},
        )

        logger.info("Updated all records with use_case_type field")

        # Verify the migration by checking a sample of records
        sample_size = min(10, total)
        sample_records = []
        async for use_case_dict in self.delegate_service.list_documents_as_dict_iterator(
            query_filter={**query_filter, "use_case_type": UseCaseType.PREDICTIVE},
            page_size=sample_size,
        ):
            sample_records.append(use_case_dict)
            if len(sample_records) >= sample_size:
                break

        # Sanity check that use_case_type field is present and has correct value
        for use_case_dict in sample_records:
            assert "use_case_type" in use_case_dict, (
                f"use_case_type field missing in {use_case_dict['_id']}"
            )
            assert use_case_dict["use_case_type"] == UseCaseType.PREDICTIVE, (
                f"Unexpected use_case_type value in {use_case_dict['_id']}"
            )

        logger.info("Migrated all records successfully (total: %d)", total)
