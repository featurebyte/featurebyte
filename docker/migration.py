"""
Perform database migration
"""
import asyncio

from featurebyte.app import User
from featurebyte.logger import logger
from featurebyte.migration.run import run_migration
from featurebyte.utils.persistent import get_persistent

logger.info("Running database migration")
asyncio.run(run_migration(user=User(), persistent=get_persistent()))
logger.info("Database migration completed")
