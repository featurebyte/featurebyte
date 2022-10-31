"""
Perform database migration
"""
import asyncio

from featurebyte.app import User
from featurebyte.migration.run import run_migration
from featurebyte.utils.persistent import get_persistent

print("Running database migration")
asyncio.run(run_migration(user=User(), persistent=get_persistent()))
print("Database migration completed")
