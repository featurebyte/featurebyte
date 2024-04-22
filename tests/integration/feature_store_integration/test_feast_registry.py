"""
Test feast registry service
"""

import asyncio
import logging
import time
from threading import Thread

import pytest
from redis.exceptions import LockNotOwnedError


@pytest.fixture(name="registry_service")
def fixture_registry_service(app_container):
    """
    Create a registry service instance
    """
    return app_container.feast_registry_service


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
def test_registry_service__get_registry_storage_lock(registry_service, caplog):
    """Test get_registry_storage_lock"""

    async def task_with_lock(service, lock_name):
        """Task that acquires and releases a lock"""
        # Acquire lock
        with service.get_registry_storage_lock(timeout=2):
            logging.info("Lock acquired: " + lock_name)  # pylint: disable=logging-not-lazy
            time.sleep(1)
        logging.info("Lock released: " + lock_name)  # pylint: disable=logging-not-lazy

    # Set log level to INFO to capture log messages
    caplog.set_level(logging.INFO)

    # Run task_with_lock in two separate threads
    thread1 = Thread(target=asyncio.run, args=(task_with_lock(registry_service, "thread1"),))
    thread2 = Thread(target=asyncio.run, args=(task_with_lock(registry_service, "thread2"),))

    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()

    # Check log messages
    lines = [record.msg for record in caplog.records if record.msg.startswith("Lock")]
    assert len(lines) == 4
    assert "Lock acquired: thread" in lines[0]
    assert "Lock released: thread" in lines[1]
    assert "Lock acquired: thread" in lines[2]
    assert "Lock released: thread" in lines[3]

    # check timeout: when the lock is released, it is no longer owned
    with pytest.raises(LockNotOwnedError) as exc:
        with registry_service.get_registry_storage_lock(timeout=0.1):
            time.sleep(1)

    assert "Cannot release a lock that's no longer owned" in str(exc.value)
