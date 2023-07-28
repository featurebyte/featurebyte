"""
Thread-safety tests for sessions
"""
import asyncio
import threading

import pytest

from featurebyte.session.manager import SOURCE_TYPE_SESSION_MAP


def get_threadsafe_sessions():
    """
    Get a list of sessions that are thread-safe
    """
    testable = {"snowflake", "spark"}
    threadsafe_sessions = {
        str(source_type)
        for source_type, cls in SOURCE_TYPE_SESSION_MAP.items()
        if cls.is_threadsafe()
    }
    return list(testable & threadsafe_sessions)


@pytest.mark.skip
@pytest.mark.parametrize("source_type", get_threadsafe_sessions(), indirect=True)
@pytest.mark.asyncio
async def test_threadsafety(session, source_type):
    """
    Test that sessions marked as thread safe are actually thread safe
    """
    table_name = "TEST_THREADSAFETY"

    async def run(session_obj, value):
        """
        Insert value into column A of the table
        """
        print(f"Inserting value {value}")
        query = f"""
            merge into {table_name} as target
            using (select {value} as A) as source
            on target.A = source.A
            when matched then update set target.A = source.A
            when not matched then insert (A) values (source.A)
            """
        await session_obj.execute_query(query)

    if source_type == "spark":
        await session.execute_query(f"create table {table_name} (A int) using delta")
    else:
        await session.execute_query(f"create table {table_name} (A int)")

    threads = []
    values = list(range(10))
    for i in values:
        t = threading.Thread(target=asyncio.run, args=(run(session, i),))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()

    df = await session.execute_query(f"select * from {table_name}")
    assert set(df["A"].tolist()) == set(values)


@pytest.mark.skip
@pytest.mark.parametrize("source_type", ["snowflake", "spark"], indirect=True)
@pytest.mark.asyncio
async def test_threadsafety_api_object(event_table):
    """
    Test that the API objects are thread safe regardless of source type
    """

    def run(data_obj, value):
        df = data_obj.preview(limit=value)
        assert df.shape[0] == value

    threads = []
    values = list(range(1, 11))
    for i in values:
        t = threading.Thread(target=run, args=(event_table, i))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
