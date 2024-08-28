"""
Helper functions for udf teseting
"""

from sqlglot import expressions

from featurebyte.query_graph.sql.common import get_fully_qualified_function_call, sql_to_string


async def execute_query_with_udf(session, function_name, args):
    udf_expr = get_fully_qualified_function_call(
        session.database_name,
        session.schema_name,
        function_name,
        args,
    )
    query = sql_to_string(
        expressions.select(expressions.alias_(udf_expr, alias="OUT", quoted=False)),
        session.source_type,
    )
    df = await session.execute_query(query)
    output = df.iloc[0]["OUT"]
    return output
