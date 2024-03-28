"""
CLI tools to run in featurebyte-server container
"""

import base64
import sys

from featurebyte.session.hive import HiveConnection

if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise ValueError("sql_b64 is required")
    if sys.argv[1] == "":
        raise ValueError("sql_b64 is empty")

    SQL_B64 = sys.argv[1]
    sql = base64.b64decode(SQL_B64.encode("utf-8")).decode("utf-8")

    conn = HiveConnection(host="spark-thrift")
    for statement in sql.split(";"):
        cursor = conn.cursor()
        statement = statement.strip()
        if not statement:
            continue
        print(statement)
        cursor.execute(statement)
        print(cursor.fetchall())
