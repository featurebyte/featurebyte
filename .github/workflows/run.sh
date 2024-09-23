#!/bin/sh

set +e
make test-integration-spark
EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
    echo "Spark Thrift Server logs:"
    docker logs spark-thrift --tail 2000
    exit $EXIT_CODE
fi
