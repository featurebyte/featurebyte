#!/bin/bash

mongosh --port=27021 <<EOF
var config = {
    "_id": "rs0",
    "version": 1,
    "members": [
        {
            "_id": 1,
            "host": "localhost:27021",
            "priority": 1
        },
        {
            "_id": 2,
            "host": "localhost:27022",
            "priority": 2
        },
    ]
};
rs.initiate(config, { force: true });
rs.status();
EOF
