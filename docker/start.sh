#/bin/sh

# Loads docker image into your docker daemon
docker load -i ./featurebyte-beta.tar

# Brings up the docker server
docker compose up
