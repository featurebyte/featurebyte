#/bin/sh

# Loads docker image into your docker daemon
if docker images | grep featurebyte-beta; then
  echo "featurebyte-beta image found"
else
  echo "loading featurebyte-beta into local docker daemon"
  docker load -i ./featurebyte-beta.tar
fi

# Brings up the docker server
docker compose up
