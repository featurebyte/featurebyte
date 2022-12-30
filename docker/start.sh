#/bin/sh

# Checks if docker is installed
if which docker; then
  echo "Docker is not installed, or not found on PATH"
  echo "Please install docker and/or add docker binary to your PATH"
fi

# Loads docker image into your docker daemon
if docker images | grep featurebyte-beta; then
  echo "featurebyte-beta image found"
else
  echo "loading featurebyte-beta into local docker daemon"
  docker load -i ./featurebyte-beta.tar
fi

# Starts service
docker compose up
