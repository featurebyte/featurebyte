#/bin/sh

# Checks if docker is installed
if which docker; then
  echo "Docker is not installed, or not found on PATH"
  echo "Please install docker and/or add docker binary to your PATH"
fi

# Setting credentials to pull from featurebyte repository
cat "beta_key.json.b64" | docker login -u "_json_key_base64" --password-stdin https://us-central1-docker.pkg.dev

# Pulls image (This checks for image updates)
docker compose pull

# Starts service
docker compose up
