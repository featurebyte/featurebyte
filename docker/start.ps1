# Checks if docker is installed
if ((Get-Command "docker.exe" -ErrorAction SilentlyContinue) -eq $null) {
  Write-Host "Docker is not installed, or not on your PATH"
  Write-Host "Please install docker and/or configure docker cli to be on PATH"
}

# Setting credentials to pull from featurebyte repository
Get-Content "beta_key.json.b64" | docker login -u "_json_key_base64" --password-stdin https://us-central1-docker.pkg.dev

# Pulls image (This checks for image updates)
docker compose pull

# Starts the service
#    Ctrl+c to stop the service
docker compose up
