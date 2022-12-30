# Checks if docker is installed
if ((Get-Command "docker.exe" -ErrorAction SilentlyContinue) -eq $null) {
  Write-Host "Docker is not installed, or not on your PATH"
  Write-Host "Please install docker and/or configure docker cli to be on PATH"
}

# Loads docker image into your docker daemon
if ((docker images | Select-String Pattern "featurebyte-beta" -ErrorAction SilentlyContinue).Line -eq $null) {
  docker load -i .\featurebyte-beta.tar
} else {
  Write-Host "featurebyte-beta" already uploaded
}

# Starts service
docker compose up
