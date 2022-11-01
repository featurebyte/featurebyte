# Installation

FeatureByte consists of a Python SDK, and a REST API service.

The Python SDK used to define and manage features. It can be used in notebooks for data exploration and feature engineering, and in python scripts to facilitate workflow integration.

The API service is used to manage and store metadata in a database.

## Python SDK
The FeatureByte Python SDK requires Python 3.8 and later as a prerequisite.
It’s generally a good practice to use a virtual environment for installing python packages.

### pip
If you use `pip`, you can install FeatureByte with:
```python
pip install -U featurebyte
```

### Configuration
The default location for the FeatureByte configuration file is `~/.featurebyte/config.yaml`.

#### profile
A profile defines the location of the API service, and the access token to use (if necessary).
The default profile that connects to the local service looks like this:
```toml
profile:
  - name: local
    api_url: http://localhost:8088
```
Multiple profiles can be added for access to different FeatureByte API services

## REST API Service
The API service can be set up in a local environment, or on a server for remote access.

To setup the service, check out the source code from the [FeatureByte repository](https://github.com/featurebyte/featurebyte).

### Quick start
**Note:** Customization is needed to setup the service on a Windows machine

Start the service using the following command
```commandline
make start-service
```

This will build a local docker image and serve the API service at `http://localhost:8088`

Stop the service using the following command
```commandline
make stop-service
```

### Configuration
Configurations for the service can be specified in the configuration file.

#### logging
Customize the logging level to control how much logging information is displayed.
```toml
logging:
  level: INFO
```

#### credentials
Specify credentials to be used to access feature stores in the service
```toml
credential:
  - feature_store: "Snowflake Store"
    credential_type: USERNAME_PASSWORD
    username: demo
    password: xxx
```

### Customization
Customization can be made to serve the service on a different port, or to map data directories to a different host location.

A `docker-compose.yml` file with default settings is provided in the `docker` directory.

```toml
services:
  featurebyte-service:
    container_name: featurebyte-service
    image: local/featurebyte:latest
    volumes:
      - ~/.featurebyte/mongo:/data
      - ~/.featurebyte:/home/user/.featurebyte
      - /tmp:/tmp
    environment:
      MONGODB_URI: mongodb://localhost:27021/?replicaSet=rs0
      API_PORT: 8088
      WORKERS: 3
    ports:
      - 27021:27021
      - 27022:27022
      - 8088:8088
```
Modify the contents of the docker-compose file to customize settings for the service.

### Troubleshooting
Check the docker logs if service fails to work.
```commandline
docker logs featurebyte-service
```
