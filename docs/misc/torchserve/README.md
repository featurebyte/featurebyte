# Example TorchServe handler implementation

This project contains an example of TorchServe handler with [Sentence Transformers](https://www.sbert.net) model deployed.

> **Note**: The goal of this project is to show an example of how to implement torchserve handler with transformer model, how to run and deploy it.
> This example might not be sufficient for real production scenario (e.g. GPU hardware requirement).

### Prerequisites
1. [Java 11+](https://docs.oracle.com/en/java/javase/11/install/overview-jdk-installation.html)
2. [Python 3.9](https://www.python.org/downloads/release/python-390/)
3. [Poetry](https://python-poetry.org/docs/#installation)
4. [Taskfile.dev](https://taskfile.dev)
5. (Optional) [Docker](https://www.docker.com)

### Steps

##### Install dependencies

```bash
task install
```

##### Run locally

This will create torchserve archive and start the server.

```bash
task run-local
```

Once the server is started it can be tested via:

```bash
curl -XPOST http://localhost:8080/predictions/transformer -H 'Content-Type: application/json' -d '{"text": "hello world"}'
```

##### Run in Docker

This will create torchserve archive, build docker image and run container.

```bash
task run-docker
```

Once container is started, it can be tested in the same way as in local run.
