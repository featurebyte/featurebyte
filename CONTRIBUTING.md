# How to contribute

## System Requirements

### Python

We use `python` >=3.9 with `uv` to manage the dependencies.

### Java

Java 11 or higher is required.

### Build tools

We use `task` to manage our build and development scripts and `docker` to manage the environment.
</br><small>Refer to [taskfile](https://taskfile.dev/#/usage?id=installation) for installation instructions.</small>
</br><small>Refer to [docker](https://docs.docker.com/get-docker/) for installation instructions.</small>


## Init Development environment



```shell
$ task install
```

To activate your `virtualenv` run.

### Linting

Linting tools are configured for this project, Command `task lint` applies all lints.

## Testing

We use `pytest` to run tests. To run tests use `task test` command.

* Command `task test` will run all tests
* Command `test test-unit` will run unit tests

**NOTE: You will be unable to run snowflake or databricks tests because you do not have a connection to the databases.**

Open a pull request with your changes and said tests will run in the CI environment.

### Before submitting

Before submitting your code please do the following steps:

1. Add any changes you want
2. Add tests for the new changes
3. Edit documentation if you have changed something significant
4. Run `task format` to format your changes.
5. Run `task lint` to ensure that types, security and docstrings are okay.
6. Add a changelog file in .changelog/ folder. Refer to `.changelog/TEMPLATE.yaml`.


### Checking documentation

To check documentation locally run `task docs` command. It will build documentation and start a server listening on port `localhost:8000`.

## Other help

You can contribute by spreading a word about this library.
It would also be a huge contribution to write
a short article on how you are using this project.
You can also share your best practices with us.
