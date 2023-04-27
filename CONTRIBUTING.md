# How to contribute

---

## System Requirements

---

### Python

We use `python` >=3.8 with `poetry` to manage the dependencies. </br><small>Refer to [poetry](https://python-poetry.org/docs/#installation) for installation instructions.</small>

### Java

Java 11 or higher is required.

### Build tools

We use `task` to manage our build and development scripts and `docker` to manage the environment.
</br><small>Refer to [taskfile](https://taskfile.dev/#/usage?id=installation) for installation instructions.</small>
</br><small>Refer to [docker](https://docs.docker.com/get-docker/) for installation instructions.</small>


## Init Development environment

---

```bash
task install
```

To activate your `virtualenv` run `poetry shell`.

## Initializing

Run `task init` to install precommit hooks and
validate that you have all the required dependencies to develop.

## Codestyle

### Formatting

Apply automatic code formatting by running this command:
```commandline
task format
```

### Checks

Many checks are configured for this project:

* Command `task lint-style` will check black, isort and darglint
* Command `task lint-type` will check typing issues using mypy
* Command `task lint-safety` command will look at the security of your code

Command `task lint` applies all checks.

### Before submitting

Before submitting your code please do the following steps:

1. Add any changes you want
2. Add tests for the new changes
3. Edit documentation if you have changed something significant
4. Run `task format` to format your changes.
5. Run `task lint` to ensure that types, security and docstrings are okay.

## Creating a Pull Request

We use [`Release Drafter`](https://github.com/marketplace/actions/release-drafter) to draft release notes from pull requests as they get merged. Label your pull request according to the table below to track changes under the correct category.

|        **Pull Request Label**         | **Category in Release Notes** |
|:-------------------------------------:|:-----------------------------:|
|       `enhancement`, `feature`        |          🚀 Features          |
| `bug`, `refactoring`, `bugfix`, `fix` |    🔧 Fixes & Refactoring     |
|       `build`, `ci`, `testing`        |    📦 Build System & CI/CD    |
|              `breaking`               |      💥 Breaking Changes      |
|            `documentation`            |       📝 Documentation        |
|            `dependencies`             |    ⬆️ Dependencies updates    |

## Checking documention

To check documentation locally run `task docs` command. It will build documentation and start a server listening on port `localhost:8000`.

## Other help

You can contribute by spreading a word about this library.
It would also be a huge contribution to write
a short article on how you are using this project.
You can also share your best practices with us.
