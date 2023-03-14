# How to contribute

## Python
Python 3.8 or higher is required.

## Java
Java 11 or higher is required.

## Dependencies

We use `poetry` to manage the [dependencies](https://github.com/python-poetry/poetry).

To install dependencies and prepare [`pre-commit`](https://pre-commit.com/) hooks you would need to run `install` command:

```bash
make install
```

To activate your `virtualenv` run `poetry shell`.

## Initializing

Run `make init` to install precommit hooks and
validate that you have all the required dependencies to develop.

## Codestyle

### Formatting

Apply automatic code formatting by running this command:
```commandline
make format
```

### Checks

Many checks are configured for this project:

* Command `make lint-style` will check black, isort and darglint
* Command `make lint-type` will check typing issues using mypy
* Command `make lint-safety` command will look at the security of your code

Command `make lint` applies all checks.

### Before submitting

Before submitting your code please do the following steps:

1. Add any changes you want
2. Add tests for the new changes
3. Edit documentation if you have changed something significant
4. Run `make format` to format your changes.
5. Run `make lint` to ensure that types, security and docstrings are okay.

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


## Makefile usage

[`Makefile`](https://github.com/featurebyte/featurebyte/blob/main/Makefile) contains a lot of functions for faster development.

<details>
<summary>1. <code>make init</code></summary>
<p>

+ Checks for the cli dependencies.
+ And adds pre-commit hook.
</p>
</details>

<details>
<summary>2. <code>make install</code></summary>
<p>

+ Installs dependencies with respect to the generated poetry.lock file
</p>
</details>

<details>
<summary>3. <code>make format</code></summary>
<p>

+ runs <code>toml-sort</code>
+ runs <code>isort</code>
+ runs <code>black</code>
+ runs <code>pylint</code>
+ runs <code>darglint</code>

These configuration are specified in pyproject.toml and setup.cfg
</p>
</details>

<details>
<summary>4. <code>make lint</code></summary>
<p>

Runs linting checks.
These configuration are specified in pyproject.toml and setup.cfg
</p>
</details>

<details>
<summary>5. <code>make test</code></summary>
<p>

+ Starts a local mongodb replicaset in docker
+ Starts a local featurebyte server
+ Runs unit and functional test against them
</p>
</details>

<details>
<summary>6. <code>make docs</code></summary>
<p>

Using `mkdocs` + `mike` to build docs and serves it locally.
If you are looking to develop/edit the docs, use `make docs-dev` which does hot reloading
</p>
</details>


## Other help

You can contribute by spreading a word about this library.
It would also be a huge contribution to write
a short article on how you are using this project.
You can also share your best practices with us.
