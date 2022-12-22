<-- TODO (Chester): Update this -->

# How to contribute

## Python
Python 3.8 and higher is required.

## Dependencies

We use `poetry` to manage the [dependencies](https://github.com/python-poetry/poetry).

If you dont have `poetry`, you should install with `make poetry-download`. Make sure the poetry binary is included in `PATH` so it's accessible in the terminal.



To install dependencies and prepare [`pre-commit`](https://pre-commit.com/) hooks you would need to run `install` command:

```bash
make install
make pre-commit-install
```

To activate your `virtualenv` run `poetry shell`.

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
1. Add tests for the new changes
1. Edit documentation if you have changed something significant
1. Run `make format` to format your changes.
1. Run `make lint` to ensure that types, security and docstrings are okay.

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
<summary>1. Download and remove Poetry</summary>
<p>

To download and install Poetry run:

```bash
make poetry-download
```
Add Poetry binary to `PATH` to make it easily accessible.

To uninstall

```bash
make poetry-remove
```

</p>
</details>

<details>
<summary>2. Install all dependencies and pre-commit hooks</summary>
<p>

Install requirements:

```bash
make install
```

Pre-commit hooks coulb be installed after `git init` via

```bash
make pre-commit-install
```

</p>
</details>

<details>
<summary>3. Codestyle</summary>
<p>

Automatic formatting uses `pyupgrade`, `isort` and `black`.

```bash
make format
```

Codestyle checks only, without rewriting files:

```bash
make lint-style
```

> Note: `lint-style` uses `isort`, `black` and `darglint` library

Update all dev libraries to the latest version using one comand

```bash
make update-dev-deps
```
</p>
</details>

<details>
<summary>4. Code security</summary>
<p>

This command launches `Poetry` integrity checks as well as identifies security issues with `Safety` and `Bandit`.

```bash
make lint-safety
```

</p>
</details>

<details>
<summary>5. Type checks</summary>
<p>

Run `mypy` static type checker

```bash
make mypy
```

</p>
</details>

<details>
<summary>6. Tests with coverage badges</summary>
<p>

Run `pytest`

```bash
make test
```

</p>
</details>

<details>
<summary>7. All linters</summary>
<p>

Of course there is a command to ~~rule~~ run all linters in one:

```bash
make lint
```

</p>
</details>

<details>
<summary>8. Build Artifacts</summary>
<p>

Build distribution artifacts:

```bash
make build-artifacts
```

Artifacts will be created in the folder `dist`

</p>
</details>

<details>
<summary>9. Build Documentation</summary>
<p>

Build documentation:

```bash
make build-docs
```

The documentation will be created in the folder `build/docs`

</p>
</details>

<details>
<summary>10. Cleanup</summary>
<p>
Delete pycache files

```bash
make pycache-remove
```

Remove package build

```bash
make build-remove
```

Delete .DS_STORE files

```bash
make dsstore-remove
```

Remove .mypycache

```bash
make mypycache-remove
```

Or to remove all above run:

```bash
make cleanup
```

</p>
</details>

## Other help

You can contribute by spreading a word about this library.
It would also be a huge contribution to write
a short article on how you are using this project.
You can also share your best practices with us.
