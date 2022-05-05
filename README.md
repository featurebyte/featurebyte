# featurebyte

<div align="center">

[![Build status](https://github.com/featurebyte/featurebyte/workflows/build/badge.svg?branch=main&event=push)](https://github.com/featurebyte/featurebyte/actions?query=workflow%3Abuild)
[![Python Version](https://img.shields.io/pypi/pyversions/featurebyte.svg)](https://pypi.org/project/featurebyte/)
[![Dependencies Status](https://img.shields.io/badge/dependencies-up%20to%20date-brightgreen.svg)](https://github.com/featurebyte/featurebyte/pulls?utf8=%E2%9C%93&q=is%3Apr%20author%3Aapp%2Fdependabot)

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Security: bandit](https://img.shields.io/badge/security-bandit-green.svg)](https://github.com/PyCQA/bandit)
[![Pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/featurebyte/featurebyte/blob/main/.pre-commit-config.yaml)
[![Semantic Versions](https://img.shields.io/badge/%20%20%F0%9F%93%A6%F0%9F%9A%80-semantic--versions-e10079.svg)](https://github.com/featurebyte/featurebyte/releases)
[![License](https://img.shields.io/github/license/featurebyte/featurebyte)](https://github.com/featurebyte/featurebyte/blob/main/LICENSE)
![Coverage Report](assets/images/coverage.svg)

Manage and serve Machine Learning Features for Data Science applications

</div>

## Installation

```bash
pip install -U featurebyte
```

or install with `Poetry`

```bash
poetry add featurebyte
```

Then you can run

```bash
featurebyte --help
```

or with `Poetry`:

```bash
poetry run featurebyte --help
```

### Install from source

Checkout the featurebyte repo:
```bash
git clone git@github.com:featurebyte/featurebyte.git && cd featurebyte
```

If you don't have `Poetry` installed run:

```bash
make poetry-download
```

Install module:

```bash
make install
```

## 🚀 Features

- Supports for `Python 3.7` and higher.

## 📈 Releases

You can see the list of available releases on the [GitHub Releases](https://github.com/featurebyte/featurebyte/releases) page.

We follow [Semantic Versions](https://semver.org/) specification.

We use [`Release Drafter`](https://github.com/marketplace/actions/release-drafter). As pull requests are merged, a draft release is kept up-to-date listing the changes, ready to publish when you’re ready. With the categories option, you can categorize pull requests in release notes using labels.

### List of labels and corresponding titles

|               **Label**               |  **Title in Releases**  |
| :-----------------------------------: | :---------------------: |
|       `enhancement`, `feature`        |       🚀 Features       |
| `bug`, `refactoring`, `bugfix`, `fix` | 🔧 Fixes & Refactoring  |
|       `build`, `ci`, `testing`        | 📦 Build System & CI/CD |
|              `breaking`               |   💥 Breaking Changes   |
|            `documentation`            |    📝 Documentation     |
|            `dependencies`             | ⬆️ Dependencies updates |

You can update it in [`release-drafter.yml`](https://github.com/featurebyte/featurebyte/blob/main/.github/release-drafter.yml).

GitHub creates the `bug`, `enhancement`, and `documentation` labels for you. Dependabot creates the `dependencies` label. Create the remaining labels on the Issues tab of your GitHub repository, when you need them.

## 🛡 License

[![License](https://img.shields.io/github/license/featurebyte/featurebyte)](https://github.com/featurebyte/featurebyte/blob/main/LICENSE)

This project is licensed under the terms of the `Apache Software License 2.0` license. See [LICENSE](https://github.com/featurebyte/featurebyte/blob/main/LICENSE) for more details.

## 📃 Citation

```bibtex
@misc{featurebyte,
  author = {FeatureByte},
  title = {Python Library for FeatureOps},
  year = {2022},
  publisher = {GitHub},
  journal = {GitHub repository},
  howpublished = {\url{https://github.com/featurebyte/featurebyte}}
}
```

## Credits [![🚀 Your next Python package needs a bleeding-edge project structure.](https://img.shields.io/badge/python--package--template-%F0%9F%9A%80-brightgreen)](https://github.com/TezRomacH/python-package-template)

This project was generated with [`python-package-template`](https://github.com/TezRomacH/python-package-template)
