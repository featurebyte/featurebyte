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
![Coverage Report](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/kchua78/773e2960183c0a6fe24c644d95d71fdb/raw/coverage.json)

**Next Generation Feature Engineering and Management**

</div>

## Overview

Created for data scientists and data engineers, the FeatureByte SDK is a free and source available platform that radically simplifies the creation and serving of state-of-the-art Machine Learning features. It empowers data scientists to build, deploy and share features and production-ready machine learning data pipelines in minutes — instead of weeks or months.

With FeatureByte SDK, data scientists and data engineers can speed up experimentation and deployment of more accurate ML models. By putting control in the hands of data scientists, FeatureByte accelerates innovation and frees up time for data engineers.

## 🚀 Features

* Feature Engineering Python SDK: Purpose-built for Machine Learning
* Infrastructure Abstraction: Integrate seamlessly with scalable data platforms
* Create features, not data pipelines: Leave the plumbing to FeatureByte
* Centralized Feature Management: Manage feature engineering with one tool

## :hammer_and_pick: Installation

### 1. Standalone Installation

Local installation of the FeatureByte service is the easiest way to get started with the FeatureByte SDK.
It is a single-user installation that can be used to prototype features locally with your data warehouse.

**1.1. Hardware Requirements:**

We recommend the following minimal hardware for running the local service

* Intel, AMD or Apple Silicon processor with 4 cpu cores
* 8GB of RAM

**1.2. Software Requirements**

* Docker service

**1.3. Install featurebyte Package**

* To avoid potential conflicts with other packages it is strongly recommended to use a virtual environment (venv) or a conda environment.

```bash
pip install featurebyte
```

**1.4. Launch the playground environment with python, or from the command shell**

```python
import featurebyte as fb
fb.playground()
```

**1.5. Cleaning Up**

```python
fb.stop()
```

### 2. Hosted Service & High Availability Installation

Refer to <a href="https://docs.featurebyte.com/0.1/get_started/installation/">Documentation</a>


## 📝 Documentation

[//]: # (TODO: When documentation server is released, change the URL)
Read the latest [documentation](https://docs.featurebyte.com).


## :card_file_box: Examples

#### Register and Annotate Data

```python
# Get table from data warehouse
data_source = catalog.get_data_source(
    "Spark Warehouse"
)
source_table = data_source.get_table(
    database_name="RETAIL",
    schema_name="PUBLIC",
    table_name="INVOICE",
)

# Register table as event table
invoices = source_table.create_event_table(
    name="INVOICE",
    event_id_column="InvoiceId",
    event_timestamp_column="Timestamp",
)
```

#### Create and Save Feature

```python
# Get view from catalog
invoices = catalog.get_view("INVOICES")

# Customer average spend over past 5 weeks
features = invoices.groupby(
    "CustomerId"
).aggregate_over(
    "Amount",
    method="avg",
    feature_names=["AvgSpend5w"],
    fill_value=0,
    windows=["5w"]
)

# Save feature
features["AvgSpend5w"].save()
```

#### Create and Deploy Feature List

```python
# Create feature list
feature_list = fb.FeatureList(
    [
        catalog.get_feature("AvgSpend5w"),
        catalog.get_feature("AvgSpend3w"),
    ],
    name="CustomerSpend",
)

# Get training data
train_df = feature_list.get_historical_features(
    observation_set
)

# Deploy for online serving
feature_list.deploy()
```

#### Define Data Cleaning Policy on Table

```python
# Get table from catalog
line_items = catalog.get_table("LINE_ITEM")

# Discount cannot be negative
line_items.Discount.update_critical_data_info(
    cleaning_operations=[
        fb.MissingValueImputation(
            imputed_value=0
        ),
        fb.ValueBeyondEndpointImputation(
            type="less_than",
            end_point=0,
            imputed_value=0
        ),
    ]
)
```

## 📈 Releases

You can see the list of available releases on the [GitHub Releases](https://github.com/featurebyte/featurebyte/releases) page.
Releases are versioned using the [Semantic Versions](https://semver.org/) specification.

## 🛡 License

[![License](https://img.shields.io/github/license/featurebyte/featurebyte)](https://github.com/featurebyte/featurebyte/blob/main/LICENSE)

This project is licensed under the terms of the `Elastic License 2.0` license. See [LICENSE](https://github.com/featurebyte/featurebyte/blob/main/LICENSE) for more details.

## :wrench: Contributing
All contributions are welcomed. Please adhere to the [CODE_OF_CONDUCT](https://github.com/featurebyte/featurebyte/blob/main/CODE_OF_CONDUCT.md) and read the
[Developer's Guide](https://github.com/featurebyte/featurebyte/blob/main/CONTRIBUTING.md) to get started.
