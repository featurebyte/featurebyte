## Releasing a new Version

This document outlines the steps to release a new version of the project.

### 1. Updating the version number

Update pyproject.toml with the new version for example 3.4.0 is the new release

```diff
[project]
authors = [{name = "FeatureByte", email = "it-admin@featurebyte.com"}]
name = "featurebyte"
- version = "3.3.1"
+ version = "3.4.0"
```
then run `task lint:format` to rebuild the package and the lockfile.

### 2. Updating the changelog

Run `task changelog` to generate the new release notes and update the CHANGELOG.md file.

### 3. Merging to main

Create a PR and merge to main

### 4. Creating the release

For creating a patch release, you should only update the existing branch `release/x.y` where x.y is the major.minor version number. For example, for version 3.4.1, you should update the branch `release/3.4`.

For a minor or major release, create a new branch `release/x.y` from main where x.y is the major.minor version number. For example, for version 3.4.0, you should create the branch `release/3.4`.

In all cases, push the branch to remote or create a PR to merge to the release branch.

### 5. Publishing

Go to actions tab in github and trigger the publish-public workflow on the release branch. Remember to select the release branch that you are publishing for. For example for a new version 3.1.2, you should select the branch `release/3.1` and a version of `3.1.2`.
