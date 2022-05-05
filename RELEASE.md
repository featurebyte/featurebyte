## Release Steps

### Building and releasing featurebyte package

- Bump the version with `poetry version <version>`. You can pass the new version explicitly, or a rule such as `major`, `minor`, or `patch`. For more details, refer to the [Semantic Versions](https://semver.org/) standard.
- Create a commit
- Create a `GitHub release`.
- And... publish 🙂 `poetry publish --build`
