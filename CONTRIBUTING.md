# Contributing

## Pull requests

1. Branch from the `main` branch. If you are implementing a feature name it using the 
   Jira ticket `111-name-of-feature`, if you are implementing a bugfix name it
   `bug-issue-name`. If they are associated with a specific issue, you
   may use the issue number in place of the name.
2. Update the README and other documentation with details of major changes.
3. Once you are ready for review please open a pull/merge request to the
   `main` branch.
4. You may merge the Pull/Merge Request in once you have the sign-off of one
   maintainers.

## Releases

1. The versioning scheme we use is [SemVer](http://semver.org/). Use `bump2version` to
   increment the version number. Use `beta` for UAT releases.

## Code style

- We name variables using few nouns in lowercase, e.g. `mapping_names`
  or `increment`.
- We name functions using verbs in lowercase, e.g. `map_variables_to_names` or
  `change_values`.
- We use the [numpydoc](https://numpydoc.readthedocs.io/en/latest/format.html)
  format for documenting features using docstrings.
- Black is used to enforce PEP8.

## Review process

1. When we want to release the package we will request a formal review for any
   non-minor changes.
2. Reviewers comments should be addressed before merging.
4. Only once reviewers are satisfied, will the feature branch be merged onto `main`.
