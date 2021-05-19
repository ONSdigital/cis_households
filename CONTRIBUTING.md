# Contributing

**Please note that this project is open for external reuse and review but closed for contribution.**

These standards are for internal contributors only.

## Pull requests

1. Branch from the `main` branch. All branches should be named using the
   Jira ticket number `111-name-of-feature`.
2. Update the README and other documentation with details of major changes.
3. Once you are ready for review please open a pull/merge request to the
   `main` branch and select one or more reviewers.
4. You may merge the Pull/Merge Request in once you have the sign-off of one
   reviewer.

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


## Development Setup

### Development installation

Create a new virtual environment, including python 3.6.8. `conda` can be used for this purpose:

```
conda create -n "cis" python=3.6.8
```

Download the repository using `git clone` and change directory into the project.

Activate the `conda` environment and install the project development dependencies:

```
activate cis
pip install pypandoc==1.5
pip install -e .[dev]
```

Our required `pyspark` version depends on `pypandoc`, but dependency resolution doesn't seem to install this first. As such, this needs to be installed manually.

### Configure pre-commit

We use `pre-commit` to clean up the format of our files and to screen for secrets before commiting.

Install `pre-commit` in the repository by running:

```
pre-commit install
```

**If you are developing on Windows**, you will need to manually direct the pre-commit hook to use the python executable that is used in our virtual environment.

Run `where python` to identify the path to the Python executable.

Open the pre-commit hook files within the repo, at `./.git/hooks/pre-commit` and append the path to the Python executable to the first line. This folder is hidden, so you may need to reveal it in the file explorer.
For example, the first line of the file should now appear:

```
#!/usr/bin/env C:\Anaconda\envs\cis\python.exe
```

Your next commit should prepare the pre-commit hook (takes ages) and then run the hooks described in `.pre-commit-config.yaml`.
Subsequent commits will trigger the hooks (considerably faster) and clean up the files before creating the commit.

If one of the hooks (e.g. `black`) alters a file, you will need to `git add` the file again and re-run the commit to confirm the changes.
