# ESMF Branch Summary Tool

Foobar is a Python library for dealing with word pluralization.



## Installation

[![CodeFactor](https://www.codefactor.io/repository/github/ryanlong1004/esmf-branch-summary/badge)](https://www.codefactor.io/repository/github/ryanlong1004/esmf-branch-summary)
[![PyPi Version](https://img.shields.io/pypi/v/new_project.svg)](https://pypi.org/project/new_project/)

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install foobar.

```bash
pip install esmf_branch_summary
```

## Usage

```bash
usage: esmf_branch_summary.py [-h] [-n NAME] [-log LOG] repo_path

esmf_branch_summary aggregates esmf framework test results from other branches into a summary file .

positional arguments:
  repo_path             path to esmf-artifacts-merge

optional arguments:
  -h, --help            show this help message and exit
  -n NAME, --name NAME  name of the branch to use. Example --name 'develop'
  -log LOG, --log LOG   Provide logging level. Example --log debug', default='warning'
```


## License
[MIT](https://choosealicense.com/licenses/mit/)