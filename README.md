# ESMF Branch Summary Tool

ESMF Branch Summary Tool automate summarizing ESMF Framework test results across
multiple permutations.

## Whats New?

* Using a sqlite3 database to aggregate results and ensures unique entries
can be added but not removed as each final summary is generated directly from the underly database.
* Can generate a larger history with the ```--number``` flag.
* Build Results have been added

## How Does it Work?
A user can run the tool by calling it with only the ```repo_path``` argument.

In this case, the tool fetches all remote branches and and generates permuations for processing in the form of *machine_name/branch_name*.

### Important!
It's important to understand that each **machine_name** has a corresponding branch, thought no referred to as branches.

Similiarly, **branch_name** refers to the branch of the ESMF Framework being tested, though it is represented as a root directory in the corresponding **machine_name** branch.

#### Example:
To check the test results of the *develop* branch on machine *Cheyenne*, you would look in the *Cheyenne* branch in the root folder *develop*.

## Flow

Continuing from our example above in which we invoked the tool with only the ```repo_path`` argument:

* Git fetch new changes
* If no branches have been supplied via the command line, fetch all the remote branches. (Otherwise, use only the branch(es) specified with the -b flag)
* Generate permutations for each branch/machine and itterate over them creating summaries.
* Checkout the *branch* that corresponds to the machine_name
* For each hash request with the ```--number``` flag, gather all ```build.log``` and ```summary.dat``` files that contain said hash.
* Search through the matching files for test results and build results for each permutation of *hash/branch/machine* and update the underlying database.
* Checkout the *summary* branch and generate summary files for each unique hash (as INSERT OR REPLACE)
* Write the summary files, commit, and push to *Summary* branch.


## Installation

[![CodeFactor](https://www.codefactor.io/repository/github/ryanlong1004/esmf-branch-summary/badge)](https://www.codefactor.io/repository/github/ryanlong1004/esmf-branch-summary)
[![PyPi Version](https://img.shields.io/pypi/v/new_project.svg)](https://pypi.org/project/new_project/)

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install esmf-branch-summary tool.

```bash
pip install esmf-branch-summary
```

## Usage

```bash
usage: esmf_branch_summary.py [-h] [-b BRANCHES [BRANCHES ...]] [-n NUMBER] [-l LOG] repo_path

esmf_branch_summary aggregates esmf framework test results from other branches into a summary file .

positional arguments:
  repo_path             path to esmf-artifacts-merge

optional arguments:
  -h, --help            show this help message and exit
  -b BRANCHES [BRANCHES ...], --branches BRANCHES [BRANCHES ...]
                        branch(es) to summarize. All by default. Example --name develop feature_1 feature_2
  -n NUMBER, --number NUMBER
                        number of commits to compile from most recentExample --number 10
  -l LOG, --log LOG     Provide logging level. Example --log debug', default='info'
```

If no branch is specified, the tool will checkout all remote branches from the repository and summarize each 
if the necessary assets are available.

The ```--numer``` flag allows you to build summaries for more than the most recent branch hash.

The ```--log``` flag takes standard Python logging options.




## License
[MIT](https://choosealicense.com/licenses/mit/)