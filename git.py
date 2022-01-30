import logging
import os
import subprocess
from typing import Any, List, Union


class Git:
    def __init__(self, repopath=os.getcwd()):
        self.repopath = repopath

    def _command_safe(
        self, cmd: Union[str, List[str]], cwd=None
    ) -> subprocess.CompletedProcess:
        """_command_safe ensures commands are run safely and raise exceptions
        on error

        https://stackoverflow.com/questions/4917871/does-git-return-specific-return-error-codes
        """
        WARNINGS = ["not something we can merge"]

        cwd = self.repopath if cwd is None else cwd
        cmd = list(cmd)
        try:
            return subprocess.run(
                cmd,
                cwd=cwd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
                encoding="utf-8",
            )
        except subprocess.CalledProcessError as error:
            logging.info(error.stdout)
            if error.stderr:
                if any((warning for warning in WARNINGS if warning in error.stderr)):
                    logging.warning(error.stderr)
                else:
                    logging.error(error.stderr)
                    raise
            return subprocess.CompletedProcess(
                returncode=0, args="", stdout=error.stdout
            )

    def git_reset_branch(self):
        return self._command_safe(["git", "checkout", "."], self.repopath)

    def git_list_all_branches(self, url=None) -> List[str]:
        if url is None:
            return [
                item.strip()
                for item in self._command_safe(
                    ["git", "branch", "-r"], self.repopath
                ).stdout.split("\n")[1:-1]
            ]
        return [
            item
            for item in self._command_safe(
                ["git", "ls-remote", "--heads", "--refs", url],
                self.repopath,
            ).stdout.split("\n")
        ]

    def git_snapshot(self, url) -> List[Any]:
        # TODO return better data type
        return [
            item.split("\t")[1].replace("refs/heads/", "")
            for item in self._command_safe(
                ["git", "ls-remote", "--heads", "--refs", url],
                self.repopath,
            ).stdout.split("\n")
            if len(item) > 0
        ]

    def git_show(self, branch, path_spec):
        return self._command_safe(
            [
                "git",
                "show",
                f"{branch}:{path_spec}",
            ],
            self.repopath,
        )

    def git_fetch(self):
        return self._command_safe(["git", "fetch"], self.repopath)

    def git_add(self, _file_path=None):
        cmd = ["git", "add", "--all"]
        if _file_path is not None:
            cmd = ["git", "add", _file_path]
        return self._command_safe(cmd, self.repopath)

    def git_checkout(self, branch_name, path_spec=None, local_path=None, force=False):

        cmd = ["git", "checkout"]

        cmd.append(branch_name)
        if path_spec is not None:
            cmd.append("--")
            cmd.append(path_spec)
            if local_path is not None:
                cmd.append(local_path)
        try:
            return self._command_safe(cmd, self.repopath)
        except subprocess.CalledProcessError as _:
            if force:
                cmd.insert(2, "-b")
                logging.info("%s does not exist; force flag is %s", branch_name, force)
                return self._command_safe(cmd, self.repopath)

    def git_commit(self, message):
        """git_commit

        Args:
            username (str):
            name (str): name of report to commit
            repopath (str, optional): local repository path if not cwd. Defaults to os.getcwd().

        Returns:
            CompletedProcess:
        """
        return self._command_safe(
            ["git", "commit", "-m", f"'{message}'"], self.repopath
        )

    def git_status(self):
        """status returns the output from git status

        Args:
            repopath (str, optional): The root path of the repo. Defaults to os.getcwd().

        Returns:
            CompletedProcess
        """
        return self._command_safe(["git", "status"], self.repopath)

    def git_pull(self, destination="origin", branch=None):
        """git_pull

        Args:
            destination (str, optional): Defaults to "origin".
            branch (str, optional): Defaults to current branch.
            repopath (str, optional): Defaults to os.getcwd().

        Returns:
            CompletedProcess
        """

        cmd = ["git", "pull", destination]
        if branch:
            cmd.append(branch)
        return self._command_safe(cmd, self.repopath)

    def git_push(self, destination="origin", branch=None):
        """git_push

        Args:
            destination (str, optional): Defaults to "origin".
            branch (str, optional): Defaults to current branch.
            repopath (str, optional): Defaults to os.getcwd().

        Returns:
            CompletedProcess
        """
        cmd = ["git", "push", destination]
        if branch is not None:
            cmd.append(branch)
        return self._command_safe(cmd, self.repopath)

    def git_clone(self, url, target_path):
        """git_clone

        Args:
            url (str): remote url
            target_path (str): local target path

        Returns:
            CompletedProcess
        """
        cmd = ["git", "clone", url, target_path]
        return self._command_safe(cmd, target_path)

    def git_merge(self, machine_name):
        cmd = ["git", "merge", f"{machine_name}"]
        return self._command_safe(cmd)

    def git_rebase(self, branch_name):
        return self._command_safe(["git", "rebase", f"origin/{branch_name}"])

    def git_log(self, branch_name):
        return self._command_safe(["git", "log", "--format=%B", f"{branch_name}"])
