#!/usr/bin/env python
# -*- coding: utf-8 -*-

## Info----------------------------------------------------------------------------
##
## Author: Zhifang Ye
##
## Email: zhifang.ye.fghm@gmail.com
##
## Date Created: 2020-08-13
##
## Last Updated: 2020-09-15
##
## Notes:
##

from __future__ import annotations
from pathlib import Path
import os
import subprocess
import time
from typing import Optional, Union


class Slurm:
    def __init__(
        self,
        account: Optional[str] = None,
        partition: Optional[str] = None,
        n_nodes: int = 1,
        n_cpus: int = 1,
        n_mem: str = "8G",
        time: str = "3-00:00:00",
        job_name: str = "Job",
        log_dir: Union[str, os.PathLike] = None,
        log_format: Optional[str] = None,
        chdir: Optional[Union[str, os.PathLike]] = None,
        mail_user: Optional[str] = None,
        mail_type: Optional[str] = None,
        extra_args: list[str] = None,
    ) -> None:

        self.account = _parse_account(account=account)
        self.partition = _parse_partition(partition=partition)
        self.n_nodes = n_nodes
        self.n_cpus = n_cpus
        self.n_mem = n_mem
        self.time = time
        self.job_name = job_name
        self.log_dir = _parse_logdir(log_dir=log_dir)
        self.log_format = log_format
        self.output = (
            f"{self.log_dir}/%x_%j.log" if log_format is None else f"{self.log_dir}/{log_format}"
        )
        self.chdir = chdir
        self.mail_user = _parse_mail(mail_user)
        self.mail_type = mail_type
        self.extra_args = extra_args if extra_args is not None else []
        self.job_id = []
        self.job_status = dict()
        self.job_script = []

    def wrap_command(
        self,
        command: list[str],
        batch_mode: bool = True,
        array_list: list[str] = None,
        array_index: str = "index",
        interpreter: str = "bash",
    ) -> str:

        # Command
        if not isinstance(command, list):
            raise ValueError(
                "Command needs to be a list of string. "
                "Each string represents a line of command in the script (batch mode) "
                "or a single command (commandline mode)."
            )
        # Array job
        if array_list is not None:
            self.extra_args += [f"--array=0-{len(array_list)-1}"]
            if interpreter == "bash":
                command = [
                    f"ARRAY_VALUE=({' '.join(array_list)})",
                    f"{array_index}=${{ARRAY_VALUE[$SLURM_ARRAY_TASK_ID]}}",
                ] + command
            elif interpreter == "python":
                command = [
                    "import os",
                    f"array_value = {array_list}",
                    f'{array_index} = array_value[int(os.getenv("SLURM_ARRAY_TASK_ID"))]',
                ] + command
        # Log file
        if array_list is not None and self.log_format is None:
            self.output = f"{self.log_dir}/%x_%A_%a.log"
        # Assemble command
        if batch_mode:
            if interpreter == "bash":
                script = ["#!/bin/bash\n"]
            elif interpreter == "python":
                script = ["#!/usr/bin/env python\n"]
            else:
                script = [f"#!{interpreter}\n"]
            script += self.__parse_metadata(batch_mode=True)
            script = "\n".join(script + [""] + command)
        else:
            script = ["sbatch"]
            script += self.__parse_metadata(batch_mode=False)
            script = " ".join(script + [f"--wrap=\"{'; '.join(command)}\""])

        return script

    def submit_command(
        self, command: list[str], array_list: list[str] = None, array_index: str = "index"
    ):

        wrapped = self.wrap_command(command, array_list=array_list, array_index=array_index)
        proc = subprocess.run(
            ["sbatch"], input=wrapped, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
        )
        self.__register_job(proc.stdout)
        self.job_script.append(wrapped)
        print(proc.stdout)

    def submit_file(self, job_file: Union[str, os.PathLike], override: bool = False):

        if override:
            script = ["sbatch"] + self.__parse_metadata(batch_mode=False) + [job_file]
            print(script)
        else:
            script = ["sbatch", job_file]
        proc = subprocess.run(script, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        self.__register_job(proc.stdout)
        if override:
            self.job_script.append(
                Path(job_file).read_text()
                + "\n\n# Note: Slrum parameters have been modified during job submission."
            )
        else:
            self.job_script.append(Path(job_file).read_text())
        print(proc.stdout)

    def write_command(
        self,
        command: list[str],
        filename: Union[str, os.PathLike] = "Job.sh",
        batch_mode: bool = True,
        array_list: list[str] = None,
        array_index: str = "index",
        interpreter: str = "bash",
    ):
        wrapped = self.wrap_command(
            command,
            batch_mode=batch_mode,
            array_list=array_list,
            array_index=array_index,
            interpreter=interpreter,
        )
        with open(filename, "w") as f:
            f.write(wrapped)

    def get_status(self):
        job_status = get_status(self.job_id)
        self.job_status.update(job_status)
        return job_status

    def get_info(
        self,
        output_format: str = "default",
        no_header: bool = False,
        allocations: bool = True,
        extra_args: list[str] = [],
    ):
        get_info(
            self.job_id,
            output_format=output_format,
            no_header=no_header,
            allocations=allocations,
            extra_args=extra_args,
        )

    def wait_completion(self):

        job_status = wait_completion(self.job_id, return_status=True)
        self.job_status.update(job_status)

    def __parse_metadata(self, batch_mode: bool = True) -> list[str]:

        metadata = [
            f"--account={self.account}",
            f"--partition={self.partition}",
            f"--job-name={self.job_name}",
            f"--nodes={self.n_nodes}",
            f"--cpus-per-task={self.n_cpus}",
            f"--mem={self.n_mem}",
            f"--time={self.time}",
            f"--output={self.output}",
        ]
        if self.mail_user is not None and self.mail_type is not None:
            metadata += [f"--mail-user={self.mail_user}", f"--mail-type={self.mail_type}"]
        if self.chdir is not None:
            metadata += [f"--chdir={self.chdir}"]
        metadata += self.extra_args

        if batch_mode:
            metadata = [f"#SBATCH {i}" for i in metadata]

        return metadata

    def __register_job(self, stdout):
        if stdout.split()[0] == "Submitted":
            master_id = stdout.split()[-1]
            while True:
                job_id = []
                queue = get_queue(
                    no_header=True, extra_args=[f"-j {master_id}", "-r"], notebook_mode=False
                )
                for line in queue.split("\n"):
                    if not line == "":
                        job_id.append(line.split()[0])
                if len(job_id) > 0:
                    break
                time.sleep(1)
            self.job_id += job_id
            job_status = get_status(job_id)
            if len(job_status) == 0:
                for i in job_id:
                    job_status[i] = None
            self.job_status.update(job_status)


def submit_file(job_file: Union[str, os.PathLike], notebook_mode: bool = True):

    proc = subprocess.run(
        ["sbatch", job_file], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )
    if notebook_mode:
        print(proc.stdout)
    else:
        return proc.stdout


def get_queue(
    user_id: str = None,
    account_id: str = None,
    no_header: bool = False,
    extra_args: list[str] = [],
    notebook_mode: bool = True,
) -> str:
    cmd = ["squeue"] + extra_args
    cmd += ["--user", user_id] if user_id else []
    cmd += ["--account", account_id] if account_id else []
    cmd += ["--noheader"] if no_header else []
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if notebook_mode:
        print(proc.stdout)
    else:
        return proc.stdout


def get_info(
    job_id: Union[int, str],
    output_format: str = "default",
    no_header: bool = False,
    allocations: bool = True,
    extra_args: list[str] = [],
    notebook_mode: bool = True,
) -> str:

    cmd = ["sacct", "-j", _parse_jobid(job_id)] + extra_args
    cmd += ["-n"] if no_header else []
    cmd += ["-X"] if allocations else []
    if output_format == "default":
        cmd += [
            "--format",
            ",".join(
                [
                    "JobID%20",
                    "JobName%25",
                    "State",
                    "Partition",
                    "Elapsed",
                    "AllocCPUS",
                    "AllocNodes",
                ]
            ),
        ]
    else:
        cmd += ["--format", output_format]

    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    if notebook_mode:
        print(proc.stdout)
    else:
        return proc.stdout


def get_status(job_id: Union[int, str]) -> dict:

    job_info = get_info(
        job_id,
        output_format="JobID,State",
        no_header=True,
        extra_args=["--parsable2"],
        notebook_mode=False,
    ).split("\n")

    status = dict()
    for line in job_info:
        if not line == "":
            status[line.split("|")[0]] = line.split("|")[1]

    return status


def wait_completion(job_id: Union[int, str, list[Union[int, str]]], return_status: bool = False):

    while True:
        job_status = get_status(job_id)
        # All jobs are successfully finished
        if _all_status(job_status, "COMPLETED"):
            print("All jobs have successfully finished.")
            break
        # Any job is not in RUNNING or PENDING state
        elif not _any_status(job_status, "RUNNING") and not _any_status(job_status, "PENDING"):
            err_id = [i for i, j in job_status.items() if j not in ["RUNNING", "PENDING"]]
            print("Failed jobs:\n")
            get_queue(extra_args=["-j", _parse_jobid(err_id)])
            break
        # Special case for PENDING due to "ReqNodeNotAvail, Reserved for maintenance"
        if _any_status(job_status, "PENDING"):
            pend_id = [i for i, j in job_status.items() if j == "PENDING"]
            queue_info = get_queue(extra_args=["-j", _parse_jobid(pend_id)], print=False).split(
                "\n"
            )
            print("Failed jobs:\n")
            print(queue_info[0])
            for i in [i for i in queue_info if "ReqNodeNotAvail" in i]:
                print(i)
            break
        time.sleep(10)
    if return_status:
        return job_status


def _any_status(job_status, status):
    return any([i == status for i in job_status.values()])


def _all_status(job_status, status):
    return all([i == status for i in job_status.values()])


def _parse_jobid(job_id: Union[int, str, list[Union[int, str]]]) -> str:

    msg = (
        "Job id should be int, str or list of int/str.\n"
        "For specifying multiple job id in one string, differet ids should be separated by comma."
    )
    if isinstance(job_id, int):
        valid_id = str(job_id)
    elif isinstance(job_id, str) and all(
        [i.isdigit() for i in job_id.replace("_", ",").split(",")]
    ):
        valid_id = job_id
    elif isinstance(job_id, (list, tuple)):
        valid_id = []
        for i in job_id:
            if isinstance(i, (int, str)) and str(i).replace("_", "").isdigit():
                valid_id.append(str(i))
            else:
                raise ValueError(msg)
        valid_id = ",".join(valid_id)
    else:
        raise ValueError(msg)

    return valid_id


def _parse_account(account: Optional[str] = None) -> str:
    if account is None:
        if os.getenv("SLURM_ACCOUNT") is not None:
            account = os.getenv("SLURM_ACCOUNT")
        else:
            raise ValueError("Account should be set by argument or the env variable SLURM_ACCOUNT")
    return account


def _parse_partition(partition: Optional[str] = None) -> str:
    if partition is None:
        if os.getenv("SLURM_PARTITION") is not None:
            partition = os.getenv("SLURM_PARTITION")
        else:
            raise ValueError(
                "Partition should be set by argument or the env variable SLURM_PARTITION"
            )
    return partition


def _parse_logdir(log_dir: Union[str, os.PathLike] = None) -> Union[str, os.PathLike]:
    if log_dir is None:
        log_dir = os.getenv("LOG_DIR") if os.getenv("LOG_DIR") is not None else os.getcwd()
    return log_dir


def _parse_mail(mail_user: Optional[str] = None) -> Optional(str):
    if mail_user is None and os.getenv("SLURM_NOTIFY_EMAIL") is not None:
        mail_user = os.getenv("SLURM_NOTIFY_EMAIL")
    return mail_user

