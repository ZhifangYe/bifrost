#!/usr/bin/env python
# -*- coding: utf-8 -*-

## Info----------------------------------------------------------------------------
##
## Author: Zhifang Ye
##
## Email: zhifang.ye.fghm@gmail.com
##
## Date Created: 2020-09-02
##
## Last Updated: 2020-09-02
##
## Notes:
##

import os
import subprocess

# Add user slurm if it is not presented in /etc/passwd
# This hack is used for using Slurm inside a singularity container
# The content of the environment variable USER_SLURM will be added in /etc/passwd
with open("/etc/passwd", "r") as f:
    user = [line.split(":")[0] for line in f]
if not "slurm" in user and os.getenv("USER_SLURM") is not None:
    subprocess.run("echo $USER_SLURM >> /etc/passwd", shell=True)