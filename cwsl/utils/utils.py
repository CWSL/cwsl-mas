"""

Copyright 2014 CSIRO

Authors: Tim Erwin, Tim Bedin

Utility functions to determine version information (both git and vistrails),
which is used to annotate netCDF files for reproducability.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

import os, re, subprocess, logging

import vistrails.api


def get_git_status(ifile):
    """
    Get git version information from filename
    """

    if not os.path.exists(ifile):
        raise Exception

    cwd = os.getcwd()
    os.chdir(os.path.dirname(ifile))
    status = subprocess.check_output(['git','status',ifile])
    version = subprocess.check_output(['git','log',ifile])
    os.chdir(cwd)

    version = re.search('commit (.*?)\n',version)
    modified = re.search('Changed',status)
    if modified:
        git_version = "Git info: %s - with uncommited modifications" % version.group(1)
    else:
        git_version = "Git info: %s" % version.group(1)
    
    return git_version


def get_vistrails_info():
    """
    Return a tuple with the VisTrails vt file and version information.

    """

    this_controller = vistrails.api.get_current_controller()
    
    filename = this_controller.vistrail.locator.name
    current_version = this_controller.current_version

    return(filename, current_version)

