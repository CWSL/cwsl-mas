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
from datetime import datetime

from cwsl.configuration import USER, PROJECT

log = logging.getLogger("cwsl.utils.utils")

try:
    import vistrails.api
except ImportError:
    log.warning("Can not import VisTrails api - is it on the PYTHONPATH?")


def get_git_status(ifile):
    """
    Get git version information from filename
    """

    ifile = os.path.expandvars(ifile)

    if not os.path.exists(ifile):
        log.error("Script: {} does not exist on file system".format(ifile))
        git_version = "Could not determine file version."
        return git_version

    cwd = os.getcwd()
    (basepath, filename) = os.path.split(ifile)
    os.chdir(basepath)
    try:
        status = subprocess.check_output(['git','status',filename])
        version = subprocess.check_output(['git','log',filename])

        if not version:
            log.error("%s not in git repository" % ifile)
            return "Could not determine file version."

        version = re.search('commit (.*?)\n',version)
        modified = re.search('Changed',status)
        if modified:
            git_version = "Git info: %s - with uncommited modifications" % version.group(1)
        else:
            git_version = "Git info: %s" % version.group(1)

    except subprocess.CalledProcessError:
        log.error("Status called process failed.")
        git_version = "Could not determine file version."

    os.chdir(cwd)

    return git_version


def get_vistrails_info():
    """
    Return a tuple with the VisTrails vt file and version information.


    There is a possible API bug/confusion: This function is failing
    when run in headless / batch mode.

    """

    try:
        this_controller = vistrails.api.get_current_controller()
    except vistrails.api.NoVistrail:
        # This API call raises a NoVisTrail error in headless mode.
        return (None, None)

    filename = this_controller.vistrail.locator.name
    current_version = this_controller.current_version

    return(filename, current_version)


def build_metadata(command_line_list):
    """
    Takes in a full command line list, e.g. ['echo', 'infile.txt', 'outfile.txt']

    Create version metadata to embed in output files.
    Returns a string of form:
        User, NCI Project, Timestamp
        Current VisTrail, internal pipeline version, git version info
        Script to be run, git version info
        Full command line of the task.

    """

    rightnow = datetime.now()
    time_string = rightnow.isoformat()
    short_time = str(rightnow.year) + str(rightnow.month) + str(rightnow.day) + ':'

    vt_info = get_vistrails_info()

    if vt_info[0]:
        log.debug("vt_info is: {}".format(vt_info))
        # Get the git information about the script and the vistrails file.
        vt_git = get_git_status(vt_info[0])
    else:
        log.warning("No VisTrail internal information found!")
        vt_git = "No VisTrail infomation found."

    script_git = get_git_status(command_line_list[0])

    vt_info_list = [short_time, 'VISTRAIL FILE:', str(vt_info[0]), 'NODE NUMBER:', str(vt_info[1]), 'VERSION:', vt_git]

    real_script_name = os.path.expandvars(command_line_list[0])
    full_ver_string = (' '.join([short_time, 'USER:', USER, 'NCI PROJECT:', PROJECT, 'DATE/TIME:', time_string]) + '\n' +
                       ' '.join(vt_info_list) + '\n' +
                       ' '.join([short_time, 'SCRIPT:', real_script_name, 'VERSION:', script_git]) + '\n' +
                       ' '.join([short_time, 'CMD:'] + [real_script_name] + command_line_list[1:]) + '\n')

    return full_ver_string

