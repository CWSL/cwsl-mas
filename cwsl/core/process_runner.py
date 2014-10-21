"""
Authors: Tim Bedin, Tim Erwin

Copyright 2014 CSIRO

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contains the ProcessRunner class

"""

import subprocess
import os
import logging

module_logger = logging.getLogger('cwsl.core.process_runner')



class ProcessRunner(object):

    ''' This class handles the actual execution of tasks within the VisTrails
    plugin.

    We may replace this with something from CLTools later, but I think this
    will be simpler for now.

    '''

    def process_run(self, command, infiles, outfiles, simulate=False):

        """ This method executes a command either on the local machine

        or on a remote computer.

        """

        command_list = command.split()

        if simulate:
            for output in outfiles:
                print("Making directory: {0}".\
                      format(os.path.dirname(output)))

            print("Simulating command...")
            arguments = ['echo'] + command_list + infiles + outfiles
            return_code = subprocess.call(arguments)
            if return_code != 0:
                raise BadReturnError

            return return_code

        else:

            print("Creating directories")
            for output in outfiles:
                try:
                    os.makedirs(os.path.dirname(output))
                except OSError:
                    # Catch the case when you are working in a base
                    # directory - '' is not a directory.
                    pass

            arguments = command_list + infiles + outfiles
            print("Running command arguments: {0}".format(arguments))
            module_logger.debug("Running command arguments: {0}".format(arguments))

            return_code = subprocess.call(arguments)
            if return_code != 0:
                raise BadReturnError

            return return_code


class BadReturnError(Exception):
    """ An error raised when the command being run by the runner doesn't

    return 0
    """

    pass
