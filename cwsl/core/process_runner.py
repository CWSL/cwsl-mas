'''
Created on 31/05/2013

The ProcessRunner class is used as a mixin that to
tell the ProcessUnit class how to actually run a
command on the system.

Contact: Tim Bedin (Tim.Bedin@csiro.au)
         Ric Pascual (Ricardo.Pascual@csiro.au)
         Tim Erwin (Tim.Erwin@csiro.au)

@filename: process_runner.py

Part of the WP2 Model Analysis Service
VisTrails plugin.

Copyright CSIRO 2013

'''

__version__ = "$Revision: 1545 $"

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
