# -*- coding: utf-8 -*-
"""

Authors: David Kent, Tim Bedin, Tim Erwin

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

"""

import os, sys
from textwrap import dedent
import tempfile
import subprocess
import logging

log = logging.getLogger('cwsl.core.scheduler')

class Job(object):

    """The string that is used to generate the batch script"""
    def __init__(self, **kwargs):
        """Initialise a Job. Allowed args are:
           1. Resources defined in L{__valid_resources}.
           2. Fields in L{__req_fields}.
        """
    
        self.precmds = []
        self.cmds = []
        self.outdirs = set()

    def add_pre_cmd(self, args):
        """Add a command to the list of commands to be executed by the Job.

        @param args: Command line to be added.
        @type  args: string
        """
        self.precmds.append(args)

    def queue_cmd(self, args):
        """Add a command to the list of commands to be executed by the Job.

        @param args: Command line to be added.
        @type  args: string
        """
        self.cmds.append(args)

    def __repr__(self):
        return self.to_str()
    
    def escape(self, s):
        return s #:s.replace('"', '\"')

    def to_str(self):
        raise NotImplementedException

class SimpleJob(Job):
    """Represents a job that can be submitted to a queue.
    Tracks the resources the job will be allowed to use and the commands it will
    execute. Also provides functionality to convert the job to a shell script
    for submission to the queue.
    """

    __template = """
                 #!/bin/sh

                 %(cmds)s
                 """

    def __repr__(self):
        return self.to_str()

    def to_str(self, deps=None):
        """Return this Job as a string. The string will be a multi-line string
        that can be output to file as a shell script complete with PBS options
        to set batch job options and resources.
        """
        t = dedent(self.__template).strip()
        d = {}

        cmdlines = [self.escape(' '.join(args)) for args in self.precmds + self.cmds]
        cmds = '\n'.join(cmdlines) + '\n'
        d['cmds'] = cmds
        
        return t % d
        
    def submit(self, deps=True, noexec=False):
        """Submit the job to the queue.

        @param deps: Whether or not to submit the tasks on which this task
                     depends.
        @type  deps: boolean
        @param noexec: Whether or not to actually submit the job for execution.
                       If True the resulting shell script is printed to stdout.
        @type  noexec: boolean
        """

        #Add directory creation
        if len(self.outdirs) > 0:
            self.add_pre_cmd(['mkdir', '-p'] + sorted(self.outdirs))

        if noexec:
            log.warning("Would run script:\n\n========>\n%s\n<========\n\n" % self.to_str())
            ret_code = 0
        else:

            script_file, script_name = tempfile.mkstemp('.sh')
            script_file = os.fdopen(script_file, 'w+b')
            script_file.write(self.to_str() + '\n')
            script_file.close()
            
            args = ['sh', script_name]
            ret_code = subprocess.call(args)
            if ret_code != 0:
                raise BadReturnError
            os.remove(script_name)

        return ret_code

class AbstractExecManager(object):

    def __init__(self, verbose, noexec):
        self.noexec = noexec
        self.verbose = verbose

    def new_task(self, exec_node, ratio_unique=1.0, dep=None):
        raise NotImplementedException

    def add_dep(self, task, dep):
        task.add_dep(dep)

    def add_pre_cmd(self, job, allargs):
        job.add_pre_cmd(allargs)
 
    def queue_cmd(self, job, allargs):
        job.queue_cmd(allargs)

    def submit(self, job):
        raise NotImplementedException

    def add_positional_args(self, arg_list, constraint_dict, positional_args):
        """ Add positional args to a list of command arguments. """
        
        for arg_tuple in positional_args:
            arg_name = arg_tuple[0]
            position = arg_tuple[1]

            this_att_value = constraint_dict[arg_name]
            if position != -1:
                position += 1   # +1 because arg_list[0] is the actual command!
                arg_list.insert(position, this_att_value)
            else:
                arg_list.append(this_att_value)
                
        return arg_list


class SimpleExecManager(AbstractExecManager):

    def __init__(self, verbose=False, noexec=False):

        super(SimpleExecManager, self).__init__(verbose,noexec)
        self.job = SimpleJob()
        # Clear loaded modules inherited from parent
        self.add_pre_cmd(self.job,['module','purge'])

    def add_module_dep(self, module):
        self.add_pre_cmd(self.job, ['module', 'load', module])

    def add_module_deps(self, module_list):
        for module in module_list:
            self.add_module_dep(module)

    def add_environment_variables(self,environ_vars):
        for var in environ_vars.keys():
            self.add_pre_cmd(self.job,['export',"%s=%s" % (var,environ_vars[var])])

    def add_python_paths(self,python_paths):
        for path in python_paths:
            self.add_pre_cmd(self.job,['export','PYTHONPATH=$PYTHONPATH:%s' % path])

    def add_cmd(self, cmd, in_files, out_files,
                constraint_dict={}, kw_args=[], positional_args=[]):
        
        for ofile in out_files:
            self.job.outdirs.add(os.path.dirname(ofile))
        self._out_files = out_files

        cmdlist = cmd.split() 
        allargs = cmdlist + in_files + out_files

        final_args = self.add_positional_args(allargs, constraint_dict,
                                              positional_args)

        self.queue_cmd(self.job, final_args)

    def submit(self):
        """Creates a simple shell script with all the commands to be executed.
           Uses Popen to run the script.

           Popen isn't used directly for each command as otherwise the 
           "module load" commands have no effect as each command is run in its
           own subshell....
        """
        self.job.submit(noexec=self.noexec)


class BadReturnError(Exception):
    """ An error raised when the command being run by the scheduler doesn't return 0 """

    pass
