# -*- coding: utf-8 -
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

import abc
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
                 set -e

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
            log.debug("Would run script:\n\n========>\n%s\n<========\n\n" % self.to_str())
        else:
            script_file, script_name = tempfile.mkstemp('.sh')
            script_file = os.fdopen(script_file, 'w+b')
            script_file.write(self.to_str() + '\n')
            script_file.close()
            
            args = ['sh', script_name]
            
            try:
                output = subprocess.check_output(args, stderr=subprocess.STDOUT)
            except subprocess.CalledProcessError, e:
                output = e.output
                raise
            finally:
                # For now, print output to console as well.
                print(output)
                os.remove(script_name)

class AbstractExecManager(object):

    __metaclass__ = abc.ABCMeta

    def __init__(self, verbose, noexec):
        self.noexec = noexec
        self.verbose = verbose

    def add_dep(self, task, dep):
        task.add_dep(dep)

    def add_pre_cmd(self, job, allargs):
        job.add_pre_cmd(allargs)
 
    def queue_cmd(self, job, allargs):
        job.queue_cmd(allargs)

    @abc.abstractmethod
    def new_task(self, exec_node, ratio_unique=1.0, dep=None):
        raise NotImplementedException

    @abc.abstractmethod
    def submit(self, job):
        raise NotImplementedException


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

    def add_cmd(self, cmd_list, out_files, annotation=None):
        self._out_files = out_files
        for ofile in out_files:
            self.job.outdirs.add(os.path.dirname(ofile))

        self.queue_cmd(self.job, cmd_list)
        
        # If there is an annotation, add a second job that annotates the outfile.
        if annotation:
            self.add_annotation(annotation, out_files)
            
    def add_annotation(self, annotation, out_files):
        """ Annotate the vistrails_history metadata tag with an annotation string."""
        self.add_module_deps(['nco'])
        att_desc = 'vistrails_history,global,a,c,"' + annotation + '"'
        for out_file in out_files:
            if os.path.splitext(out_file)[1] in ['.nc', '.NC']:
                annotate_list = ['ncatted', '-O', '-a', att_desc, out_file]
                self.queue_cmd(self.job, annotate_list)
            else:
                log.warning("Not annotating file '%s' - not NetCDF" % out_file)
            
    def submit(self):
        """Creates a simple shell script with all the commands to be executed.
           Uses Popen to run the script.

           Popen isn't used directly for each command as otherwise the 
           "module load" commands have no effect as each command is run in its
           own subshell....


        """
        self.job.submit(noexec=self.noexec)

    def add_dep(self, task, dep):
        raise NotImplementedException

    def new_task(self, exec_node, ratio_unique=1.0, dep=None):
        raise NotImplementedException
