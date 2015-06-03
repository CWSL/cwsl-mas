"""

Wrapper for cdscan (CDAT) fo create a virtual aggregation catalogue file.

Part of the CWSLab VisTrails plugin.

Authors: Tim Bedin, Tim.Bedin@csiro.au
         Tim Erwin, Tim.Erwin@csiro.au

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

import os
import subprocess

from vistrails.core.modules import vistrails_module
from vistrails.core.modules.basic_modules import String, List

from cwsl.configuration import configuration
from cwsl.core.constraint import Constraint
from cwsl.core.process_unit import ProcessUnit
from cwsl.core.pattern_generator import PatternGenerator


class CDScan(vistrails_module.Module):
    """
    Create a single cdms catalogue file for the entire time period for a set of files.

    Inputs:    dataset

    Outputs:   dataset
    Filepath:  <user_basepath>/%mip%/%product%/%institute%/%model%/%experiment%/%frequency%/%realm%/%variable%/%ensemble%/
    Filename:  %variable%_%mip_table%_%model%_%experiment%_%ensemble%_cdat-lite-6-0rc2-py2.7.%suffix%

    Requires: python, cdat

    """


    # Define the module ports.
    _input_ports = [('in_dataset', 'csiro.au.cwsl:VtDataSet')]

    _output_ports = [('out_dataset', 'csiro.au.cwsl:VtDataSet')]

    _execution_options = {'required_modules': ['cdo', 'nco',
                                               'python/2.7.5','python-cdat-lite/6.0rc2-py2.7.5']}

    def __init__(self):

        super(CDScan, self).__init__()

        self.command = '${CWSL_CTOOLS}/aggregation/version_safe_cdscan.py'
        self.out_pattern = PatternGenerator('user', 'cdat_lite_catalogue').pattern

    def compute(self):

        in_dataset = self.getInputFromPort('in_dataset')

        # Change the file_type constraint from nc to xml
        cons_for_output = set([Constraint('suffix', ['xml'])])

        # Execute the cdscan
        this_process = ProcessUnit([in_dataset],
                                   self.out_pattern,
                                   self.command,
                                   cons_for_output,
                                   execution_options=self._execution_options)

        try:
            this_process.execute(simulate=configuration.simulate_execution)
        except subprocess.CalledProcessError, e:
            raise vistrails_module.ModuleError(self, e.output)

        process_output = this_process.file_creator

        self.setResult('out_dataset', process_output)
