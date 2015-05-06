# -*- coding: utf-8 -*-
"""

Authors:  Tim Bedin (Tim.Bedin@csiro.au)
          Tim Erwin (Tim.Erwin@csiro.au)

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

This module calculates a time slice change by subtracting
files from two datasets.

Part of the CWSLab Model Analysis Service VisTrails plugin.

"""

from vistrails.core.modules import vistrails_module
from vistrails.core.modules import basic_modules

from cwsl.configuration import configuration
from cwsl.core.constraint import Constraint
from cwsl.core.process_unit import ProcessUnit
from cwsl.core.pattern_generator import PatternGenerator


class TimesliceChange(vistrails_module.Module):
    ''' This module calculates the change between a future and
    baseline dataset.

    It wraps the cct cdo_perc_change and cdo_abs_change scripts.
    
    '''

    # Define the module ports.
    _input_ports = [('future_dataset', 'csiro.au.cwsl:VtDataSet'),
                    ('baseline_dataset', 'csiro.au.cwsl:VtDataSet')]
    
    _output_ports = [('out_dataset', 'csiro.au.cwsl:VtDataSet')]
    
    _execution_options = {'required_modules': ['cdo', 'cct', 'nco']}
                          
    def __init__(self):

        super(TimesliceChange, self).__init__()
        
        # Command Line Tool
        tools_base_path = configuration.cwsl_ctools_path
        self.command = 'echo ${CWSL_CTOOLS}/change_script_path'
        # Output file structure declaration 
        self.out_pattern = PatternGenerator('user', 'timeslice_change').pattern
        
    def compute(self):

        # Required input
        future_dataset = self.getInputFromPort("future_dataset")
        baseline_dataset = self.getInputFromPort("baseline_dataset")

        # Execute the process.
        new_constraints = [Constraint('change_type', ['abs-change'])]
        this_process = ProcessUnit([future_dataset, baseline_dataset],
                                   self.out_pattern,
                                   self.command,
                                   extra_constraints=new_constraints,
                                   execution_options=self._execution_options,
                                   map_dict={'fut_start': ('year_start', 0),
                                             'fut_end': ('year_end', 0),
                                             'hist_start': ('year_start', 1),
                                             'hist_end': ('year_end', 1)})

        try:
            this_process.execute(simulate=configuration.simulate_execution)
        except Exception as e:
            raise vistrails_module.ModuleError(self, e.output)

        process_output = this_process.file_creator
        self.setResult('out_dataset', process_output)
