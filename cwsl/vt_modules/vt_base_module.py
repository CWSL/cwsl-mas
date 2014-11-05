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

This is a base module for developers to use to create their own modules.

Part of the CWSLab Model Analysis Service VisTrails plugin.

"""

import abc

from vistrails.core.modules import vistrails_module
from vistrails.core.modules import basic_modules

from cwsl.configuration import configuration
from cwsl.core.constraint import Constraint
from cwsl.core.process_unit import ProcessUnit
from cwsl.core.pattern_generator import PatternGenerator


class BaseModule(object):
    ''' An abstract class to build your own VisTrails modules. '''

    __metaclass__ = abc.ABCMeta
    
    # Define the module ports.
    _input_ports = [('in_dataset', 'csiro.au.cwsl:VtDataSet'),
                    ('added_constraints', basic_modules.List, True,
                     {'defaults': ["[]"]})]
    
    _output_ports = [('out_dataset', 'csiro.au.cwsl:VtDataSet')]
    
    _execution_options = {'required_modules': []}

    _module_setup = {'command': '',
                     'user_or_authoritative': '',
                     'data_type': ''}
    _positional_args = []
    _added_cons = set([])
    

    def __init__(self):
        
        super(BaseModule, self).__init__()
        
        self.command = self._module_setup['command']
        # Output file structure declaration 
        self.out_pattern = PatternGenerator(self._module_setup['user_or_authoritative'],
                                            self._module_setup['data_type']).pattern
        
        self.positional_args = self._positional_args
        self.execution_options = self._execution_options

        
    def compute(self):


        # Required input
        in_dataset = self.getInputFromPort("in_dataset")
        
        new_cons = self._added_cons
        
        # Execute the process.
        this_process = ProcessUnit([in_dataset],
                                   self.out_pattern,
                                   self.command,
                                   cons_for_output,
                                   positional_args=self.positional_args,
                                   execution_options=self.execution_options)

        try:
            this_process.execute(simulate=configuration.simulate_execution)
        except Exception, e:
            raise vistrails_module.ModuleError(self, e.output)

        process_output = this_process.file_creator

        self.setResult('out_dataset', process_output)
