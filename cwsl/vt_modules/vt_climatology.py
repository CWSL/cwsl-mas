"""

Authors:  Tim Bedin (Tim.Bedin@csiro.au)
          Ricardo Pascual (Ricardo.Pascual@csiro.au)
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

This module wraps the CDO command to create a climatology from input files.
This is an experiment to test out the BaseModule inheritance class.

Part of the CWSLab Model Analysis Service VisTrails plugin.

"""

import subprocess

from vistrails.core.modules import vistrails_module, basic_modules

from cwsl.configuration import configuration
from cwsl.core.constraint import Constraint
from cwsl.core.process_unit import ProcessUnit
from cwsl.core.pattern_generator import PatternGenerator


class Climatology(vistrails_module.Module):

    ''' This module takes in a start and end year and performs time averaging on the input DataSet. '''

    # Define any extra module ports.
    _input_ports = [('in_dataset', 'csiro.au.cwsl:VtDataSet'),
                    ('start_year', basic_modules.Integer,
                     {'labels': str(['Begin at this year'])}),
                    ('end_year', basic_modules.Integer,
                     {'labels': str(['End at this year'])})]

    _output_ports = [('out_dataset', 'csiro.au.cwsl:VtDataSet')]
    
    _execution_options = {'required_modules': ['cdo', 'nco']}

    _module_setup = {'command': '${CWSL_CTOOLS}/aggregation/cdo_climatology.sh',
                     'user_or_authoritative': 'user',
                     'data_type': 'seasonal_aggregate'}
    
    _positional_args = [('year_start', 0), ('year_end', 1)]

    def compute(self):

        self.out_pattern = PatternGenerator(self._module_setup['user_or_authoritative'],
                                            self._module_setup['data_type']).pattern
        self.command = self._module_setup['command']

        in_dataset = self.getInputFromPort('in_dataset')
        start_year = self.getInputFromPort('start_year')
        end_year = self.getInputFromPort('end_year')

        cons_for_output = set([Constraint('agg_type', ['clim']),
                               Constraint('year_start', [start_year]),
                               Constraint('year_end', [end_year])])
        
        # Execute the climatology process.
        this_process = ProcessUnit([in_dataset],
                                   self.out_pattern,
                                   self.command,
                                   cons_for_output,
                                   execution_options=self._execution_options,
                                   positional_args=self._positional_args)

        try:
            this_process.execute(simulate=configuration.simulate_execution)
        except subprocess.CalledProcessError, e:
            raise vistrails_module.ModuleError(self, e.output)
            
        process_output = this_process.file_creator

        self.setResult('out_dataset', process_output)
