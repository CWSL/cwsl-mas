"""

Authors: Tim Bedin

Copyright 2015 CSIRO, Australian Government Bureau of Meteorology

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Module to pull a timeseries from a netCDF file in JSON format.

"""

from vistrails.core.modules import vistrails_module
from vistrails.core.modules.basic_modules import String, List

from cwsl.configuration import configuration
from cwsl.core.process_unit import ProcessUnit
from cwsl.core.file_creator import FileCreator
from cwsl.core.constraint import Constraint


class ExtractTimeseries(vistrails_module.Module):
    ''' This module pulls a JSON timeseries from a netCDF file.'''

    # Define the module ports.
    _input_ports = [('in_dataset', 'csiro.au.cwsl:VtDataSet'),
                    ('x_value', String),
                    ('y_value', String)]
    _output_ports = [('out_dataset', 'csiro.au.cwsl:VtDataSet')]

    def __init__(self):

        super(ExtractTimeseries, self).__init__()
        
        self._required_modules = {'required_modules': ['python']}

    def compute(self):

        in_dataset = self.getInputFromPort('in_dataset')
        x_value = self.getInputFromPort('x_value')
        y_value = self.getInputFromPort('y_value')

        command = "echo"
        
        # The data is written out to the default location.
        output_pattern = FileCreator.default_pattern(in_dataset.constraints, temp=True) + ".json"
        this_process = ProcessUnit([in_dataset], output_pattern,
                                   command, in_dataset.constraints,
                                   execution_options=self._required_modules,
                                   positional_args=[(x_value, 1, 'raw'), (y_value, 2, 'raw')])

        this_process.execute(simulate=configuration.simulate_execution)
        process_output = this_process.file_creator

        self.setResult('out_dataset', process_output)
