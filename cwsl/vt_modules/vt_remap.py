"""
Authors:  Damien Irving (irving.damien@gmail.com)

Copyright 2015 CSIRO

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

This module wraps a shell script that remaps data to new horizontal grid: 
cwsl-ctools/utils/cdo_remap.sh

Part of the CWSLab Model Analysis Service VisTrails plugin.

"""

import subprocess
from vistrails.core.modules import vistrails_module, basic_modules

from cwsl.configuration import configuration
from cwsl.core.constraint import Constraint
from cwsl.core.process_unit import ProcessUnit
from cwsl.core.pattern_generator import PatternGenerator


class Remap(vistrails_module.Module):
    """This module remaps data to a new horizontal grid.

    It wraps the cwsl-ctools/utils/cdo_remap.sh script.

    """

    _input_ports = [('in_dataset', 'csiro.au.cwsl:VtDataSet'),
                    ('method', basic_modules.String),
                    ('grid', basic_modules.String),
                   ]

    _output_ports = [('out_dataset', 'csiro.au.cwsl:VtDataSet')]
    
    _execution_options = {'required_modules': ['cdo', 'python/2.7.5', 'python-cdat-lite/6.0rc2-py2.7.5']}

    command = '${CWSL_CTOOLS}/utils/cdo_remap.sh'

    def __init__(self):

        super(Remap, self).__init__()
        self.out_pattern = PatternGenerator('user', 'default').pattern

    def compute(self):

        in_dataset = self.getInputFromPort('in_dataset')
        method = self.getInputFromPort('method')
        grid = self.getInputFromPort('grid')

        self.positional_args = [(method, 0, 'raw'), (grid, 1, 'raw'), ]
        self.keyword_args = {}
        
        grid = grid.split('/')[-1]
        if len(grid.split('.')) > 1:  # i.e. a weights file as opposed to pre-defined grid
            grid_constraint = method+'-'+grid.split('.')[0]
        else:
            grid_constraint = method+'-'+grid

        new_constraints_for_output = set([Constraint('grid_info', [grid_constraint]),
                                          Constraint('suffix', ['nc']),
                                          ])
        
        this_process = ProcessUnit([in_dataset],
                                   self.out_pattern,
                                   self.command,
                                   new_constraints_for_output,
                                   execution_options=self._execution_options,
                                   positional_args=self.positional_args,
                                   cons_keywords=self.keyword_args)

        try:
            this_process.execute(simulate=configuration.simulate_execution)
        except Exception as e:
            raise vistrails_module.ModuleError(self, repr(e))
            
        process_output = this_process.file_creator

        self.setResult('out_dataset', process_output)

