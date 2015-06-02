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

This module wraps a shell script that calculates the temporal correlation: 
cwsl-ctools/statistics/cdo_timcor.sh

Part of the CWSLab Model Analysis Service VisTrails plugin.

"""

from vistrails.core.modules import vistrails_module, basic_modules

from cwsl.configuration import configuration
from cwsl.core.constraint import Constraint
from cwsl.core.process_unit import ProcessUnit
from cwsl.core.pattern_generator import PatternGenerator


class TemporalCorrelation(vistrails_module.Module):
    """Temporal correlation.

    Wraps the cwsl-ctools/statistics/cdo_timcor.sh script.

    """

    _input_ports = [('in_dataset1', 'csiro.au.cwsl:VtDataSet',
                     {'labels': str(['Input dataset 1'])}),
                    ('in_dataset2', 'csiro.au.cwsl:VtDataSet',
                     {'labels': str(['Input dataset 2'])}),
                    ('merge_constraints', basic_modules.String,
                     {'labels': str(['Constraints to merge']),
                      'defaults': str([''])})]

    _output_ports = [('out_dataset', 'csiro.au.cwsl:VtDataSet')]
    
    _execution_options = {'required_modules': ['cdo', 'python/2.7.5', 'python-cdat-lite/6.0rc2-py2.7.5']}

    command = '${CWSL_CTOOLS}/statistics/cdo_timcor.sh'

    def __init__(self):

        super(TemporalCorrelation, self).__init__()
        self.out_pattern = PatternGenerator('user', 'default').pattern

    def compute(self):

        in_dataset1 = self.getInputFromPort('in_dataset1')
        in_dataset2 = self.getInputFromPort('in_dataset2')

        self.positional_args = []
        self.keyword_args = {}

        new_constraints_for_output = set([Constraint('extra_info', ['timcor']),
                                          Constraint('suffix', ['nc']),
                                          ])

        merge_val =  self.getInputFromPort('merge_constraints')
        if merge_val:
            extra_merge = [cons_name.strip() for cons_name in merge_val.split(',')]
        else:
            extra_merge = []
        
        this_process = ProcessUnit([in_dataset1, in_dataset2],
                                   self.out_pattern,
                                   self.command,
                                   new_constraints_for_output,
                                   execution_options=self._execution_options,
                                   positional_args=self.positional_args,
                                   cons_keywords=self.keyword_args,
                                   merge_output=extra_merge)

        try:
            this_process.execute(simulate=configuration.simulate_execution)
        except Exception as e:
            raise vistrails_module.ModuleError(self, repr(e))
            
        process_output = this_process.file_creator

        self.setResult('out_dataset', process_output)

