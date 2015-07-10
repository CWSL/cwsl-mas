"""
Authors:  Craig Heady (craig.heady@csiro.au)

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

This module wraps a shell script that performs time aggregation: 
cwsl-ctools/aggregation/cdo_histogram.sh

Part of the CWSLab Model Analysis Service VisTrails plugin.

"""

from vistrails.core.modules import vistrails_module, basic_modules

from cwsl.configuration import configuration
from cwsl.core.constraint import Constraint
from cwsl.core.process_unit import ProcessUnit
from cwsl.core.pattern_generator import PatternGenerator


class Histogram(vistrails_module.Module):
    """Histogram.

    Wraps the cwsl-ctools/statistics/cdo_histogram.sh script.

    Inputs:
      in_dataset: Can consist of netCDF files
      
      method: Histogram method.
        - histcount (Histogram count: Number of elements in the bin range)
        - histsum   (Histogram sum: Sum of elements in the bin range)
        - histmean  (Histogram mean: Mean of elements in the bin range)
        - histfreq  (Histogram frequency: Relative frequency of elements in the bin range)
      
      bin_list: Comma seperated list of bin values.
        eg temperature   -inf,-40,-30,-20,-10,0,10,20,30,40,inf"
        eg precipitation 0,20,40,60,80,100,150,200,300,400,inf"
    
    Outputs:
      out_dataset: Consists of netCDF files
    
    """

    _input_ports = [('in_dataset', 'csiro.au.cwsl:VtDataSet', 
                     {'labels': str(['Input dataset'])}),
                    ('method', basic_modules.String, 
                     {'labels': str(['Hist Method'])}),
                    ('bin_list', basic_modules.String, 
                     {'labels': str(['Hist Bins'])}),
                   ]
                   
    _output_ports = [('out_dataset', 'csiro.au.cwsl:VtDataSet')]
    
    _execution_options = {'required_modules': ['cdo', 'python/2.7.5', 'python-cdat-lite/6.0rc2-py2.7.5']}

    command = '${CWSL_CTOOLS}/statistics/cdo_histogram.sh'

    def __init__(self):

        super(Histogram, self).__init__()
        self.out_pattern = PatternGenerator('user', 'default').pattern

    def compute(self):

        in_dataset = self.getInputFromPort('in_dataset')
        method = self.getInputFromPort('method')
        bin_list = self.getInputFromPort('bin_list')

        
        self.positional_args = [('%s,%s' %(method,bin_list), 0, 'raw'), ]
        self.keyword_args = {}

        if len(method.split(',')) > 1:
            agg_constraint = "".join(method.split(','))
        else:
            agg_constraint = method
        

        new_constraints_for_output = set([Constraint('timeagg_info', [agg_constraint]),
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

