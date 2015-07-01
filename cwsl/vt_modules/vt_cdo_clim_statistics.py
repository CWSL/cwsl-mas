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
cwsl-ctools/statistics/cdo_clim_statistics.sh

Part of the CWSLab Model Analysis Service VisTrails plugin.

"""

from vistrails.core.modules import vistrails_module, basic_modules

from cwsl.configuration import configuration
from cwsl.core.constraint import Constraint
from cwsl.core.process_unit import ProcessUnit
from cwsl.core.pattern_generator import PatternGenerator


class ClimStatistics(vistrails_module.Module):
    """ClimStatistics.

    Wraps the cwsl-ctools/statistics/cdo_clim_statistics.sh script.

    Inputs:
      in_dataset: Can consist of netCDF files
      
      method: Statistical anlaysis method.min, max, sum, mean,avg,var,std.
        - min  (min: monthly, seasonal and annual Minimum)
        - max  (sum: monthly, seasonal and annual Maximum)
        - sum  (sum: monthly, seasonal and annual Sum)
        - mean (mean:monthly, seasonal and annual Mean)
        - avg  (avg: monthly, seasonal and annual Average)
        - var  (var: monthly, seasonal and annual Variance)
        - std  (std: monthly, seasonal and annual Standard Deviation)
    
    Outputs:
      out_dataset_mon: Consists of netCDF files
      out_dataset_seas:Consists of netCDF files
      out_dataset_ann: Consists of netCDF files   
    """

    _input_ports = [('in_dataset', 'csiro.au.cwsl:VtDataSet', 
                     {'labels': str(['Input dataset'])}),
                    ('method', basic_modules.String, 
                     {'labels': str(['Statistic'])}),
                   ]
                   
    _output_ports = [('out_dataset_mon', 'csiro.au.cwsl:VtDataSet'),
                     ('out_dataset_seas', 'csiro.au.cwsl:VtDataSet'),
                     ('out_dataset_ann', 'csiro.au.cwsl:VtDataSet'),]
    
    _execution_options = {'required_modules': ['cdo/1.6.4', 'python/2.7.5', 'python-cdat-lite/6.0rc2-py2.7.5']}

    command = '${CWSL_CTOOLS}/aggregation/cdo_time_agg.sh'
    
    
   
    #output_data = {'mon':'','seas':'','ann':''}
    
    def __init__(self):

        super(ClimStatistics, self).__init__()
        self.out_pattern = PatternGenerator('user', 'default').pattern

    def compute(self):

        in_dataset = self.getInputFromPort('in_dataset')
        method = self.getInputFromPort('method')

        seas_list = {'mon':'ymon','seas':'yseas','ann':'tim'}
        
        ### loop over seas_list to generate all 3 season files ###
        for seas in seas_list.keys():
            self.positional_args = [('%s%s' %(seas_list[seas],method), 0, 'raw'), ]
            self.keyword_args = {}

            if len(method.split(',')) > 1:
                agg_constraint = "".join(method.split(','))
            else:
                agg_constraint = method
        
            new_constraints_for_output = set([Constraint('timeagg_info', ['%s%s'%(seas_list[seas],method)],),
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

            self.setResult('out_dataset_%s' %seas, process_output)

