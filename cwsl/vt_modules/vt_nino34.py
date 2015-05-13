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

This module calculates a timeseries of the Niño3.4 index from
a monthly sea surface temperature input.

Part of the CWSLab Model Analysis Service VisTrails plugin.

"""

from vistrails.core.modules import vistrails_module
from vistrails.core.modules import basic_modules

from cwsl.configuration import configuration
from cwsl.core.constraint import Constraint
from cwsl.core.process_unit import ProcessUnit
from cwsl.core.pattern_generator import PatternGenerator


class IndicesNino34(vistrails_module.Module):
    ''' This module calculates the Niño 3.4 index from an input monthly time series


    of sea surface temperature data. It uses a 30-year rolling climatology to calculate
    the surface temperature anomaly.
    
    This wraps the cwsl-ctools/indices/nino_34.sh script.

    '''

    # Define the module ports.
    _input_ports = [('in_dataset', 'csiro.au.cwsl:VtDataSet'),
                    ('added_constraints', basic_modules.List, True,
                     {'defaults': ["[]"]})]
    
    _output_ports = [('out_dataset', 'csiro.au.cwsl:VtDataSet'),
                     ('out_constraints', basic_modules.String, True)]
    
    _execution_options = {'required_modules': ['cdo', 'nco', 
                                               'python/2.7.5','python-cdat-lite/6.0rc2-py2.7.5']}

    def __init__(self):

        super(IndicesNino34, self).__init__()
        
        #Command Line Tool
        tools_base_path = configuration.cwsl_ctools_path
        self.command = '${CWSL_CTOOLS}/indices/nino34.sh'
        #Output file structure declaration 
        self.out_pattern = PatternGenerator('user', 'default').pattern
        
        # Set up the output command for this module, adding extra options.
        self.positional_args = [('year_start', 2), ('year_end', 3)]
        
    def compute(self):

        # Required input
        in_dataset = self.getInputFromPort("in_dataset")

        new_cons = set([Constraint('extra_info', ['nino34']),
                        Constraint('southlat_info', ['5S']),
                        Constraint('northlat_info', ['5N']),
                        Constraint('latagg_info', ['fldavg']),
                        Constraint('westlon_info', ['190E']),
                        Constraint('eastlon_info', ['240E']),
                        Constraint('lonagg_info', ['fldavg']),
                        Constraint('toplevel_info', ['surface']),
                        Constraint('bottomlevel_info', ['surface']),
                        Constraint('anomaly_info', ['anom-wrt-unknown']),
                       ])
        
        cons_for_output = new_cons
        
        # Execute the process.
        this_process = ProcessUnit([in_dataset],
                                   self.out_pattern,
                                   self.command,
                                   cons_for_output,
                                   positional_args=self.positional_args,
                                   execution_options=self._execution_options)

        try:
            this_process.execute(simulate=configuration.simulate_execution)
        except Exception, e:
            raise vistrails_module.ModuleError(self, e.output)

        process_output = this_process.file_creator

        self.setResult('out_dataset', process_output)
