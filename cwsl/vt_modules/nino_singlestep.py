# -*- coding: utf-8 -*-
"""

Authors:  Tim Bedin (Tim.Bedin@csiro.au)

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
monthly sea surface temperature file inputs.

Part of the CWSLab Model Analysis Service VisTrails plugin.

"""

import subprocess

from vistrails.core.modules import vistrails_module
from vistrails.core.modules import basic_modules

from cwsl.configuration import configuration
from cwsl.core.constraint import Constraint
from cwsl.core.process_unit import ProcessUnit
from cwsl.core.file_creator import FileCreator


class SingleStepNino34(vistrails_module.Module):
    ''' This module calculates the Niño 3.4 index from monthly time series


    of sea surface temperature data.

    This is done in a single step for use in the webservice.

    '''

    # Define the module ports.
    _input_ports = [('in_dataset', 'csiro.au.cwsl:VtDataSet')]
    
    _output_ports = [('out_dataset', 'csiro.au.cwsl:VtDataSet')]
    
    _execution_options = {'required_modules': ['cdo']}


    def __init__(self):

        super(SingleStepNino34, self).__init__()
        
        #Command Line Tool
        tools_base_path = configuration.cwsl_ctools_path
        self.command = '${CWSL_CTOOLS}/indices/nino34_onestep.sh'
        

    def compute(self):

        in_dataset = self.getInputFromPort("in_dataset")
        
        #Output file structure declaration 
        new_cons = set([in_dataset.get_constraint("model"),
                        in_dataset.get_constraint("experiment"),
                        Constraint('extra_info', ['nino34']),
                        Constraint('latsouth_info', ['5S']),
                        Constraint('latnorth_info', ['5N']),
                        Constraint('latagg_info', ['fldavg']),
                        Constraint('lonwest_info', ['190E']),
                        Constraint('loneast_info', ['240E']),
                        Constraint('lonagg_info', ['fldavg']),
                        Constraint('leveltop_info', ['surface']),
                        Constraint('levelbottom_info', ['surface']),
                        Constraint('anomaly_info', ['anom'])])

        output_pattern = FileCreator.default_pattern(new_cons, jobdir=True) + ".nc"

        # Execute the process.
        this_process = ProcessUnit([in_dataset],
                                   output_pattern,
                                   self.command,
                                   new_cons,
                                   execution_options=self._execution_options)
        
        try:
            this_process.execute(simulate=configuration.simulate_execution)
        except subprocess.CalledProcessError as e:
            raise vistrails_module.ModuleError(self, e.output)
        except Exception as e:
            raise vistrails_module.ModuleError(self, repr(e))

        process_output = this_process.file_creator
        
        self.setResult('out_dataset', process_output)
