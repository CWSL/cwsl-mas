"""

Creates a timeseries plot 

This module wraps the plot_timeseries.sh script found in git repository cwsl-ctools.

Part of the CWSLab VisTrails plugin.

Authors: Tim Bedin, Tim.Bedin@csiro.au
         Tim Erwin, Tim.Erwin@csiro.au

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

"""

import os

from vistrails.core.modules import vistrails_module
from vistrails.core.modules.basic_modules import String, List

from cwsl.configuration import configuration
from cwsl.core.process_unit import ProcessUnit
from cwsl.core.constraint import Constraint
from cwsl.core.pattern_generator import PatternGenerator


class PlotTimeSeries(vistrails_module.Module):
    """
    Plots a timeseries. 

    Required inputs: variable_name (varible name of data from input file)

    Other inputs:    infile (from dataset connector)
                     title (model name from dataset connector)

    Requires: python, cdat

    """

    # Define the module ports.
    _input_ports = [('in_dataset', 'csiro.au.cwsl:VtDataSet',
                     {'labels': str(['Input Dataset'])}),
                    ('variable_name', String),
                   ]

    _output_ports = [('out_dataset', 'csiro.au.cwsl:VtDataSet')]

    _execution_options = {'required_modules': ['python/2.7.5','python/2.7.5-matplotlib', 'python-cdat-lite/6.0rc2-py2.7.5']
                         }

    def __init__(self):

        super(PlotTimeSeries, self).__init__()

        self.command = '${CWSL_CTOOLS}/visualisation/plot_timeseries.py'

        # Get the output pattern using the PatternGenerator object.
        # Gets the user infomation / authoritative path etc from the
        # user configuration.
        self.out_pattern = PatternGenerator('user', 'default').pattern

    def compute(self):

        # Required input
        in_dataset = self.getInputFromPort("in_dataset")
        variable_name = self.getInputFromPort("variable_name")
        self.positional_args=[(variable_name,0,'raw'),('model',1)]

        new_cons = set([Constraint('variable', [variable_name]),
                        Constraint('suffix', ['png']),
                        ])

        # Optional added constraints.
        try:
            # Add extra constraints if necessary.
            added_constraints = self.getInputFromPort('added_constraints')
            cons_for_output = new_cons.intersection(added_constraints)
        except vistrails_module.ModuleError:
            cons_for_output = new_cons


        # Execute the seas_vars process.
        this_process = ProcessUnit([in_dataset],
                                   self.out_pattern,
                                   self.command,
                                   cons_for_output,
                                   execution_options=self._execution_options,
                                   positional_args=self.positional_args)

        try:
            this_process.execute(simulate=configuration.simulate_execution)
        except Exception, e:
            raise vistrails_module.ModuleError(self, e.output)

        process_output = this_process.file_creator

        self.setResult('out_dataset', process_output)
