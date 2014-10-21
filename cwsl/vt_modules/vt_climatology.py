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

Part of the CWSLab Model Analysis Service VisTrails plugin.

"""

__version__ = '$Id$'

import os

from vistrails.core.modules import vistrails_module
from vistrails.core.modules import basic_modules

from cwsl.configuration import configuration
from cwsl.core.module_loader import ModuleLoader
from cwsl.core.constraint import Constraint
from cwsl.core.process_unit import ProcessUnit
from cwsl.core.pattern_generator import PatternGenerator


class Climatology(vistrails_module.Module):
    ''' This module creates climatogies from input seasonal files. '''

    # Define the module ports.
    true_false = ["['true', 'false']"]
    _input_ports = [('in_dataset', 'csiro.au.cwsl:VtDataSet',
                     {'labels': str(['Input Dataset'])}),
                    ('start_year', basic_modules.Integer,
                     {'labels': str(['Begin at this year'])}),
                    ('end_year', basic_modules.Integer,
                     {'labels': str(['End at this year'])}),
                    ('added_constraints', basic_modules.List, True,
                     {'defaults': ["[]"]})]

    _output_ports = [('out_dataset', 'csiro.au.cwsl:VtDataSet'),
                     ('out_constraints', basic_modules.String)]

    def __init__(self):

        super(Climatology, self).__init__()

        self.simulate = configuration.simulate_execution

        self.required_modules = ['cdo', 'cct_module', 'nco']
        self.module_loader = ModuleLoader()
        self.module_loader.load(self.required_modules)

        self.command = os.environ['CCT'] + '/manipulation/nc/cdo_climatology.sh'

    def compute(self):

        in_dataset = self.getInputFromPort("in_dataset")

        start_year = self.getInputFromPort("start_year")
        end_year = self.getInputFromPort("end_year")

        added_constraints = self.getInputFromPort("added_constraints")

        cons_for_output = set([Constraint('file_type', ['nc']),
                               Constraint('seas_agg', ['seasavg'])])

        # Add any extra constraints.
        cons_for_output = set.union(cons_for_output, set(added_constraints))

        out_pattern = PatternGenerator('user', 'seasonal_aggregate').pattern

        command = self.command + ' ' + start_year + ' ' + end_year

        # Execute the climatology process.
        this_process = ProcessUnit([in_dataset],
                                   out_pattern,
                                   command,
                                   cons_for_output)

        this_process.execute(simulate=self.simulate)
        process_output = this_process.file_creator

        self.setResult('out_dataset', process_output)
        self.setResult('out_constraints', str(process_output.constraints))
