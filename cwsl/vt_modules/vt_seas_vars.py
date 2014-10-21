"""

Authors: Tim Bedin, Tim Erwin

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

This module wraps the CCT seas_vars_mod.sh script.

Part of the Model Analysis Service VisTrails plugin.

"""


import os

from vistrails.core.modules import vistrails_module
from vistrails.core.modules.basic_modules import String, List

from cwsl.configuration import configuration
from cwsl.core.module_loader import ModuleLoader
from cwsl.core.process_unit import ProcessUnit
from cwsl.core.constraint import Constraint
from cwsl.core.pattern_generator import PatternGenerator


class SeasVars(vistrails_module.Module):
    """
    Creates a seasonal timeseries with:
        annual, monthly, djf, mam, jja, son, ndjfma, mjjaso

        Requires: cdo, nco and cdat (if xml input)

    """

    # Define the module ports.
    true_false = ["['true', 'false']"]
    _input_ports = [('in_dataset', 'csiro.au.cwsl:VtDataSet',
                     {'labels': str(['Input Dataset'])}),
                    ('year_start', String),
                    ('year_end', String),
                    ('seas_agg', String),
                    ('added_constraints', List, True,
                     {'defaults': ['[]']})]

    _output_ports = [('out_dataset', 'csiro.au.cwsl:VtDataSet')]

    _execution_options = {'required_modules': ['cdo', 'cct', 'nco', 
                                               'python/2.7.5','python-cdat-lite/6.0rc2-py2.7.5']
                         }

    def __init__(self):

        super(SeasVars, self).__init__()

        tools_base_path = configuration.cwsl_ctools_path
        self.command = tools_base_path + '/aggregation/seas_vars_mod.sh'

        self.simulate = configuration.simulate_execution

    def compute(self):

        # Required input
        in_dataset = self.getInputFromPort("in_dataset")
        year_start = self.getInputFromPort("year_start")
        year_end = self.getInputFromPort("year_end")
        seas_agg = self.getInputFromPort("seas_agg")

        new_cons = set([Constraint('year_start', [year_start]),
                        Constraint('year_end', [year_end]),
                        Constraint('seas_agg', [seas_agg]),
                        Constraint('file_type', ['nc']),
                        Constraint('grid', ['native'])])

        # Optional added constraints.
        try:
            # Add extra constraints if necessary.
            added_constraints = self.getInputFromPort('added_constraints')
            cons_for_output = new_cons.intersection(added_constraints)
        except vistrails_module.ModuleError:
            cons_for_output = new_cons

        # Get the output pattern using the PatternGenerator object.
        # Gets the user infomation / authoritative path etc from the
        # user configuration.
        out_pattern = PatternGenerator('user', 'seasonal').pattern

        # Set up the output command for this module, adding extra options.
        #cons_for_output = set.union(cons_for_output, set(added_constraints))
        #if check_vars == "true":
        #    self.command += " -c "
        #if remove_times == "true":
        #    self.command += " -r "
        #self.command += " -a "
        command = self.command + ' tas tas ' + year_start + ' ' + year_end + ' ' + seas_agg

        # Execute the seas_vars process.
        this_process = ProcessUnit([in_dataset],
                                   out_pattern,
                                   command,
                                   cons_for_output)

        this_process.execute(simulate=self.simulate)
        process_output = this_process.file_creator

        self.setResult('out_dataset', process_output)

        # Unload the modules at the end.
        #self.module_loader.unload(self.required_modules)
