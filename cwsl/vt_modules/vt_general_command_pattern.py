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


Module containing a VT module that is very open.

DataSet of some kind and a Pipeline-style pattern.

"""

from vistrails.core.modules import vistrails_module
from vistrails.core.modules.basic_modules import String, List

from cwsl.configuration import configuration
from cwsl.core.process_unit import ProcessUnit
from cwsl.core.file_creator import FileCreator
from cwsl.core.constraint import Constraint


class GeneralCommandPattern(vistrails_module.Module):
    '''
    This module gives users freedom to run what ever command they want
    on some data.
    '''

        # Define the module ports.
    _input_ports = [('in_dataset', 'csiro.au.cwsl:VtDataSet'),
                    ('added_constraints', List, True,{'defaults': ['[]']},),
                    ('command', String),
                    ('output_pattern', String)]

    _output_ports = [('out_dataset', 'csiro.au.cwsl:VtDataSet'),
                     ('out_constraints', String)]

    def __init__(self):

        super(GeneralCommandPattern, self).__init__()
        self.simulate = configuration.simulate_execution

        self.required_modules = ['python', 'cdo', 'nco']

    def compute(self):

        in_dataset = self.getInputFromPort('in_dataset')
        try:
            # Add extra constraints if necessary.
            added_constraints = self.getInputFromPort('added_constraints')
        except vistrails_module.ModuleError:
            added_constraints = None


        command = self.getInputFromPort("command")
        output_pattern = self.getInputFromPort("output_pattern")

        cons_for_output = set()
        if added_constraints:
            cons_for_output = set.union(cons_for_output,
                                        set(added_constraints))

        # Do the stuff.
        this_process = ProcessUnit([in_dataset],
                                   self.output_pattern,
                                   command,
                                   cons_for_output)

        this_process.execute(simulate=configuration.simulate_execution)
        process_output = this_process.file_creator

        self.setResult('out_dataset', process_output)
        self.setResult('out_constraints', str(process_output.constraints))

        # Unload the modules at the end.
        self.module_loader.unload(self.required_modules)
