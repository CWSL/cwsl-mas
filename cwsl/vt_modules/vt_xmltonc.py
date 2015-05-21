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

This module converts a CDAT cdml (xml) catalogue to a single netCDF file by
using the xml_to_nc.py script from the cwsl-ctools git repository.

Part of the CWSLab Model Analysis Service VisTrails plugin.

"""

from vistrails.core.modules import vistrails_module
from vistrails.core.modules import basic_modules

from cwsl.configuration import configuration
from cwsl.core.constraint import Constraint
from cwsl.core.process_unit import ProcessUnit
from cwsl.core.pattern_generator import PatternGenerator


class XmlToNc(vistrails_module.Module):
    """
    This module selects a time period from a single netCDF file or cdml catalogue file

    Requires: date_start - Start date of time selection, format YYYY-MM-DD
              date_end   - End date of time selection, format YYYY-MM-DD

    """

    # Define the module ports.
    _input_ports = [('in_dataset', 'csiro.au.cwsl:VtDataSet',
                     {'labels': str(['Input Dataset'])}),
                    ('start_date', basic_modules.Integer,
                     {'labels': str(['Start Date (YYYY-MM-DD)'])}),
                    ('end_date', basic_modules.Integer,
                     {'labels': str(['End Date (YYYY-MM-DD)'])}),
                    ('added_constraints', basic_modules.List, True,
                     {'defaults': ["[]"]})]

    _output_ports = [('out_dataset', 'csiro.au.cwsl:VtDataSet'),
                     ('out_constraints', basic_modules.String, True)]

    _execution_options = {'required_modules': ['cdo', 'python/2.7.5','python-cdat-lite/6.0rc2-py2.7.5']}


    def __init__(self):

        super(XmlToNc, self).__init__()

        #Command Line Tool
        tools_base_path = configuration.cwsl_ctools_path
        self.command = '${CWSL_CTOOLS}/utils/xml_to_nc.py'
        #Output file structure declaration ??
        self.out_pattern = PatternGenerator('user', 'default').pattern

        # Set up the output command for this module, adding extra options.
        self.positional_args = [('variable', 0)]
        # This means that we will now have the positional arguments on form
        # ./script variable infile outfile.

        # Add the --time_bounds argument as positional, because it is of
        # list form (--option ARG ARG)
        self.positional_args += [('--time_bounds', 3, 'raw'),
                                 ('startdate_info', 4),
                                 ('enddate_info', 5),]

        self.keyword_args = {}

    def compute(self):

        # Required input
        in_dataset = self.getInputFromPort("in_dataset")
        date_start = self.getInputFromPort("start_date")
        date_end = self.getInputFromPort("end_date")

        new_cons = set([Constraint('startdate_info', [date_start]),
                        Constraint('enddate_info', [date_end]),
                        Constraint('suffix', ['nc']),])

        cons_for_output = new_cons

        # Execute the xml_to_nc process.
        this_process = ProcessUnit([in_dataset],
                                   self.out_pattern,
                                   self.command,
                                   cons_for_output,
                                   positional_args=self.positional_args,
                                   cons_keywords=self.keyword_args,
                                   execution_options=self._execution_options)

        try:
            this_process.execute(simulate=configuration.simulate_execution)
        except Exception as e:
            raise vistrails_module.ModuleError(self, repr(e))

        process_output = this_process.file_creator

        self.setResult('out_dataset', process_output)
