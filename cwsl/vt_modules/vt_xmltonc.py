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

    """

    # Define the module ports.
    _input_ports = [('in_dataset', 'csiro.au.cwsl:VtDataSet',
                     {'labels': str(['Input Dataset'])}),
                    ('startdate', basic_modules.String,
                     {'labels': str(['Start Date (YYYY-MM-DD)']),'optional': True}),
                    ('enddate', basic_modules.String,
                     {'labels': str(['End Date (YYYY-MM-DD)']), 'optional': True}),
                    ('westlon', basic_modules.String,
                     {'labels': str(['Western longitude']), 'optional': True}),
                    ('eastlon', basic_modules.String,
                     {'labels': str(['Eastern longitude']), 'optional': True}),
                    ('southlat', basic_modules.String,
                     {'labels': str(['Southern Latitude']), 'optional': True}),
                    ('northlat', basic_modules.String,
                     {'labels': str(['Northern Latitude']), 'optional': True}),
                    ('bottomlevel', basic_modules.String,
                     {'labels': str(['Bottom level']), 'optional': True}),
                    ('toplevel', basic_modules.String,
                     {'labels': str(['Top level']), 'optional': True})]

    _output_ports = [('out_dataset', 'csiro.au.cwsl:VtDataSet')]

    _execution_options = {'required_modules': ['cdo', 'python/2.7.5','python-cdat-lite/6.0rc2-py2.7.5']}


    def __init__(self):

        super(XmlToNc, self).__init__()

        #Command Line Tool
        self.command = '${CWSL_CTOOLS}/utils/xml_to_nc.py'

        # Output file structure declaration
        self.out_pattern = PatternGenerator('user', 'default').pattern

    def compute(self):

        # Required input
        in_dataset = self.getInputFromPort("in_dataset")

        # Set up the output command for this module, adding extra options.
        positional_args = [('variable', 0)]

        port_names = ["startdate", "enddate", "westlon",
                      "eastlon", "southlat", "northlat",
                      "bottomlevel", "toplevel"]
        port_vals = {}
        for name in port_names:
            try:
                port_vals[name+"_info"] = self.getInputFromPort(name)
            except vistrails_module.ModuleError as e:
                port_vals[name+"_info"] = None

        # Add the --time_bounds argument as positional, because it is of
        # list form (--option ARG ARG)
        # This means that we will now have the positional arguments on form
        # ./script variable infile outfile - other arguments start at 3
        arg_number = 3

        cons_for_output = set([Constraint('suffix', ['nc'])])

        if port_vals["startdate_info"] and port_vals["enddate_info"]:
            positional_args += [('--time_bounds', arg_number, 'raw'),
                                ('startdate_info', arg_number+1),
                                ('enddate_info', arg_number+2)]
            arg_number += 3
            cons_for_output |= set([Constraint('startdate_info', [port_vals["startdate_info"]]),
                                    Constraint('enddate_info', [port_vals["enddate_info"]])])

        if port_vals["eastlon_info"] and port_vals["westlon_info"]:
            positional_args += [('--lon_bounds', arg_number, 'raw'),
                                ('westlon_info', arg_number+1),
                                ('eastlon_info', arg_number+2)]
            arg_number += 3
            cons_for_output |= set([Constraint('westlon_info', [port_vals["westlon_info"]]),
                                    Constraint('eastlon_info', [port_vals["eastlon_info"]])])

        if port_vals["southlat_info"] and port_vals["northlat_info"]:
            positional_args += [('--lat_bounds', arg_number, 'raw'),
                                ('southlat_info', arg_number+1),
                                ('northlat_info', arg_number+2)]
            arg_number += 3
            cons_for_output |= set([Constraint('southlat_info', [port_vals["southlat_info"]]),
                                    Constraint('northlat_info', [port_vals["northlat_info"]])])

        if port_vals["bottomlevel_info"] and port_vals["toplevel_info"]:
            positional_args += [('--level_bounds', arg_number, 'raw'),
                                ('bottomlevel_info', arg_number+1),
                                ('toplevel_info', arg_number+2)]
            arg_number += 3
            cons_for_output |= set([Constraint('bottomlevel_info', [port_vals["bottomlevel_info"]]),
                                    Constraint('toplevel_info', [port_vals["toplevel_info"]])])

        # Execute the xml_to_nc process.
        this_process = ProcessUnit([in_dataset],
                                   self.out_pattern,
                                   self.command,
                                   cons_for_output,
                                   positional_args=positional_args,
                                   execution_options=self._execution_options)

        try:
            this_process.execute(simulate=configuration.simulate_execution)
        except Exception as e:
            raise vistrails_module.ModuleError(self, repr(e))

        process_output = this_process.file_creator

        self.setResult('out_dataset', process_output)
