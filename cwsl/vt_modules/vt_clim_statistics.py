"""

Authors:  Tim Bedin (Tim.Bedin@csiro.au)
          Tim Erwin (Tim.Erwin@csiro.au)
          Damien Irving (irving.damien@gmail.com)
          Craig Heady (craig.heady@csiro.au)

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


def longitude_label(lon):
    """Create a longitude label ending with E.
    
    Input longitude can be a string or float and in
      -135, 135W, 225 or 225E format.

    """

    lon = str(lon).upper()
    
    if 'W' in lon:
        deg_east = 360 - float(lon[:-1]) 
    elif 'E' in lon:
        deg_east = float(lon[:-1])
    elif float(lon) < 0.0:
        deg_east = 360 + float(lon)
    else: 
        deg_east = float(lon)
    
    assert 0 <= deg_east <= 360, "Longitude must lie between 0-360E"
    
    return str(deg_east)+'E'


def latitude_label(lat):
    """Create a latitude label ending with S or N.
    
    Input latitude can be a string or float and in
      -55 or 55S format.

    """

    if 'S' in str(lat).upper() or 'N' in str(lat).upper():
        label = str(lat).upper()
    elif float(lat) >= 0.0:
        label = str(lat) + 'N'
    else: 
        label = str(lat)[1:] + 'S'
        
    return label


class ClimStats(vistrails_module.Module):
    """Perform climatolgical statistics on a dataset allowing subsetting of longitude, latitude, time and/or level axis.

    Wraps the cwsl-ctools/utils/xml_to_nc.py script.

    All inputs (besides in_dataset and stat) are optional (i.e. they can be left blank).

    If an optional input is provided, so must its pair (e.g. if you enter a timestart
      you must also enter a timeend). 

    """

    # Define the module ports.
    _input_ports = [('in_dataset', 'csiro.au.cwsl:VtDataSet',
                     {'labels': str(['Input dataset'])}),
                    ('stat', 'csiro.au.cwsl:VtDataSet',
                     {'labels': str(['Statistic'])}),
                    ('timestart', basic_modules.String,
                     {'labels': str(['Start date (YYYY-MM-DD)']),'optional': True}),
                    ('timeend', basic_modules.String,
                     {'labels': str(['End date (YYYY-MM-DD)']), 'optional': True}),
                    ('lonwest', basic_modules.String,
                     {'labels': str(['Western longitude (0-360E)']), 'optional': True}),
                    ('loneast', basic_modules.String,
                     {'labels': str(['Eastern longitude (0-360E)']), 'optional': True}),
                    ('latsouth', basic_modules.String,
                     {'labels': str(['Southern latitude']), 'optional': True}),
                    ('latnorth', basic_modules.String,
                     {'labels': str(['Northern latitude']), 'optional': True}),
                    ('levelbottom', basic_modules.String,
                     {'labels': str(['Bottom level']), 'optional': True}),
                    ('leveltop', basic_modules.String,
                     {'labels': str(['Top level']), 'optional': True})]

    _output_ports = [('out_dataset_mon',  'csiro.au.cwsl:VtDataSet'),
                     ('out_dataset_seas', 'csiro.au.cwsl:VtDataSet'),
                     ('out_dataset_ann',  'csiro.au.cwsl:VtDataSet'),]


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

        port_names = ["timestart", "timeend", "lonwest",
                      "loneast", "latsouth", "latnorth",
                      "levelbottom", "leveltop"]
        port_vals = {}
        for name in port_names:
            try:
                port_vals[name+"_info"] = self.getInputFromPort(name)
            except vistrails_module.ModuleError as e:
                port_vals[name+"_info"] = None

        arg_number = 3
        cons_for_output = set([Constraint('suffix', ['nc'])])

        if port_vals["timestart_info"] and port_vals["timeend_info"]:
            positional_args += [('--time_bounds', arg_number, 'raw'),
                                ('timestart_info', arg_number+1),
                                ('timeend_info', arg_number+2)]
            arg_number += 3
            cons_for_output |= set([Constraint('timestart_info', [port_vals["timestart_info"]]),
                                    Constraint('timeend_info', [port_vals["timeend_info"]])])

        if port_vals["loneast_info"] and port_vals["lonwest_info"]:
            positional_args += [('--lon_bounds', arg_number, 'raw'),
                                ('lonwest_info', arg_number+1),
                                ('loneast_info', arg_number+2)]
            arg_number += 3

            lonwest_text = longitude_label(port_vals["lonwest_info"])
            loneast_text = longitude_label(port_vals["loneast_info"])

            cons_for_output |= set([Constraint('lonwest_info', [lonwest_text]),
                                    Constraint('loneast_info', [loneast_text])])

        if port_vals["latsouth_info"] and port_vals["latnorth_info"]:
            positional_args += [('--lat_bounds', arg_number, 'raw'),
                                ('latsouth_info', arg_number+1),
                                ('latnorth_info', arg_number+2)]
            arg_number += 3
            
            latsouth_text = latitude_label(port_vals["latsouth_info"])
            latnorth_text = latitude_label(port_vals["latnorth_info"])
            
            cons_for_output |= set([Constraint('latsouth_info', [latsouth_text]),
                                    Constraint('latnorth_info', [latnorth_text])])

        if port_vals["levelbottom_info"] and port_vals["leveltop_info"]:
            positional_args += [('--level_bounds', arg_number, 'raw'),
                                ('levelbottom_info', arg_number+1),
                                ('leveltop_info', arg_number+2)]
            arg_number += 3
            cons_for_output |= set([Constraint('levelbottom_info', [port_vals["levelbottom_info"]]),
                                    Constraint('leveltop_info', [port_vals["leveltop_info"]])])

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
