"""
Authors:  Damien Irving (irving.damien@gmail.com)

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

This module wraps a shell script that calculates the temporal anomaly:
cwsl-ctools/utils/cdo_temporal_anomaly.sh

Part of the CWSLab Model Analysis Service VisTrails plugin.

"""

from vistrails.core.modules import vistrails_module, basic_modules

from cwsl.configuration import configuration
from cwsl.core.constraint import Constraint
from cwsl.core.process_unit import ProcessUnit
from cwsl.core.pattern_generator import PatternGenerator


class TemporalAnomaly(vistrails_module.Module):
    """Temporal anomaly.

    Wraps the cwsl-ctools/utils/cdo_temporal_anomaly.sh script.

    Inputs:
      in_dataset: Can consist of netCDF files and/or cdml catalogue files
      clim_bounds (optional): Time bounds for the climatology used to 
        calculate the anomaly timeseries. 
      timescale (optional): Timescale for anomaly calculation (can be yday, 
        ymon, yseas for daily, monthly or seasonal anomaly)

    Outputs:
      out_dataset: Consists of netCDF files (i.e. cdml catalogue files
      are converted). 

    """

    # Define the module ports.
    _input_ports = [('in_dataset', 'csiro.au.cwsl:VtDataSet',
                     {'labels': str(['Input dataset'])}),
                    ('clim_bounds', basic_modules.String,
                     {'labels': str(['YYYY-MM-DD,YYYY-MM-DD']),'optional': True}),
                    ('timescale', basic_modules.String,
                     {'labels': str(['Anomaly timescale']), 'optional': True}),]

    _output_ports = [('out_dataset', 'csiro.au.cwsl:VtDataSet')]

    _execution_options = {'required_modules': ['cdo', 'python/2.7.5','python-cdat-lite/6.0rc2-py2.7.5']}


    def __init__(self):

        super(TemporalAnomaly, self).__init__()

        #Command Line Tool
        self.command = '${CWSL_CTOOLS}/utils/cdo_temporal_anomaly.sh'

        # Output file structure declaration
        self.out_pattern = PatternGenerator('user', 'default').pattern

    def compute(self):

        # Required input
        in_dataset = self.getInputFromPort("in_dataset")

        # Set up the output command for this module, adding extra options.
        positional_args = []
        anom_label = 'anom-wrt-all'
        arg_number = 0
        
        try:
            clim_bounds = self.getInputFromPort('clim_bounds')
            positional_args += [('-b', arg_number, 'raw'),
                                (clim_bounds, arg_number+1, 'raw')]
            start_date, end_date = clim_bounds.split(',')
            anom_label = 'anom-wrt-'+start_date+'-'+end_date
            arg_number += 2
        except vistrails_module.ModuleError as e:
            pass

        try:
            timescale = self.getInputFromPort('timescale')
            positional_args += [('-t', arg_number, 'raw'),
                                (timescale, arg_number+1, 'raw')]
            anom_label = timescale+anom_label
        except vistrails_module.ModuleError as e:
            pass

        cons_for_output = set([Constraint('suffix', ['nc']),
                               Constraint('anomaly_info', [anom_label])])

        # Execute the process.
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

