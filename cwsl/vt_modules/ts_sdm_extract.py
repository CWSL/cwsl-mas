"""

Authors: Tim Bedin

Copyright 2015 CSIRO, Australian Government Bureau of Meteorology

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

VT module to wrap the data extraction using a SDM change-of-date file.

"""

from vistrails.core.modules import vistrails_module
from vistrails.core.modules.basic_modules import String

from cwsl.configuration import configuration
from cwsl.core.process_unit import ProcessUnit
from cwsl.core.file_creator import FileCreator
from cwsl.core.constraint import Constraint


class DataExtractSDM(vistrails_module.Module):
    ''' This module extracts the relevant SDM data

    from an observational it does this by re-ordering
    daily data from a specially formatted AWAP dataset.

    '''

    # Define the module ports.
    _input_ports = [('cod_dataset', 'csiro.au.cwsl:VtDataSet'),
                    ('latitude', String),
                    ('longitude', String)]

    _output_ports = [('out_dataset', 'csiro.au.cwsl:VtDataSet')]

    def __init__(self):

        super(DataExtractSDM, self).__init__()

        self._required_modules = {'required_modules': ['python']}

    def compute(self):

        print("percent complete: 5")

        in_dataset = self.getInputFromPort('cod_dataset')
        lat = self.getInputFromPort('latitude')
        lon = self.getInputFromPort('longitude')

        command = "/usr/local/venv/bin/python /opt/cwslab-ctools/sdm/ts_extract/sdm_ts_extract.py"

        positional_args = [(lat, 0, 'raw'),
                           (lon, 1, 'raw')]

        # The data is written out to the default location.
        output_pattern = FileCreator.default_pattern(in_dataset.constraints, jobdir=True) + ".json"
        this_process = ProcessUnit([in_dataset],
                                   output_pattern,
                                   command,
                                   positional_args=positional_args,
                                   in_dataset.constraints,
                                   execution_options=self._required_modules)

        this_process.execute(simulate=configuration.simulate_execution)
        process_output = this_process.file_creator

        self.setResult('out_dataset', process_output)

        print("percent complete: 98")
