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

VT module for use in the Nino webservice - extracts multiple
nino timeseries to a single JSON file.

"""

from vistrails.core.modules import vistrails_module
from vistrails.core.modules.basic_modules import String, List

from cwsl.configuration import configuration
from cwsl.core.process_unit import ProcessUnit
from cwsl.core.file_creator import FileCreator


class NinoWebserviceExtract(vistrails_module.Module):
    " This module merges multiple nino timeseries netCDF files to a single JSON."

    # Define the module ports.
    _input_ports = [('nino_dataset', 'csiro.au.cwsl:VtDataSet')]
    _output_ports = [('out_dataset', 'csiro.au.cwsl:VtDataSet')]
    _required_modules = {'required_modules': ['python']}
    _command = '${CWSL_CTOOLS}/indices/nino_extract.py'


    def compute(self):

        in_dataset = self.getInputFromPort('nino_dataset')

        out_constraints = set([in_dataset.get_constraint('variable'),
                               in_dataset.get_constraint('experiment')])

        print("percent complete: 80")

        # The data is written out to the default location.
        output_pattern = FileCreator.default_pattern(in_dataset.constraints, jobdir=True) + ".json"
        this_process = ProcessUnit([in_dataset], output_pattern,
                                   self._command, out_constraints,
                                   execution_options=self._required_modules)

        this_process.execute(simulate=configuration.simulate_execution)
        process_output = this_process.file_creator

        print("percent complete: 98")

        self.setResult('out_dataset', process_output)
