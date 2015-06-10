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

Print a summary of an input DataSet.

"""

from vistrails.core.modules import vistrails_module
from vistrails.core.modules.basic_modules import String


class DatasetSummary(vistrails_module.Module):
    ''' This module creates a HTML summary of an input DataSet.'''

    # Define the module ports.
    _input_ports = [('in_dataset', 'csiro.au.cwsl:VtDataSet')]
    _output_ports = [('html_summary', String)]

    def compute(self):

        in_dataset = self.getInputFromPort('in_dataset')

        header = "<!DOCTYPE html><html><body>"
        footer = "</body></html>"

        cons_header = "<h3>Constraint information:</h3><ul>"

        cons_list = ["<li>name: {}".format(constraint.key) + " values: {}</li>".format(str(constraint.values))
                     for constraint in in_dataset.constraints]

        file_header = "<h3>Files in DataSet:</h3><ul>"

        file_list = ["<li>{}</li>".format(meta.full_path)
                     for meta in in_dataset.files]

        outstring = ""
        outstring += header + cons_header + " ".join(cons_list) + "</ul>"
        outstring += file_header + " ".join(file_list) + "</ul>" + footer

        self.setResult('html_summary', outstring)
