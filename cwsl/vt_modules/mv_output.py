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

Module to rename output files to a specified output file name.

"""

import os
import shutil

from vistrails.core.modules import vistrails_module
from vistrails.core.modules.basic_modules import String


class MoveOutput(vistrails_module.Module):
    ''' This module moves all files in a DataSet to a specified filename.'''

    # Define the module ports.
    _input_ports = [('in_dataset', 'csiro.au.cwsl:VtDataSet'),
                    ('output_name', String)]

    def compute(self):

        in_dataset = self.getInputFromPort('in_dataset')
        output_name = self.getInputFromPort('output_name')

        for metafile in in_dataset.files:
            shutil.move(metafile.full_path, output_name)
