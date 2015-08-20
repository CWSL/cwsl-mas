"""

Authors: Tim Bedin, Tim Erwin, Damien Irving

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


VT Module to build a DataSet from the COD files.

"""

import os.path

from vistrails.core.modules import vistrails_module
from vistrails.core.modules.basic_modules import String

from cwsl.core.constraint import Constraint
from cwsl.core.pattern_dataset import PatternDataSet


class TosDataset(vistrails_module.Module):
    ''' DataSet getting the local storage tos files.'''

    _input_ports = [('model', String),
                    ('experiment', String),
                    ('tos_datapath', String)]

    _output_ports = [('out_dataset', 'csiro.au.cwsl:VtDataSet')]

    _required_modules = ['python']


    def compute(self):
        
        print("percent complete: 1")

        cons_list = ['model', 'experiment']
        in_cons = set([Constraint(cons_name, [self.getInputFromPort(cons_name)])
                       for cons_name in cons_list])

        tos_datapath = self.getInputFromPort('tos_datapath')
        
        file_pattern = os.path.join(tos_datapath,
                                    "%institute%/%model%/%experiment%/mon/ocean/tos/r1i1p1/tos_Omon_%model%_%experiment%_r1i1p1_%otherthing%.nc")
        output_ds = PatternDataSet(file_pattern, in_cons)

        self.setResult('out_dataset', output_ds)
