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


VT Module to build a DataSet from a pattern on the file system.

"""

from cwsl.core.pattern_dataset import PatternDataSet

from vistrails.core.modules import vistrails_module
from vistrails.core.modules.basic_modules import String


class OpenDataSet(vistrails_module.Module):
    ''' Build a DataSet from a provided pattern string.

    If you are interested in the files:
        /data/model1/variable1_date1.nc
        /data/model2/variable2_date2.nc
        /data/model3/variable3_date3.nc
    
    The pattern should look like the following:
    
        "/data/%name-of-model%/%variable-name%_%date%.%file-type%"
    
    '''

    _input_ports = [("filesystem_pattern", String)]
    _output_ports = [('out_dataset', 'csiro.au.cwsl:VtDataSet')]

    def compute(self):

        file_pattern = self.getInputFromPort("filesystem_pattern")

        extra_constraints = set()

        output_ds = PatternDataSet(file_pattern, extra_constraints)

        self.setResult('out_dataset', output_ds)
