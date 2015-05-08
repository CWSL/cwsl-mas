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

from vistrails.core.modules import vistrails_module

from cwsl.core.constraint import Constraint
from cwsl.core.pattern_dataset import PatternDataSet


class ChangeOfDate(vistrails_module.Module):
    ''' DataSet getting the change_of_date files.'''

    # Define the module ports.
    _input_ports = [('cod_dataset', 'csiro.au.cwsl:VtDataSet')]
    _output_ports = [('out_dataset', 'csiro.au.cwsl:VtDataSet')]

    def __init__(self):

        super(ChangeOfDate, self).__init__()
        
        self.required_modules = ['python']

    def compute(self):

        file_pattern = "/short/dk7/yxw548/datastore/cod/CMIP5_v2/GFDL-ESM2G_rcp85/mec/rain/season_3/rawfield_analog_3"
        output_ds = PatternDataSet(file_pattern)
        
        self.setResult('out_dataset', output_ds)
