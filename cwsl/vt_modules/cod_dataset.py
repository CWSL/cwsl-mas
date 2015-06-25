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
from vistrails.core.modules.basic_modules import String

from cwsl.core.constraint import Constraint
from cwsl.core.pattern_dataset import PatternDataSet


class ChangeOfDate(vistrails_module.Module):
    ''' DataSet getting the change_of_date files.'''

    _input_ports = [('model', String, {"defaults": str([''])}),
                    ('experiment', String, {"defaults": str([''])}),
                    ('variable', String, {"defaults": str([''])}),
                    ('season_number', String, {"defaults": str([''])}),
                    ('region', String, {"defaults": str([''])})]

    _output_ports = [('out_dataset', 'csiro.au.cwsl:VtDataSet')]

    def __init__(self):

        super(ChangeOfDate, self).__init__()
        
        self.required_modules = ['python']

    def compute(self):
        
        cons_list = ['model', 'experiment', 'variable',
                     'season_number', 'region']
        in_cons = set([Constraint(cons_name, [self.getInputFromPort(cons_name)])
                       for cons_name in cons_list
                       if self.getInputFromPort(cons_name)])

        file_pattern = "/g/data/ua6/CAWCR_CVC_processed/staging/users/CWSL/SDM/COD/CMIP5_v2/%model%_%experiment%/%region%/%variable%/season_%season_number%/rawfield_analog_%season_number%"
        output_ds = PatternDataSet(file_pattern, in_cons)

        self.setResult('out_dataset', output_ds)
