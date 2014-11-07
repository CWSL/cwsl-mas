"""

Authors:  Tim Bedin (Tim.Bedin@csiro.au)
          Ricardo Pascual (Ricardo.Pascual@csiro.au)
          Tim Erwin (Tim.Erwin@csiro.au)

Copyright 2014 CSIRO

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

This module wraps the CDO command to create a climatology from input files.
This is an experiment to test out the BaseModule inheritance class.

Part of the CWSLab Model Analysis Service VisTrails plugin.

"""

from cwsl.vt_modules.base_module import BaseModule


class Climatology(BaseModule):
    ''' This module takes in a start and end year and performs time averaging on the input DataSet. '''

    # Define any extra module ports.
    _input_ports += [('start_year', basic_modules.Integer,
                      {'labels': str(['Begin at this year'])}),
                     ('end_year', basic_modules.Integer,
                      {'labels': str(['End at this year'])})]
    
    _execution_options['required_modules': ['cdo', 'nco']]

    _module_setup['command': '${CWSL_CTOOLS}/aggregation/cdo_climatology.sh',
                  'user_or_authoritative': 'user',
                  'data_type': 'seasonal_aggregate']
    
    _positional_args = [('start_year', 0), ('end_year', 1)]]
