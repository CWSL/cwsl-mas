"""

Authors: Tim Bedin, Tim Erwin

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


This module creates constraints to restrict datasets.

Part of the Model Analysis Service VisTrails plugin.

"""

from vistrails.core.modules.vistrails_module import Module
from vistrails.core.modules.basic_modules import List, String

from cwsl.core.constraint import Constraint


class ConstraintBuilder(Module):
    """ Outputs a set of Constraint objects to feed into VT modules.

    Some constraints are pre-built,

    Enter strings in the form KEY1=VALUE1,VALUE2,VALUE3;KEY2=VALUE4,VALUE5,VALUE6 etc.

    e.g. model = MIROC5, ACCESS1-3 ; variable = tas, tos, rsds
    
    The best way to view a list of CMIP5 model names on NCI is to type the following at the command line:
      $ ls /g/data/ua6/drstree/CMIP5/GCM

    """

    # Define ports.
    _input_ports = [('constraint_string', String)]
    _output_ports = [('constraint_set', List)]

    def compute(self):
        in_string = self.getInputFromPort('constraint_string')

        output_cons = []

        split_strings = in_string.split(';')

        for cons_string in split_strings:
            constraint_list = cons_string.split('=')

            key = constraint_list[0].strip()
            raw_values = [val for val in constraint_list[1].split(',')]
            final_values = [val_string.strip() for val_string in raw_values]
            output_cons.append(Constraint(key, final_values))

        self.setResult('constraint_set', output_cons)
