"""

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

Authors: Tim Bedin, Tim Erwin

"""

from vistrails.core.modules.vistrails_module import Module


class VtDataSet(Module):

    # Define ports
    _input_ports = [('in_dataset', '(csiro.au.cwsl:VtDataSet)')]
    _output_ports = [('out_dataset', '(csiro.au.cwsl:VtDataSet)')]

    def compute(self):

        in_dataset = self.getInputFromPort("in_dataset")

        # This is an abstract module and should never have 'compute' called.
        raise Exception

        self.setResult('out_dataset', in_dataset)
