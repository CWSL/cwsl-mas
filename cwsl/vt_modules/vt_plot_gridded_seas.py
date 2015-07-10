"""

Creates a gridded seasonal plot

This module wraps the plot_gridded_seas.sh script found in git repository cwsl-ctools.

Part of the CWSLab VisTrails plugin.

Authors: Craig Heady, craig.heady@csiro.au
         Tim Bedin,   Tim.Bedin@csiro.au
         Tim Erwin,   Tim.Erwin@csiro.au

Copyright 2015 CSIRO

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

import os

from vistrails.core.modules import vistrails_module
from vistrails.core.modules.basic_modules import String, List

from cwsl.configuration import configuration
from cwsl.core.process_unit import ProcessUnit
from cwsl.core.constraint import Constraint
from cwsl.core.pattern_generator import PatternGenerator


class PlotGriddedSeas(vistrails_module.Module):
    """
    Plots a gridded field(season).

    Required inputs: 

    Other inputs:    infile (from dataset connector)
                     title (model name from dataset connector)

    Requires: python, cdat, cct, matplotlib, basemap

region's:
'AUS_PCCSP'
'PACIFIC'
'PACCSAP'
'VANUATU'
'AUSTRALIA'
'AUSTRALIA_NZ'
'AUSTRALIA_EXT'
'AUSTRALIA_EXT_NZ'
'SE_AUSTRALIA'
'QLD'
'SEQLD'
'BRISBANE'
'WORLD'
'WORLD360'
'STH_SULAWESI'
'MAKASSAR'
'INDONESIA'
'EAST_INDONESIA'
'NTB'
'ZOOM'
'AUREL_AND_LAWSON'
'MONSOONAL_NORTH'
'WET_NORTH'
'RANGELANDS'
'MURRAY_BASIN'
'EAST_COAST'
'SOUTHERN_SLOPES'
'S&SW_FLATLANDS'
'BBBASIN'
'INDIA'
'SOUTHASIA'
'australia_NRM-eval'
'australia_NRM-eval-psl'
    """

    # Define the module ports.
    _input_ports = [('in_dataset', 'csiro.au.cwsl:VtDataSet',
                     {'labels': str(['Input Dataset'])}),
                    ('variable', String, {"defaults": str([''])}),
                    ('plot_type', String, {"defaults": str(['pcolor'])}),
                    ('title', String, {"defaults": str([''])}),
                    ('region', String, {"defaults": str(['WORLD360'])}),
                    ('colormap', String, {"defaults": str([''])}),
                    ('ticks', String, {"defaults": str([''])}),
                    ('conv_units', String, {"defaults": str(['False'])}),
                   ]

    _output_ports = [('out_dataset', 'csiro.au.cwsl:VtDataSet',)]

    _execution_options = {'required_modules': ['cdo','python/2.7.5','python-cdat-lite/6.0rc2-py2.7.5',
                                               'cct/trunk','python/2.7.5-matplotlib',
                                               'python-basemap/1.0.7-py2.7']
                         }

    def __init__(self):

        super(PlotGriddedSeas, self).__init__()

        self.command = '${CWSL_CTOOLS}/visualisation/plot_gridded_seas.py'

        # Get the output pattern using the PatternGenerator object.
        # Gets the user infomation / authoritative path etc from the
        # user configuration.
        self.out_pattern = PatternGenerator('user', 'default').pattern

    def compute(self):

        # Required input
        in_dataset = self.getInputFromPort("in_dataset")

        variable = self.getInputFromPort("variable")
        #self.positional_args=[(variable_name, 0, 'raw')]
        
        plot_type = self.getInputFromPort("plot_type")
        #self.positional_args=[(plot_type, 1, 'raw')]
        
        title = self.getInputFromPort("title")
        #self.positional_args=[(title, 2, 'raw')]
        
        region = self.getInputFromPort("region")
        #self.positional_args=[(plot_type, 3, 'raw')]
        
        colormap = self.getInputFromPort("colormap")
        #self.positional_args=[(colormap, 4, 'raw')]
        
        ticks = self.getInputFromPort("ticks")
        #self.positional_args=[(plot_type, 5, 'raw')]

        conv_units = self.getInputFromPort("conv_units")
        #self.positional_args=[(conv_units, 6, 'raw')]
        
        cons_for_output = set([Constraint('suffix', ['png'])])

        run_opts = ''
        if variable:
            run_opts = run_opts + ' --variable %s' %variable
        if plot_type:
            run_opts = run_opts + ' --plot_type %s' %plot_type
        if title:
            run_opts = run_opts + ' --title %s' %title
        if region:
            run_opts = run_opts + ' --region %s' %region
        if colormap:
            run_opts = run_opts + ' --colourmap %s' %colormap
        if ticks:
            run_opts = run_opts + " --ticks '%s'" %ticks
        if conv_units:
            run_opts = run_opts + " --conv_units '%s'" %conv_units


        # Execute plotting process.
        this_process = ProcessUnit([in_dataset],
                                   self.out_pattern,
                                   self.command,
                                   cons_for_output,
                                   execution_options=self._execution_options,
                                   #positional_args=self.positional_args,
                                   kw_string=run_opts)
                                   #kw_string="--title '${model}_${experiment}'")

        try:
            process_output = this_process.execute(simulate=configuration.simulate_execution)
        except Exception as e:
            raise vistrails_module.ModuleError(self, repr(e))

        self.setResult('out_dataset', process_output)
