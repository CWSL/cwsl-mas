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

Tests for the ProcessUnit 'mapping' API - renaming constraints
between input and output.

"""

import unittest
import logging

import mock

from cwsl.configuration import configuration
from cwsl.core.pattern_dataset import PatternDataSet
from cwsl.core.process_unit import ProcessUnit


module_logger = logging.getLogger('cwsl.tests.test_mapping')


class TestMapping(unittest.TestCase):

    def setUp(self):
        
        self.input_pattern = '/a/fake/%gcm_model%_%variable%_%file_type%'
        self.output_pattern = '/a/fake/%obs_model%_%variable%_%file_type%'

        # Constant header for the job scripts.
        self.script_header = "#!/bin/sh\nset -e\n\nmodule purge\nexport CWSL_CTOOLS={}\nexport PYTHONPATH=$PYTHONPATH:{}/pythonlib\n"\
            .format(configuration.cwsl_ctools_path, configuration.cwsl_ctools_path)

    def test_simple_mapping(self):

        # Create a mock pattern dataset to use in process unit tests.
        with mock.patch('cwsl.core.pattern_dataset.PatternDataSet.glob_fs') as mock_glob:

            mock_file_1 = '/a/fake/ACCESS1-0_tas_netCDF'
            mock_file_2 = '/a/fake/MIROC_tas_netCDF'
            mock_file_3 = '/a/fake/ACCESS1-0_pr_netCDF'

            mock_glob.return_value = [mock_file_1, mock_file_2, mock_file_3]
        
            a_pattern_ds = PatternDataSet(self.input_pattern)

            the_process_unit = ProcessUnit([a_pattern_ds], self.output_pattern,
                                           'echo', map_dict={'obs_model': ('gcm_model', 0)})
            
            ds_result = the_process_unit.execute(simulate=True)
            
            outfiles = [file_thing for file_thing in ds_result.files]
            self.assertEqual(len(outfiles), 3)
        
            expected_string = self.script_header + "mkdir -p /another/file_1\necho test_file1 /another/file_1/pattern_1.txt\n"


