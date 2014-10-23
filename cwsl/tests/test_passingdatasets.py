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

Contains tests to make sure ProcessUnits pass their data on with the correct Constraints.

"""

import logging
import unittest

import mock

from cwsl.configuration import configuration
from cwsl.core.constraint import Constraint
from cwsl.core.pattern_dataset import PatternDataSet
from cwsl.core.process_unit import ProcessUnit

module_logger = logging.getLogger("cwsl.tests.test_passingdatasets")


class TestPassingData(unittest.TestCase):

    def setUp(self):
        """ Makes a mock PatternDS that can be executed. """
        test_cons = set([Constraint('fake', ['fake_1']),
                         Constraint('file', ['file_1']),
                         Constraint('pattern', ['pattern_1'])])
        
        self.a_pattern_ds = PatternDataSet('/a/%fake%/%file%/%pattern%',
                                           constraint_set=test_cons)
        # Mock the get_files method - we will only return a single, mock file object.
        mock_file = mock.MagicMock()
        mock_file.full_path = 'test_file1'
        mock_file.__str__.return_value = 'test_file1'
        self.a_pattern_ds.get_files = mock.Mock(return_value=[mock_file])
        
        # Create a valid set of contraints for the mock.
        self.a_pattern_ds.valid_combinations = set([frozenset(test_cons)])
        
        self.script_header = "#!/bin/sh\n\nmodule purge\nexport CWSL_CTOOLS={}\nexport PYTHONPATH=$PYTHONPATH:{}/pythonlib\n"\
            .format(configuration.cwsl_ctools_path, configuration.cwsl_ctools_path)
        
        
    def test_overwrite_constraints(self):
        """ Test to ensure that Constraints are correctly overwritten when data is processed. """

        extra_cons = set([Constraint('extras', ['other_things']),
                          Constraint('fake', ['OVERWRITE'])])
        a_process_unit = ProcessUnit([self.a_pattern_ds], "/foo/%fake%/%file%/%pattern%_%extras%.txt",
                                     "echo", extra_constraints=extra_cons)

        output_ds = a_process_unit.execute(simulate=True)

        expected_cons = set([Constraint('extras', ['other_things']),
                             Constraint('fake', ['OVERWRITE']),
                             Constraint('file', ['file_1']),
                             Constraint('pattern', ['pattern_1'])])
        self.assertEqual(expected_cons, output_ds.constraints)
        
        expected_string = self.script_header + "mkdir -p /foo/OVERWRITE/file_1\necho test_file1 /foo/OVERWRITE/file_1/pattern_1_other_things.txt\n"
        self.assertEqual(expected_string, a_process_unit.scheduler.job.to_str())
