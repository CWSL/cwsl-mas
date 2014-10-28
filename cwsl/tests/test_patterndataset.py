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

Contains tests for the PatternDataSet class.

"""

import unittest
import logging

import mock

import cwsl.core.pattern_dataset
from cwsl.core.constraint import Constraint
from cwsl.core.pattern_dataset import PatternDataSet


logger = logging.getLogger("cwsl.tests.test_patterndataset")


class TestPatternDataSet(unittest.TestCase):

    def setUp(self):

        test_cons = set([Constraint('colour', ['green', 'blue']),
                         Constraint('animal', ['echidna', 'kangaroo'])])

        mock_file_1 = mock.MagicMock()
        mock_file_2 = mock.MagicMock()
        mock_file_1.full_path = '/fake/green_echidna.txt'
        mock_file_1.__str__.return_value = '/fake/green_echidna.txt'
        mock_file_2.full_path = '/fake/blue_kangaroo.txt'
        mock_file_2.__str__.return_value = '/fake/blue_kangaroo.txt'
        
        # Overwrite the 'files' attribute of the class.
        mock_file_list = [mock_file_1, mock_file_2]
                
        self.test_patternds = PatternDataSet("/fake/%colour%_%animal%.txt",
                                             constraint_set=test_cons)
        self.test_patternds._files = mock_file_list

    def test_getfiles(self):
        """ Ensure that files are correctly returned using 'get_files'. """

        
        found_files = self.test_patternds.get_files({'colour': 'green',
                                                     'animal': 'echidna'})
        expected_files = ['/fake/green_echidna.txt']
        
        self.assertEqual(found_files, expected_files)
