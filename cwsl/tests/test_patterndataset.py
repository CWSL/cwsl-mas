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
from cwsl.core.pattern_dataset import PatternDataSet, ConstraintNotFoundError


logger = logging.getLogger("cwsl.tests.test_patterndataset")


class TestPatternDataSet(unittest.TestCase):

    def setUp(self):

        self.mock_file_pattern = '/fake/%colour%_%animal%.txt'

        self.fake_constraints = set([Constraint('colour', ['green', 'blue',
                                                           'red', 'purple']),
                                     Constraint('animal', ['kangaroo', 'echidna'])])

        # Create a mock set of files to avoid hitting the file system.
        mock_file_1 = '/fake/green_echidna.txt'
        mock_file_2 = '/fake/blue_kangaroo.txt'
        mock_file_3 = '/fake/red_kangaroo.txt'
        mock_file_4 = '/fake/purple_kangaroo.txt'
        self.mock_file_list = [mock_file_1, mock_file_2,
                               mock_file_3, mock_file_4]

    def test_noconstraints(self):
        ''' If no Constraint objects are given in the constructor, the PatternDataSet should check .files to find them. '''

        with mock.patch('cwsl.core.pattern_dataset.PatternDataSet.glob_fs') as mock_glob:
            mock_glob.return_value = self.mock_file_list
            test_patternds = PatternDataSet(self.mock_file_pattern)

            self.assertEqual(test_patternds.constraints,
                             self.fake_constraints)
            # Check that we only try to glob the fs once.
            mock_glob.assert_called_once_with()


    def test_getfiles(self):
        """ Ensure that files are correctly returned using 'get_files'. """

        with mock.patch('cwsl.core.pattern_dataset.PatternDataSet.glob_fs') as mock_glob:

            # Add the mock fake glob function.
            mock_glob.return_value = self.mock_file_list
            
            test_patternds = PatternDataSet(self.mock_file_pattern)
            
            found_files = test_patternds.get_files({'colour': 'green',
                                                    'animal': 'echidna'})
            expected_files = ['/fake/green_echidna.txt']

            self.assertEqual(found_files, expected_files)
            mock_glob.assert_called_once_with()

    def test_badconstraints(self):
        """ Constructing a PatternDataset with constraints that don't exist should fail. """

        # Misspelled constraint.
        test_cons = set([Constraint('modell', 'ACCESS1-0')])

        self.assertRaises(ConstraintNotFoundError, PatternDataSet,
                          "/not/real/pattern/%model%.nc",
                          constraint_set=test_cons)
