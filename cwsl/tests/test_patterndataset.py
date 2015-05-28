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
from cwsl.core.metafile import MetaFile
from cwsl.core.pattern_dataset import PatternDataSet, ConstraintNotFoundError


logger = logging.getLogger("cwsl.tests.test_patterndataset")


class TestPatternDataSet(unittest.TestCase):

    def setUp(self):

        self.mock_file_pattern = '/fake/%colour%_%animal%.txt'
        self.mock_regex = '^/fake/(?P<colour>.+?)_(?P<animal>.+?)\\.txt$'

        self.fake_constraints = set([Constraint('colour', ['green', 'blue',
                                                           'red', 'purple']),
                                     Constraint('animal', ['kangaroo', 'echidna'])])

        # Create a mock set of files to avoid hitting the file system.
        self.mock_file_list = ['/fake/green_echidna.txt', '/fake/blue_kangaroo.txt',
                               '/fake/red_kangaroo.txt', '/fake/purple_kangaroo.txt']

    def test_noconstraints(self):
        ''' The PatternDataSet should glob the FS to find files. '''

        with mock.patch('cwsl.core.pattern_dataset.PatternDataSet.glob_fs') as mock_glob:
            mock_glob.return_value = self.mock_file_list
            test_patternds = PatternDataSet(self.mock_file_pattern)

            self.assertEqual(test_patternds.constraints,
                             self.fake_constraints)
            # Check that we only try to glob the fs once.
            mock_glob.assert_called_once_with()

    def test_regex(self):
        ''' Given an input pattern, the PatternDataSet should create a regular expression. '''

        test_patternds = PatternDataSet(self.mock_file_pattern)
        self.assertEqual(test_patternds.regex_pattern, self.mock_regex)


    def test_getfiles(self):
        """ Ensure that files are correctly returned using 'get_files'. """

        with mock.patch('cwsl.core.pattern_dataset.PatternDataSet.glob_fs') as mock_glob:

            # Add the mock fake glob function.
            mock_glob.return_value = self.mock_file_list

            test_patternds = PatternDataSet(self.mock_file_pattern)

            found_files = test_patternds.get_files({'colour': 'green',
                                                    'animal': 'echidna'})
            expected_files = [MetaFile('green_echidna.txt', '/fake',
                                       {'colour': 'green', 'animal': 'echidna'})]

            self.assertItemsEqual(found_files, expected_files)
            mock_glob.assert_called_once_with()

    def test_badconstraints(self):
        """ Constructing a PatternDataset with constraints that don't exist should fail. """

        # Misspelled constraint.
        test_cons = set([Constraint('modell', 'ACCESS1-0')])

        self.assertRaises(ConstraintNotFoundError, PatternDataSet,
                          "/not/real/pattern/%model%.nc",
                          constraint_set=test_cons)

    def test_build_glob_patterns(self):
        """ When constraints are given in the constructor, restrict the patterns on the fs to glob. """

        given_cons = set([Constraint('colour', ['pink', 'green'])])

        pattern_ds = PatternDataSet(self.mock_file_pattern,
                                    given_cons)

        expected_patterns = ['/fake/pink_*.txt',
                             '/fake/green_*.txt']

        self.assertItemsEqual(pattern_ds.glob_patterns,
                              expected_patterns)

    def test_cons_from_pattern(self):
        """ The PatternDataSet should build a complete set of constraints by globbing on the file system."""

        with mock.patch('cwsl.core.pattern_dataset.PatternDataSet.glob_fs') as mock_glob:
            mock_glob.return_value = self.mock_file_list

            pattern_ds = PatternDataSet(self.mock_file_pattern)

            expected_cons = self.fake_constraints

            self.assertEqual(pattern_ds.constraints, self.fake_constraints)

    def test_alias_constraints(self):
        """ The PatternDataSet should be able to alias Constraints.

        This means when asked to get files for the aliased Constraint, it should
        return files from another Constraints.

        """

        with mock.patch('cwsl.core.pattern_dataset.PatternDataSet.glob_fs') as mock_glob:
            mock_glob.return_value = self.mock_file_list

            pattern_ds = PatternDataSet(self.mock_file_pattern)

            # Apply the constraint alias - when asked for hue,
            # it will give you colour.
            pattern_ds.alias_constraint("colour", "hue")

            found_files = pattern_ds.get_files({'hue': 'red',
                                                'animal': 'kangaroo'})

            self.assertEqual(1, len(found_files))
            self.assertEqual("/fake/red_kangaroo.txt",
                             found_files[0].full_path)

