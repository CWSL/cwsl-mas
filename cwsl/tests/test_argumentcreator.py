"""
Authors: Tim Bedin

Copyright 2015 CSIRO / Australian Government Bureau of Meteorology

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

This module contains tests for the ArgumentCreator class.

"""

import logging
import unittest

import mock

from cwsl.core.argument_creator import ArgumentCreator
from cwsl.core.pattern_dataset import PatternDataSet
from cwsl.core.file_creator import FileCreator

module_logger = logging.getLogger('cwsl.tests.test_argumentcreator')


class TestArgumentCreator(unittest.TestCase):
    """ Tests to ensure that looping and grouping works correctly."""

    def setUp(self):
        """ Set up some basic DataSets."""

        mock_file_pattern_1 = "/fake/%food%_%animal%.file"
        mock_file_list_1 = ["/fake/pizza_moose.file", "/fake/pizza_rabbit.file",
                            "/fake/chocolate_bilby.file","/fake/chocolate_rabbit.file"]

        mock_file_pattern_2 = "/fake/%animal%.file"
        mock_file_list_2 = ["/fake/moose.file", "/fake/rabbit.file",
                            "/fake/bilby.file"]
        
        with mock.patch('cwsl.core.pattern_dataset.PatternDataSet.glob_fs') as mock_glob:
            mock_glob.return_value = mock_file_list_1
            self.test_patternds_1 = PatternDataSet(mock_file_pattern_1)

        with mock.patch('cwsl.core.pattern_dataset.PatternDataSet.glob_fs') as mock_glob:
            mock_glob.return_value = mock_file_list_2
            self.test_patternds_2 = PatternDataSet(mock_file_pattern_2)

    def test_simple_looping(self):
        """ Test that basic one-to-one looping works."""

        one_to_one_creator = FileCreator("/output/%animal%.output",
                                         [self.test_patternds_2.get_constraint("animal")])

        
        looper = ArgumentCreator([self.test_patternds_2], one_to_one_creator)
        
        all_outs = []
        for thing in looper:
            self.assertEqual(len(thing[0]), len(thing[1]))
            all_outs.append(thing)

        all_files = [thing for thing in one_to_one_creator.files]
        self.assertEqual(len(all_outs), len(all_files))

    def test_many_looping(self):
        """ Test that simple many-to-one looping works."""
        
        many_to_one_creator = FileCreator("/output/%animal%.output",
                                          [self.test_patternds_1.get_constraint("animal")])
        
        looper = ArgumentCreator([self.test_patternds_1], many_to_one_creator)

        all_outs = []
        for thing in looper:
            self.assertGreaterEqual(len(thing[0]), len(thing[1]))
            all_outs.append(thing)

        # There are three animals.
        self.assertEqual(3, len(all_outs))

