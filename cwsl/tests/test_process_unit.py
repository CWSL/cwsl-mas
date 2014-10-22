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

Tests for the ProcessUnit class.

"""

import unittest
import logging

import mock

from cwsl.core.constraint import Constraint
from cwsl.core.pattern_dataset import PatternDataSet
from cwsl.core.process_unit import ProcessUnit

module_logger = logging.getLogger('cwsl.tests.test_process_unit')


class TestProcessUnit(unittest.TestCase):

    def setUp(self):
        # This creaes a mock pattern dataset that returns a single file.

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
    
    def test_execution(self):
        """ Test that a process unit can execute a basic process without falling over. """

        # This process will echo the input and output file name to stdout.
        the_process_unit = ProcessUnit([self.a_pattern_ds], '/another/%file%/%pattern%.txt',
                                       'echo')

        ds_result = the_process_unit.execute(simulate=True)

        outfiles = [file_thing for file_thing in ds_result.files]
        self.assertEqual(len(outfiles), 1)

        expected_string = """#!/bin/sh\n\nmodule purge\necho test_file1 /another/file_1/pattern_1.txt\n"""
        self.assertEqual(expected_string, the_process_unit.scheduler.job.to_str())

    def test_positionalargs_1(self):
        """ Test that positional arguments work if the constraint is shared by both input and output. """

        the_process_unit = ProcessUnit([self.a_pattern_ds], '/another/%file%/%pattern%.txt',
                                       'echo', positional_args=[('pattern', 0)])
        
        ds_result = the_process_unit.execute(simulate=True)
        
        outfiles = [file_thing for file_thing in ds_result.files]
        self.assertEqual(len(outfiles), 1)
        
        expected_string = """#!/bin/sh\n\nmodule purge\necho pattern_1 test_file1 /another/file_1/pattern_1.txt\n"""
        self.assertEqual(expected_string, the_process_unit.scheduler.job.to_str())

    def test_positionalargs_2(self):
        """ Test that positional arguments work if the constraint is part of the input only. """

        the_process_unit = ProcessUnit([self.a_pattern_ds], '/another/%file%/%pattern%.txt',
                                       'echo', positional_args=[('fake', 0)])
        
        ds_result = the_process_unit.execute(simulate=True)
        
        outfiles = [file_thing for file_thing in ds_result.files]
        self.assertEqual(len(outfiles), 1)
        
        expected_string = """#!/bin/sh\n\nmodule purge\necho fake_1 test_file1 /another/file_1/pattern_1.txt\n"""
        self.assertEqual(expected_string, the_process_unit.scheduler.job.to_str())
