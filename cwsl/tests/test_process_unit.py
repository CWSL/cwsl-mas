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

from cwsl.configuration import configuration
from cwsl.core.constraint import Constraint
from cwsl.core.pattern_dataset import PatternDataSet
from cwsl.core.process_unit import ProcessUnit, EmptyOverwriteError


module_logger = logging.getLogger('cwsl.tests.test_process_unit')


class TestProcessUnit(unittest.TestCase):

    def setUp(self):
        # This creates a mock pattern dataset that returns a single file.
        test_cons = set([Constraint('fake', ['fake_1']),
                         Constraint('file', ['file_1']),
                         Constraint('pattern', ['pattern_1'])])

        self.a_pattern_ds = PatternDataSet('/a/%fake%/%file%/%pattern%',
                                           constraint_set=test_cons)
        # Mock the get_files method - we will only return a single, mock file object.
        mock_file = mock.MagicMock()
        mock_file.full_path = 'test_file1'
        mock_file.__str__.return_value = 'test_file1'
        mock_file.all_atts = {"fake": "fake_1",
                              "file": "file_1",
                              "pattern": "pattern_1"}
        self.a_pattern_ds.get_files = mock.Mock(return_value=[mock_file])

        # Create a valid set of contraints for the mock.
        self.a_pattern_ds.valid_combinations = set([frozenset(test_cons)])

        # Constant header for the job scripts.
        self.script_header = "#!/bin/sh\nset -e\n\nmodule purge\nexport CWSL_CTOOLS={}\nexport PYTHONPATH=$PYTHONPATH:{}/pythonlib\n"\
            .format(configuration.cwsl_ctools_path, configuration.cwsl_ctools_path)

    def test_execution(self):
        """ Test that a process unit can execute a basic process without falling over. """

        # This process will echo the input and output file name to stdout.
        the_process_unit = ProcessUnit([self.a_pattern_ds], '/another/%file%/%pattern%.txt',
                                       'echo')

        ds_result = the_process_unit.execute(simulate=True)

        outfiles = [file_thing for file_thing in ds_result.files]
        self.assertEqual(len(outfiles), 1)

        expected_string = self.script_header + "mkdir -p /another/file_1\necho test_file1 /another/file_1/pattern_1.txt\n"
        self.assertEqual(expected_string, the_process_unit.scheduler.job.to_str())

    def test_positionalargs_1(self):
        """ Test that positional arguments work if the constraint is shared by both input and output. """

        the_process_unit = ProcessUnit([self.a_pattern_ds], '/another/%file%/%pattern%.txt',
                                       'echo', positional_args=[('pattern', 0)])

        ds_result = the_process_unit.execute(simulate=True)

        outfiles = [file_thing for file_thing in ds_result.files]
        self.assertEqual(len(outfiles), 1)

        expected_string = self.script_header + "mkdir -p /another/file_1\necho pattern_1 test_file1 /another/file_1/pattern_1.txt\n"
        self.assertEqual(expected_string, the_process_unit.scheduler.job.to_str())

    def test_positionalargs_2(self):
        """ Test that positional arguments work if the constraint is part of the input only. """

        the_process_unit = ProcessUnit([self.a_pattern_ds], '/another/%file%/%pattern%.txt',
                                       'echo', positional_args=[('fake', 0)])

        ds_result = the_process_unit.execute(simulate=True)

        outfiles = [file_thing for file_thing in ds_result.files]
        self.assertEqual(len(outfiles), 1)

        expected_string = self.script_header + "mkdir -p /another/file_1\necho fake_1 test_file1 /another/file_1/pattern_1.txt\n"
        self.assertEqual(expected_string, the_process_unit.scheduler.job.to_str())

    def test_positionalargs_3(self):
        """ Test that positional arguments work if the constraint is part of the output only. """

        extra_cons = set([Constraint('animal', ['moose', 'kangaroo'])])
        the_process_unit = ProcessUnit([self.a_pattern_ds], '/another/%file%/%pattern%_%animal%.txt',
                                       'echo', extra_constraints=extra_cons,
                                       positional_args=[('animal', 0)])

        ds_result = the_process_unit.execute(simulate=True)

        outfiles = [file_thing for file_thing in ds_result.files]
        self.assertEqual(len(outfiles), 2)

        expected_string = self.script_header + 'mkdir -p /another/file_1\necho moose test_file1 /another/file_1/pattern_1_moose.txt\necho kangaroo test_file1 /another/file_1/pattern_1_kangaroo.txt\n'
        self.assertEqual(expected_string, the_process_unit.scheduler.job.to_str())

    def test_positionalargs_4(self):
        """ Test that positional arguments work if multiple extra constraints found only in the output are added. """

        extra_cons = set([Constraint('animal', ['moose', 'kangaroo']),
                          Constraint('colour', ['blue'])])

        the_process_unit = ProcessUnit([self.a_pattern_ds], '/another/%file%/%pattern%_%animal%_%colour%.txt',
                                       'echo', extra_constraints=extra_cons,
                                       positional_args=[('animal', 0), ('colour', 1)])
        ds_result = the_process_unit.execute(simulate=True)

        expected_string = self.script_header + 'mkdir -p /another/file_1\necho moose blue test_file1 /another/file_1/pattern_1_moose_blue.txt\necho kangaroo blue test_file1 /another/file_1/pattern_1_kangaroo_blue.txt\n'
        self.assertEqual(expected_string, the_process_unit.scheduler.job.to_str())

    def test_overwrites(self):
        """ Test that datasets/file creators always maintain their correct constraints during constraint overwrites. """

        extra_con = set([Constraint('fake', ['OVERWRITE'])])
        the_process_unit = ProcessUnit([self.a_pattern_ds], '/%fake%/%file%/%pattern%.txt',
                                       'echo', extra_constraints=extra_con)

        ds_result = the_process_unit.execute(simulate=True)

        expected_in_cons = set([Constraint('fake', ['fake_1']),
                                Constraint('file', ['file_1']),
                                Constraint('pattern', ['pattern_1'])])
        expected_out_cons = set([Constraint('fake', ['OVERWRITE']),
                                 Constraint('file', ['file_1']),
                                 Constraint('pattern', ['pattern_1'])])

        self.assertEqual(expected_in_cons, self.a_pattern_ds.constraints)
        self.assertEqual(expected_out_cons, ds_result.constraints)

    def test_empty_constraint_overwrite(self):
        """ Test that ProcessUnit throw an exception if a constraint is overwritten with nothing."""

        extra_con = set([Constraint('fake', [])])

        self.assertRaises(EmptyOverwriteError, ProcessUnit, [self.a_pattern_ds], '/%fake%/%file%/%pattern%.txt',
                          'echo', extra_constraints=extra_con)

    def test_mix_types(self):
        """ Test to ensure that mixing keyword and positional arguments works as expected. """

        the_process_unit_1 = ProcessUnit([self.a_pattern_ds], '/%fake%/%file%/%pattern%.txt',
                                         'echo', cons_keywords={'fake_name': 'fake'})
        result_1 = the_process_unit_1.execute(simulate=True)
        expected_string_1 = self.script_header + 'mkdir -p /fake_1/file_1\necho test_file1 /fake_1/file_1/pattern_1.txt --fake_name fake_1\n'

        self.assertEqual(expected_string_1, the_process_unit_1.scheduler.job.to_str())


        # Now try both keyword and positional.
        the_process_unit_2 = ProcessUnit([self.a_pattern_ds], '/%fake%/%file%/%pattern%.txt',
                                         'echo', cons_keywords={'fake_name': 'fake'},
                                         positional_args=[('--input', 0, 'raw'), ('--output', 2, 'raw')])
        result_2 = the_process_unit_2.execute(simulate=True)
        expected_string_2 = self.script_header + 'mkdir -p /fake_1/file_1\necho --input test_file1 --output /fake_1/file_1/pattern_1.txt --fake_name fake_1\n'

        self.assertEqual(expected_string_2, the_process_unit_2.scheduler.job.to_str())

    def test_mapping(self):
        """ Test to allow simple mapping of a constraint in the input to one in the output. """

        # Input PatternDS has constraints fake, file and pattern.
        # Use fake from first input as animal constraint.
        the_process_unit = ProcessUnit([self.a_pattern_ds], '/a/new/pattern/%animal%/%file%/%pattern%.file',
                                       'echo', map_dict={'animal': ('fake', 0)})
        output = the_process_unit.execute(simulate=True)

        all_files = [thing for thing in output.files]

        self.assertEqual(len(all_files), 1)
        self.assertEqual(all_files[0].full_path, '/a/new/pattern/fake_1/file_1/pattern_1.file')

    def test_kwstrings(self):
        """ Test to check that multi-constraint keyword arguments can be created. """

        the_process_unit = ProcessUnit([self.a_pattern_ds], '/a/new/pattern/%fake%/%file%/%pattern%.file',
                                       'echo', kw_string="--title $fake-$file")
        output = the_process_unit.execute(simulate=True)

        expected_string = self.script_header + 'mkdir -p /a/new/pattern/fake_1/file_1\necho test_file1 /a/new/pattern/fake_1/file_1/pattern_1.file --title fake_1-file_1\n'

        self.assertEqual(expected_string, the_process_unit.scheduler.job.to_str())
