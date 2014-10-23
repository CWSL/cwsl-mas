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

Tests for the scheduler classes.

"""

import unittest

from cwsl.core.scheduler import SimpleExecManager


class TestScheduler(unittest.TestCase):

    def setUp(self):
        self.test_cons_dict = {'test_1': 'first_test',
                               'test_2': 'second_test'}
    
    def test_schedulecommand(self):
        """ Test that the scheduler can create a script for a simple command. """

        in_files = ['infile_1']
        out_files = ['outfile_1']

        this_manager = SimpleExecManager(noexec=True)
        this_manager.add_cmd(['echo'] + in_files + out_files, out_files)
        this_manager.submit()

        expected_string = """#!/bin/sh\n\nmodule purge\nmkdir -p \necho infile_1 outfile_1\n"""
        self.assertEqual(this_manager.job.to_str(), expected_string)

