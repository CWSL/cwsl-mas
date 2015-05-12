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

This module contains tests for the utils module.

"""

import logging
import unittest
import tempfile

from cwsl.utils import utils


module_logger = logging.getLogger('cwsl.tests.test_utils')


class TestUtils(unittest.TestCase):
    """ Tests to ensure that the utility package works correctly."""

    def setUp(self):

        self.tempfile = tempfile.NamedTemporaryFile()

    def tearDown(self):

        self.tempfile.close()

    def test_git_status(self):
        """ Test that getting the git status of a file works correctly.

        This test doesn't mock the system calls - relies on git being
        installed and available.

        """

        # We will use this file and assume that it is in git.
        infile = __file__
        if infile[-1] == 'c':
            # Catch .pyc files - not in git.
            infile = infile[:-1]

        status_return = utils.get_git_status(infile)
        first_letters = status_return[0:10]
        
        self.assertEqual(first_letters, "Git info: ")
        
        # Try a file that doesn't exist.
        nonexist = "/this/is/not/a/file.txt"
        status_return = utils.get_git_status(nonexist)
        self.assertEqual(status_return,
                         "Could not determine file version.")

        # Try a file that exists but not in git.
        status_return = utils.get_git_status(self.tempfile.name)
        self.assertEqual(status_return,
                         "Could not determine file version.")
