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

from cwsl.utils import utils


module_logger = logging.getLogger('cwsl.tests.test_utils')


class TestUtils(unittest.TestCase):
    """ Tests to ensure that the utility package works correctly."""

    def test_git_status(self):
        """ Test that getting the git status of a file works correctly.

        This test doesn't mock the sytem calls - relies on git being
        installed and available.

        """

        infile = __file__

        thing = utils.get_git_status(infile)

        first_letters = thing[0:8]
        
        self.assertEqual(first_letters,
                         "Git info: ")
