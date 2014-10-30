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

Tests for the FileCreator class.

"""

import logging
import unittest

from cwsl.core.constraint import Constraint
from cwsl.core.file_creator import FileCreator

logging.getLogger('cwsl.tests.test_filecreator')


class TestFileCreator(unittest.TestCase):

    def test_files_method(self):
        ''' A file creator .files method should only return files that are valid combinations exist. '''

        cons_set = set([Constraint('model', ['ACCESS1-0', 'ACCESS1-3']),
                        Constraint('experiment', ['rcp45', 'rcp85'])])
        
        this_file_creator = FileCreator("/a/fake/pattern/%model%_%experiment%.nc",
                                        extra_constraints=cons_set)
        
        # Now tell the file creator which files are real!
        # ACCESS1-3 has no rcp85 experiment in this case.
        file_1 = this_file_creator.get_files({'model': 'ACCESS1-0',
                                              'experiment': 'rcp45'},
                                             check=False, update=True)
        
        file_2 = this_file_creator.get_files({'model': 'ACCESS1-0',
                                              'experiment': 'rcp85'},
                                             check=False, update=True)

        file_3 = this_file_creator.get_files({'model': 'ACCESS1-3',
                                              'experiment': 'rcp45'},
                                             check=False, update=True)

        # Ensure that the FileCreator has returned a file
        # for each combination.
        for file_thing in [file_1, file_2, file_3]:
            self.assertTrue(file_thing)

        all_files = [file_thing for file_thing in this_file_creator.files]
        # There should only be 3 valid file combinations returned.
        self.assertEqual(len(all_files), 3)
