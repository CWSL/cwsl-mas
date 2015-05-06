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

This module contains tests for the MetaFile class.

"""

import logging
import unittest

from cwsl.core.metafile import MetaFile


module_logger = logging.getLogger('cwsl.tests.test_metafile')


class TestMetaFile(unittest.TestCase):
    """ Tests to ensure the MetaFile works correctly."""

    def test_equality(self):
        """ Make sure that MetaFile equality works. """

        meta_1 = MetaFile("file1", "/a/fake", {"animal": "kangaroo"})
        meta_2 = MetaFile("file2", "/a/fake", {"animal": "kangaroo"})
        meta_3 = MetaFile("file1", "/a/fake", {"colour": "kangaroo"})
        meta_4 = MetaFile("file1", "/a/fake", {"animal": "kangaroo"})
    
        self.assertNotEqual(meta_1, meta_2)
        self.assertNotEqual(meta_1, meta_3)
        self.assertEqual(meta_1, meta_4)
