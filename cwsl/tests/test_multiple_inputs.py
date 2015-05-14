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

Tests for ProcessUnits with multiple input DataSets

"""

import unittest
import logging

import mock

from cwsl.configuration import configuration
from cwsl.core.constraint import Constraint
from cwsl.core.pattern_dataset import PatternDataSet
from cwsl.core.process_unit import ProcessUnit, EmptyOverwriteError


module_logger = logging.getLogger('cwsl.tests.test_multiple_inputs')


class TestMultipleInputs(unittest.TestCase):

    def setUp(self):

        self.mock_obs_files = ["/base/obs/tas_AWAP.nc",
                               "/base/obs/tas_HadISST.nc"]

        self.mock_model_files = ["/base/model/tas_BadModel.nc",
                                 "/base/model/tas_GoodModel.nc"]

        self.observational_pattern = "/base/obs/%variable%_%obs_model%.nc"
        self.model_pattern = "/base/model/%variable%_%model%.nc"

    def test_model_correllation(self):

        with mock.patch('cwsl.core.pattern_dataset.PatternDataSet.glob_fs') as mock_glob:
            mock_glob.return_value = self.mock_obs_files
            test_obsds = PatternDataSet(self.observational_pattern)

        with mock.patch('cwsl.core.pattern_dataset.PatternDataSet.glob_fs') as mock_glob:
            mock_glob.return_value = self.mock_model_files
            test_model_ds = PatternDataSet(self.model_pattern)

        output_pattern = "/%variable%_%obs_model%_%model%.nc"
        our_process = ProcessUnit([test_obsds, test_model_ds],
                                  output_pattern, "echo")

        output = our_process.execute()

        all_outs = [thing.full_path for thing in output.files]

        good_names = ["/tas_HadISST_BadModel.nc", "/tas_AWAP_BadModel.nc",
                      "/tas_HadISST_GoodModel.nc", "/tas_AWAP_GoodModel.nc"]

        self.assertItemsEqual(good_names, all_outs)

