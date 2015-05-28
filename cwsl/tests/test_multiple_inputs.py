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
from cwsl.core.process_unit import ProcessUnit


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

    def test_changefile_generation(self):
        """ This test works to cover the common case when you want to calculate changes.

        For example, comparing two dataset by time.
        """

        mock_files_1 = ["/model1_1986_rain", "/model2_1986_rain",
                        "/model3_1986_rain", "/model4_1986_rain",
                        "/model1_1986_temp"]
        mock_files_2 = ["/model1_2015_rain", "/model2_2015_rain",
                        "/model3_2015_rain", "/model4_2015_rain",
                        "/model1_2015_temp"]

        input_pattern = "/%model%_%date%_%variable%"

        # Create our mock DataSets.
        with mock.patch('cwsl.core.pattern_dataset.PatternDataSet.glob_fs') as mock_glob:
            mock_glob.return_value = mock_files_1
            test_ds_1 = PatternDataSet(input_pattern)

        with mock.patch('cwsl.core.pattern_dataset.PatternDataSet.glob_fs') as mock_glob:
            mock_glob.return_value = mock_files_2
            test_ds_2 = PatternDataSet(input_pattern)

        # A ProcessUnit which merges the Constraint on colour.
        the_process = ProcessUnit([test_ds_1, test_ds_2], "/tmp/%model%_%date%_%variable%",
                                  "echo", merge_output=["date"])

        output_ds = the_process.execute(simulate=True)

        outfile_names = [metafile.full_path for metafile in output_ds.files]

        expected_files = ["/tmp/model1_1986-2015_rain",
                          "/tmp/model2_1986-2015_rain",
                          "/tmp/model3_1986-2015_rain",
                          "/tmp/model4_1986-2015_rain",
                          "/tmp/model1_1986-2015_temp"]

        self.assertItemsEqual(expected_files, outfile_names)


    def test_model_correllation_2(self):
        """ This test is to try combining Constraints from two different DataSets.

        This uses the new merge_output keyword option.

        """

        mock_file_1 = ["/red_echidna"]
        mock_file_2 = ["/blue_echidna"]

        input_pattern = "/%colour%_%animal%"

        # Create our mock DataSets.
        with mock.patch('cwsl.core.pattern_dataset.PatternDataSet.glob_fs') as mock_glob:
            mock_glob.return_value = mock_file_1
            test_ds_1 = PatternDataSet(input_pattern)

        with mock.patch('cwsl.core.pattern_dataset.PatternDataSet.glob_fs') as mock_glob:
            mock_glob.return_value = mock_file_2
            test_ds_2 = PatternDataSet(input_pattern)

        # A ProcessUnit which merges the Constraint on colour.
        the_process = ProcessUnit([test_ds_1, test_ds_2], "/tmp/%animal%_%colour%.file",
                                  "echo", merge_output=["colour"])

        output_ds = the_process.execute(simulate=False)

        outfiles = [metafile for metafile in output_ds.files]

        self.assertEqual(len(outfiles), 1)

        self.assertEqual(outfiles[0].full_path, "/tmp/echidna_red-blue.file")

    def test_model_correllation_3(self):
        """ This test is to try combining multiple DataSets, each with many files. """

        mock_files_1 = ["/red_echidna", "/blue_echidna", "/green_echidna"]
        mock_files_2 = ["/blue_echidna", "/red_echidna", "/green_echidna"]

        input_pattern = "/%colour%_%animal%"

        # Create our mock DataSets.
        with mock.patch('cwsl.core.pattern_dataset.PatternDataSet.glob_fs') as mock_glob:
            mock_glob.return_value = mock_files_1
            test_ds_1 = PatternDataSet(input_pattern)

        with mock.patch('cwsl.core.pattern_dataset.PatternDataSet.glob_fs') as mock_glob:
            mock_glob.return_value = mock_files_2
            test_ds_2 = PatternDataSet(input_pattern)

        # A ProcessUnit which merges the Constraint on colour.
        the_process = ProcessUnit([test_ds_1, test_ds_2], "/tmp/%animal%_%colour%.file",
                                  "echo", merge_output=["colour"])

        output_ds = the_process.execute(simulate=True)

        outfile_names = [metafile.full_path for metafile in output_ds.files]

        expected_outfiles = ["/tmp/echidna_red-red.file",
                             "/tmp/echidna_red-blue.file",
                             "/tmp/echidna_red-green.file",
                             "/tmp/echidna_blue-red.file",
                             "/tmp/echidna_blue-blue.file",
                             "/tmp/echidna_blue-green.file",
                             "/tmp/echidna_green-red.file",
                             "/tmp/echidna_green-blue.file",
                             "/tmp/echidna_green-green.file"]

        self.assertItemsEqual(expected_outfiles, outfile_names)
