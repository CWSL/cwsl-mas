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

Tests for the ProcessUnit 'mapping' API - renaming constraints
between input and output.

"""

import unittest
import logging

import mock

from cwsl.configuration import configuration
from cwsl.core.constraint import Constraint
from cwsl.core.pattern_dataset import PatternDataSet
from cwsl.core.process_unit import ProcessUnit
from cwsl.core.metafile import MetaFile
from cwsl.core.file_creator import FileCreator


module_logger = logging.getLogger('cwsl.tests.test_mapping')


class TestMapping(unittest.TestCase):

    def setUp(self):
        
        self.input_pattern = '/a/fake/%gcm_model%_%variable%_%file_type%'
        self.output_pattern = '/a/fake/output/%obs_model%_%variable%_%file_type%'

        # Constant header for the job scripts.
        self.script_header = "#!/bin/sh\nset -e\n\nmodule purge\nexport CWSL_CTOOLS={}\nexport PYTHONPATH=$PYTHONPATH:{}/pythonlib\n"\
            .format(configuration.cwsl_ctools_path, configuration.cwsl_ctools_path)


    def test_pattern_dataset_mapping(self):
        
        with mock.patch('cwsl.core.pattern_dataset.PatternDataSet.glob_fs') as mock_glob:
            
            # Add the mock fake glob function.
            mock_glob.return_value = ["/a/file/green_monkey.txt",
                                      "/a/file/blue_monkey.txt"]
            
            test_patternds = PatternDataSet("/a/file/%colour%_%animal%.txt")

            # First get the files without mapping.
            non_mapped_files = test_patternds.get_files({'colour': 'blue',
                                                         'animal': 'monkey'})

            # Add a mapping, get files for a different constraint set.
            test_patternds.add_mapping('colour', 'new_colour')
            
            mapped_files = test_patternds.get_files({'new_colour': 'blue',
                                                     'animal': 'monkey'})

            fake_file = MetaFile('blue_monkey.txt', '/a/file',
                                 {'colour': 'blue',
                                  'animal': 'monkey'})

            self.assertEqual(fake_file.all_atts, mapped_files[0].all_atts)
            self.assertEqual(mapped_files, [fake_file])
            self.assertEqual(mapped_files, non_mapped_files)

    def test_filecreator_mapping(self):

        # Create a file creator object.
        the_file_creator = FileCreator("/a/file/%green%_%monkey%.txt",
                                       set([Constraint('animal', ['monkey']),
                                            Constraint('colour', ['green', 'blue'])]))

        assert False

    def test_simple_mapping(self):
        """ Test using constraints from the input in the output pattern. """

        with mock.patch('cwsl.core.pattern_dataset.PatternDataSet.glob_fs') as mock_glob:

            mock_file_1 = '/a/fake/ACCESS1-0_tas_netCDF'
            mock_file_2 = '/a/fake/MIROC_tas_netCDF'
            mock_file_3 = '/a/fake/ACCESS1-0_pr_netCDF'

            mock_glob.return_value = [mock_file_1, mock_file_2, mock_file_3]
        
            a_pattern_ds = PatternDataSet(self.input_pattern)

            the_process_unit = ProcessUnit([a_pattern_ds], self.output_pattern,
                                           'echo', map_dict={'obs_model': ('gcm_model', 0)})
            
            ds_result = the_process_unit.execute(simulate=True)
           
            outfiles = [file_thing for file_thing in ds_result.files]
            self.assertEqual(len(outfiles), 3)

            expected_string = (self.script_header + "mkdir -p /a/fake/output\necho /a/fake/MIROC_tas_netCDF /a/fake/output/MIROC_tas_netCDF\n" +
                               "echo /a/fake/ACCESS1-0_tas_netCDF /a/fake/output/ACCESS1-0_tas_netCDF\n" +
                               "echo /a/fake/ACCESS1-0_pr_netCDF /a/fake/output/ACCESS1-0_pr_netCDF\n")
            
            self.assertEqual(expected_string, the_process_unit.scheduler.job.to_str())

    def test_change_mapping(self):
        """ Test using multiple input datasets, like if you were calculating a change. """

        with mock.patch('cwsl.core.pattern_dataset.PatternDataSet.glob_fs') as mock_glob:
            fake_file_1 = '/a/fake/file_1956_red.nc'
            fake_file_2 = '/a/fake/file_1981_red.nc'

            mock_glob.return_value = [fake_file_1, fake_file_2]
            
            first_pattern_ds = PatternDataSet("/a/fake/file_%date%_%colour%.nc",
                                              set([Constraint('date', ['1956'])]))

            second_pattern_ds = PatternDataSet("/a/fake/file_%date%_%colour%.nc",
                                               set([Constraint('date', ['1981'])]))

            # Overwrite the valid combinations for these mock datasets.
            first_pattern_ds.valid_combinations = set([frozenset([Constraint('colour', ['red']),
                                                                  Constraint('date', ['1956'])])])

            second_pattern_ds.valid_combinations = set([frozenset([Constraint('colour', ['red']),
                                                                   Constraint('date', ['1981'])])])
            
            
            the_process_unit = ProcessUnit([first_pattern_ds, second_pattern_ds],
                                           "/a/final/output/file_%start_date%_%end_date%_%colour%.txt",
                                           'echo', map_dict={'start_date': ('date', 0),
                                                             'end_date': ('date', 1)})
        
            ds_result = the_process_unit.execute(simulate=True)
            
            outfiles = [file_thing for file_thing in ds_result.files]
            self.assertEqual(len(outfiles), 1)

            expected_string = self.script_header + "mkdir -p /a/final/output\necho /a/fake/file_1956_red.nc /a/fake/file_1981_red.nc /a/final/output/file_1956_1981_red.txt\n"     
            self.assertEqual(expected_string, the_process_unit.scheduler.job.to_str())

    def test_mapping_after_passing(self):
        """ Mapping of constraints should work if applied to the output of an earlier ProcessUnit. """

        # First create a basic process unit.
        with mock.patch('cwsl.core.pattern_dataset.PatternDataSet.glob_fs') as mock_glob:
            
            mock_glob.return_value = ['/a/fake/file/red_1986_2005_kangaroo.nc']

            first_patternds = PatternDataSet('/a/fake/file/%colour%_%start%_%end%_%animal%.nc')
            first_process = ProcessUnit([first_patternds], '/a/second/file_pattern/%colour%_%start%_%end%_%animal%.nc',
                                        'echo')
            result = first_process.execute(simulate=True)
            
            # Then take the output of that process and apply a mapping.
            second_process = ProcessUnit([result], '/a/final/pattern/%colour%_%hist_start%_%hist_end%_%animal%.txt',
                                         'echo', map_dict={'hist_start': ('start', 0),
                                                           'hist_end': ('end', 0)})

            final_out = second_process.execute(simulate=True)

            expected_string = (self.script_header + "mkdir -p /a/final/output\necho" +
                               "/a/second/file_pattern/red_1986_2005_kangaroo.nc " + 
                               "/a/final/pattern/red_1986_2005_kangaroo.nc")

            self.assertEqual(expected_string, second_process.scheduler.job.to_str())
            
