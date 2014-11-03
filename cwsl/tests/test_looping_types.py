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

Tests for ArgumentCreator looping types.

"""

import unittest
import logging
from copy import deepcopy

import mock

from cwsl.core.process_unit import ProcessUnit
from cwsl.core.pattern_dataset import PatternDataSet
from cwsl.core.constraint import Constraint
from cwsl.core.file_creator import FileCreator


module_logger = logging.getLogger('cwsl.tests.test_looping_types')

# class TestLooping(unittest.TestCase):

#     def test_change_looping(self):
#         """ The system should be able to compare input datasets to calculate change. """

#         with mock.patch('cwsl.core.pattern_dataset.PatternDataSet.glob_fs') as mock_glob_1:
#             mock_glob_2 = deepcopy(mock_glob_1)
            
#             input_pattern = "/fake/%period%_%start%_%end%.nc"
#             self.output_pattern = "/fake/%end_period%_%fut_start%_%fut_end%-wrt-%hist_start%_%hist_end%.nc"
        
#             mock_glob_1.return_value = ["/fake/rcp85_1900_2005.nc"]
#             mock_glob_2.return_value = ["/fake/historical_1900_2005.nc"]
        
#             self.start_ds = PatternDataSet(input_pattern,
#                                            set([Constraint('period', ['historical'])]))
        
#             self.end_ds = PatternDataSet(input_pattern,
#                                          set([Constraint('period', ['rcp85'])]))

#             our_process_unit = ProcessUnit([self.start_ds, self.end_ds],
#                                            self.output_pattern,
#                                            'echo',
#                                            map_dict={'hist_start': ('start', 0),
#                                                      'hist_end': ('end', 0),
#                                                      'fut_start': ('start', 1),
#                                                      'fut_end': ('end', 1),
#                                                      'end_period': ('period', 1)})
                                       
#             ds_result = our_process_unit.execute(simulate=True)

#             assert False
    
