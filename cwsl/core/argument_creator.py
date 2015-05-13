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

Contains the ArgumentCreator class.

"""

import logging
import itertools

from cwsl.core.constraint import Constraint

module_logger = logging.getLogger('cwsl.core.argument_creator')


class ArgumentCreator(object):
    """ The ArgumentCreator. """

    def __init__(self, input_datasets, output_file_creator,
                 map_dict=None):
        ''' The class takes in a list of DataSet objects for its input and
        a FileCreator object for output.

        Optionally a map_dict can be passed in which maps a constraint in the
        input with one from the output.

        '''

        self.output_file_creator = output_file_creator
        self.input_datasets = input_datasets

        # Find the Constraint names shared across input.
        all_constraints = [set(dataset.cons_names) for dataset in input_datasets]
        self.shared_constraints = set.intersection(*all_constraints)
        
        # Now get the values for the shared names.
        all_values = []
        for name in self.shared_constraints:
            for ds in input_datasets:
                all_values.append(ds.get_constraint(name))
                                 
        module_logger.debug("All values are: {}"
                            .format(all_values))

        # Remove values that are not in all inputs and outputs.
        self.final_shared = []
        for name in self.shared_constraints:
            temp_list = []
            for constraint in all_values:
                if constraint.key == name:
                    temp_list.append(constraint.values)
            self.final_shared.append(Constraint(name, set.intersection(*temp_list)))

        module_logger.debug("Final shared constraints are: {}"
                            .format(self.final_shared))

        self.all_names = [cons.key for cons in self.final_shared]

        all_vals = [cons.values for cons in self.final_shared]
        self.all_combinations = itertools.product(*all_vals)


    def get_combinations(self):
        """ Return the next group of input and output file/metafile objects."""

        for comb in self.all_combinations:
        
            this_dict = {key: value for key, value in zip(self.all_names,
                                                          comb)}
        
            module_logger.debug("This constraint dictionary is: {}"
                                .format(this_dict))

            all_outs = self.output_file_creator.get_files(this_dict)
        
            # For every output file, grab the corresponding input files.
            for output in all_outs:
                module_logger.debug("Output file is: {}"
                                    .format(output))
                in_list = []
                all_atts = {}
                for ds in self.input_datasets:
                    good_atts = {}
                    for thing, value in output.all_atts.items():
                        if thing in ds.cons_names:
                            good_atts[thing] = value
                        
                    in_list += ds.get_files(good_atts)
                    all_atts.update(good_atts)

                yield (in_list, [output], all_atts)
