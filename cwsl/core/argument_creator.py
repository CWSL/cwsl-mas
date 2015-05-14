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
import hashlib

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

        # Check for Constraint overwrites.
        for constraint in self.final_shared:
            out_cons = output_file_creator.get_constraint(constraint.key)
            if out_cons and (out_cons.values != constraint.values):
                module_logger.debug("Repeated Constraint found - removing.\n" +
                                    "old constraint: {}, new constraint: {}"
                                    .format(constraint, out_cons))
                self.final_shared.remove(constraint)

        module_logger.debug("Final shared constraints are: {}"
                            .format(self.final_shared))

        self.all_names = [cons.key for cons in self.final_shared]

        all_vals = [cons.values for cons in self.final_shared]
        self.all_combinations = itertools.product(*all_vals)


    def get_combinations(self):
        """ Return the next group of input and output file/metafile objects."""

        processed_hashes = []

        for comb in self.all_combinations:

            this_dict = {key: value for key, value in zip(self.all_names,
                                                          comb)}

            module_logger.debug("This constraint dictionary is: {}"
                                .format(this_dict))

            module_logger.debug("About to get files from output with Constraints: {}"
                                .format(self.output_file_creator.constraints))
            all_outs = self.output_file_creator.get_files(this_dict)

            # For every output file, grab the corresponding input files.
            for output in all_outs:
                out_hash = hashlib.md5(str(output)).hexdigest()
                module_logger.debug("Output file is: {}"
                                    .format(output))

                if out_hash in processed_hashes:
                    continue

                in_list = []
                all_atts = output.all_atts
                module_logger.debug("Original output attributes are: {}"
                                    .format(all_atts))
                for ds in self.input_datasets:
                    good_atts = {}
                    for thing, value in output.all_atts.items():
                        if thing in ds.cons_names:
                            in_con = ds.get_constraint(thing)
                            if value in in_con.values:
                                good_atts[thing] = value

                    module_logger.debug("Getting files from input - dictionary is: {}"
                                        .format(good_atts))
                    returned_files = ds.get_files(good_atts)
                    in_list += returned_files
                    # Update the attribute dictionary for keyword arguments
                    for returned_file in returned_files:
                        print(returned_file.all_atts)
                        all_atts.update(returned_file.all_atts)

                processed_hashes.append(out_hash)
                yield (in_list, [output], all_atts)
