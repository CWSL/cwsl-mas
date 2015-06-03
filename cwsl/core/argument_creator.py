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
from cwsl.core.metafile import MetaFile

module_logger = logging.getLogger('cwsl.core.argument_creator')


class ArgumentCreator(object):
    """ The ArgumentCreator. """

    def __init__(self, input_datasets, output_file_creator,
                 merge_output=None):
        ''' The class takes in a list of DataSet objects for its input and
        a FileCreator object for output.

        Optionally, a merge_output list can be passed in which will merge
        the constraint values for all input datasets into one.

        '''

        self.merge_output = merge_output

        self.output_file_creator = output_file_creator
        self.input_datasets = input_datasets

        # Find the Constraint names shared across input.
        all_constraints = [set(dataset.cons_names) for dataset in input_datasets]
        self.shared_constraints = set.intersection(*all_constraints)

        # Now get the values for the shared names.
        all_values = []
        for name in self.shared_constraints:
            for ds in input_datasets:
                new_con = ds.get_constraint(name)
                if new_con:
                    all_values.append(new_con)

        module_logger.debug("All values are: {}"
                            .format(all_values))

        # Remove values that are not in the output.
        out_cons_names = self.output_file_creator.cons_names
        module_logger.debug("Output constraints are: {}".format(out_cons_names))
        to_remove = []
        all_values = [cons for cons in all_values
                      if cons.key in out_cons_names]

        # Remove values that are not in all inputs.
        self.final_shared = []
        for name in self.shared_constraints:
            temp_list = []
            for constraint in all_values:
                if constraint.key == name:
                    temp_list.append(constraint.values)

            if(temp_list):
                self.final_shared.append(Constraint(name, set.intersection(*temp_list)))

        # Check for Constraint overwrites.
        to_remove = []
        for constraint in self.final_shared:
            out_cons = output_file_creator.get_constraint(constraint.key)
            if out_cons and (out_cons.values != constraint.values):
                to_remove.append(constraint)

        # Remove the bad constraints.
        for constraint in to_remove:
            self.final_shared.remove(constraint)

        self.all_names = [cons.key for cons in self.final_shared]

        all_vals = [cons.values for cons in self.final_shared]
        self.all_combinations = itertools.product(*all_vals)

    def __iter__(self):
        "Set up the ArgumentCreator as an iterator"

        return self.get_combinations()

    def get_combinations(self):
        """ Return the next group of input and output file/metafile objects."""

        processed_hashes = []

        for comb in self.all_combinations:
            this_dict = {key: value for key, value
                         in zip(self.all_names, comb)}

            all_outs = self.output_file_creator.get_files(this_dict, update=False,
                                                          check=False)

            # For every output file, grab the corresponding input files.
            for output in all_outs:
                in_list = []
                all_atts = output.all_atts
                module_logger.debug("Original output attributes are: {}"
                                    .format(all_atts))
                input_atts = {}
                for ds in self.input_datasets:
                    good_atts = {}
                    for thing, value in output.all_atts.items():
                        if thing in ds.cons_names:
                            in_con = ds.get_constraint(thing)
                            if in_con and (value in in_con.values):
                                good_atts[thing] = value

                    returned_files = ds.get_files(good_atts, check=True, update=False)
                    in_list += returned_files

                    # Update the attribute dictionary for keyword arguments
                    for returned_file in returned_files:
                        input_atts.update(returned_file.all_atts)

                input_atts.update(all_atts)
                if in_list:
                    # This is a good combination - update the output and apply
                    # any constraint merge.
                    final_atts = self.merge_outcons(input_atts, in_list)

                    module_logger.debug("Performing the output overwrite")
                    output_overwrite = self.output_file_creator.get_files(final_atts, update=True,
                                                                          check=False)
                    module_logger.debug("All the valid output files: {}"
                                        .format([thing for thing in self.output_file_creator.files]))
                    out_hash = hash(output_overwrite[0])
                    if out_hash in processed_hashes:
                        continue
                    else:
                        processed_hashes.append(out_hash)
                        module_logger.debug("Yielding a combination of input and output")
                        yield (in_list, output_overwrite, final_atts)
                else:
                    module_logger.debug("No combination found!")

    def merge_outcons(self, attdict, input_list):
        """ This function applies the merge_output list.

        This merges the constraints into and updates the output file creator.

        """

        if not self.merge_output:
            return attdict

        for consname in self.merge_output:
            thiscons_list = []
            for metafile in input_list:
                thiscons_list.append(metafile.all_atts[consname])

            attdict[consname] = '-'.join(thiscons_list)

        module_logger.debug("Returning dictionary after merging: {}"
                            .format(attdict))

        # Update the constraints of the output.
        for consname in self.merge_output:
            old_con = (self.output_file_creator
                       .get_constraint(consname)
                       .values.add(attdict[consname]))

        return attdict
