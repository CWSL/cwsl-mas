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

import itertools
import logging

from cwsl.core.constraint import Constraint

module_logger = logging.getLogger('cwsl.core.argument_creator')



class ArgumentCreator(object):
    '''
    The ArgumentCreator class compares (one or more) input DataSets with
    (one or more) output FileCreators.

    There are different cases to consider -

    1. The ArgumentCreator adds/removes no attributes.
        In this case, there is a one-to-one mapping between input and output.

        Here the ArgumentCreator should return a tuple (inputfile, outputfile),
        one output file (from the file creator) for each file
        in the input DataSet.

    2. The output has more attributes than the input.

        Here each input file will create many output files.

        In this case, it returns a mapping like
        (inputfile1, outputfile1), (inputfile1, outputfile2) ...
        up to (inputfile1, outputfileN), where N is the number of
        allowed values of the added attribute.

        It will do this once for every file in the input Dataset.

    3. The ArgumentCreator removes one or more attributes.

        Here many input files will be combined to create a single outputfile.

        For this case, the looper will return something like
        ([inputfile1, inputfile2, ..., inputfileN], outputfile)

    There are also mixed cases in which these operations are combined.

    '''

    def __init__(self, input_datasets, output_file_creators):
        ''' The class takes in a list of DataSet objects for its input and
        a list of FileCreator objects for output.

        Optionally a map_dict can be passed in which maps a constraint in the
        input with one from the output.

        The FileCreator is associated with a DataSet to check
        if files already exist.

        '''

        self.inputs = input_datasets
        self.output = output_file_creators

        # Put the inputs/outputs into lists if they are not iterable.
        if not hasattr(self.inputs, '__iter__'):
            self.input = [self.inputs]
        if not hasattr(self.output, '__iter__'):
            self.output = [self.output]

        in_constraints = set.union(*[in_set.constraints
                                     for in_set in self.inputs])
        out_constraints = set.union(*[out_set.constraints
                                      for out_set in self.output])
        module_logger.debug('All input constraints are: ' + str(in_constraints))
        module_logger.debug('All output constraints are: ' + str(out_constraints))

        # Now apply the check constraints function to map
        # empty output constraints to the corresponding constraint in the input
        # and ensure that constraints are not repeated.
        self.input_cons, self.output_cons = self.check_constraints(in_constraints,
                                                                   out_constraints)
        
        # Find the shared attributes between the input and output.
        self.shared_constraints = self.input_cons.intersection(self.output_cons)

        # Find constraints that exist in the input but not the output - these
        # will be added to the returned combination dictionary for use in positional
        # or keyword arguments.
        self.input_only = self.input_cons.difference(self.output_cons)

        # The cons.values set is turned into a list so we can just get one value.
        module_logger.debug("self.input_only: {}".format(self.input_only))
        self.input_only_dict = {cons.key: list(cons.values)[0] for cons in self.input_only}

        # Output only constraints are effectively shared constraints and must be added to
        # the valid combinations and the shared_constraints set.
        module_logger.debug("Input constraints are: {}"
                            .format(self.input_cons))
        in_keys = [cons.key for cons in self.input_cons]
        module_logger.debug("Input constraint keys are: {}"
                            .format(in_keys))
        self.output_only = [cons for cons in self.output_cons
                            if cons.key not in in_keys]
        module_logger.debug("Output only constraints are: {}"
                            .format(self.output_only))
        
        self.shared_constraints = self.shared_constraints.union(self.output_only)

        module_logger.debug("Shared input and output constraints are: {0}".
                            format(self.shared_constraints))

        self.shared_keys = set([cons.key for cons in self.shared_constraints])

    def check_constraints(self, in_cons, out_cons):
        """ This method checks and cleans up any problems with input and
            output constraints in a logical manner. """

        # Check that there are no empty constraints in the input - just
        # to make sure!
        for in_constraint in in_cons:
            if in_constraint.values == set():
                module_logger.error("Constraint {0} has no values!".format(in_constraint))
            assert(in_constraint.values != set())

        count_dict = {}
        # Check for repeated constraints in the input.
        for cons in in_cons:
            try:
                count_dict[cons.key] += 1
            except KeyError:
                count_dict[cons.key] = 1
        # If a constraint is repeated, merge into one constraint.
        bad_names = []
        for name, count in count_dict.items():
            if count > 1:
                module_logger.debug("Constraint on {} is found {} times"
                                    .format(name, count))
                bad_names.append(name)
        for name in bad_names:
            match_cons = [cons for cons in in_cons
                          if cons.key == name]
            module_logger.debug("Merging constraints: {}"
                                .format(match_cons))
            all_vals = set.union(*[cons.values for cons in match_cons])
            new_constraint = Constraint(name, all_vals)
            module_logger.debug("New constraint is: {}"
                                .format(new_constraint))
            in_cons.add(new_constraint)
            for cons in match_cons:
                in_cons.remove(cons)

        # Now fix up empty output constraints.
        new_outs = set()
        for out_constraint in out_cons:
            if out_constraint.values:
                new_outs.add(out_constraint)
            else:
                # If it is empty, get the matching constraint
                # from the input.
                new_cons = [in_con for in_con in new_ins
                            if in_con.key == out_constraint.key]
                assert(len(new_cons) == 1)
                if not new_cons:
                    module_logger.error("Output constraint {0} has no values!"
                                        .format(out_constraints))
                new_outs.add(*new_cons)

        return in_cons, new_outs

    def __iter__(self):
        # Sets up the combinations in the argument_creator for looping.

        valids = [ds.valid_combinations for ds in self.inputs]
        for combination in valids:
            module_logger.debug("Valid combination: {}".format(combination))

        set_of_valids = set.union(*valids)
        
        if self.output_only:
            module_logger.debug("Output only constraints are: {}"
                                .format(self.output_only))
            self.valid_iter = self.custom_iterator(set_of_valids, self.output_only)
        else:
            self.valid_iter = iter(set_of_valids)

        # Store which combinations of constraints
        # have been processed this iteration.
        self.done_combinations = []

        return self

    def custom_iterator(self, valid_set, output_cons):
        """ This iterator adds any output_only constraints to the valid combinations from the input."""

        module_logger.debug("Valid set is: {}, output_cons is: {}"
                            .format(valid_set, output_cons))
        
        all_possibles = []
        for combination in itertools.product(*output_cons):
            this_com_list = []
            for cons in combination:
                this_com_list.append(Constraint(cons[0], [cons[1]]))
            all_possibles.append(this_com_list)
                                     
        for items in itertools.product(iter(valid_set), all_possibles):
            input_list = list(items[0])
            input_list += items[1]
            module_logger.debug("Input list is: {}".format(input_list))
            yield(set(input_list))
        
    def next(self):
        """ Return the next group of input and output file/metafile objects."""

        next_in = None
        next_out = None

        while not next_in and not next_out:

            # Get a particular set of valid constraints from the input.
            this_combination_vals = self.valid_iter.next()
            module_logger.debug("This combination values are: {}"
                                .format(this_combination_vals))
            
            # Get the subset of this combination which is in the shared
            # constraints.
            this_shared = [cons for cons in this_combination_vals
                           if cons.key in self.shared_keys]

            this_combination_dict = {}
            for cons in this_shared:
                this_combination_dict[cons.key] = iter(cons.values).next()

            module_logger.debug("This combination is: {0}"
                                .format(this_combination_dict))

            # If this combination has been done, continue.
            this_hash = hash(frozenset(this_combination_dict.items()))
            if this_hash in self.done_combinations:
                module_logger.debug("The combination with hash {0} has already been processed".format(this_hash))
                continue

            # Get the matching files for this combination of constraints.
            in_metas = []
            out_metas = []

            for input_ds in self.inputs:
                in_metas.append(input_ds.get_files(this_combination_dict,
                                check=True))

            module_logger.debug("Found matching inputs: {0}".format(in_metas))
            # We only need to look for outputs if there is an input file present
            # for every input dataset.
            module_logger.debug("Length of in_metas: {}, length of self.inputs: {}"
                                .format(len(in_metas), len(self.inputs)))
            module_logger.debug("self.inputs: {}".format(self.inputs))
            if len(in_metas) != len(self.inputs):
                in_metas = []

            if in_metas[0]:
                module_logger.debug("Searching for output metafiles.")

                for output_ds in self.output:
                    these_files = output_ds.get_files(this_combination_dict,
                                                      check=False, update=True)
                    if these_files is not None:
                        out_metas.append(these_files)
            else:
                module_logger.debug("No files found for this combination.")

            next_in = in_metas
            next_out = out_metas

            # This combination is done, add its hash to the list.
            self.done_combinations.append(this_hash)

            # Returns a tuple of lists if a file exists, otherwise loop again.
            if next_in and next_out:
                full_return = dict(this_combination_dict.items() + self.input_only_dict.items())
                return (next_in, next_out, full_return)
            else:
                # If there is no input metafiles,
                # call for the next combination.
                continue
