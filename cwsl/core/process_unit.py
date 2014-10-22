"""
Authors: Tim Bedin, Ricardo Pascual, Tim Erwin

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

This module contains the ProcessUnit class.

"""

import logging

from cwsl.core.argument_creator import ArgumentCreator
from cwsl.core.file_creator import FileCreator
from cwsl.core.constraint import Constraint
from cwsl.core.scheduler import SimpleExecManager

module_logger = logging.getLogger('cwsl.core.process_unit')


class ProcessUnit(object):
    """ This class sets up the execution of an operation
    performed on a DataSet.

    This class takes in a list of input DataSets, an output
    pattern to write files to, the shell command that needs to be
    run and any output_constraints that need to be added to the
    output.

    It sets up the ArgumentCreator classes required to
    run a particular, single processing operation for all of
    the possible combinations in the input and output DataSets.

    This could be done using a VisTrails module directly, but doing it like
    this allows us to separate the GUI presentation from the execution.

    """

    def __init__(self, inputlist, output_pattern, shell_command,
                 extra_constraints=set([]), map_dict={},
                 cons_keywords={}, positional_args=[],
                 module_depends=[]):

        """ The class takes in a DataSet object, constraints to change and
        the path to an executable. It has an self.execute() method that
        returns a FileCreator to be used as input to the next module.

        Extra constraints to be applied to the output are given by
        extra_constraints.

        map_dict is a dictionary linking constraint names in the input DataSets
        to new constraints in the output. e.g.
        if map_dict = {"model": "obs-model"} then the "model" constraint in the
        input is renamed to be the "obs-model" Constraint in the output.

        """

        if extra_constraints:
            module_logger.debug("extra_constraints are: {0}"
                                .format(extra_constraints))

        self.inputlist = inputlist
        self.cons_keywords = cons_keywords
        self.positional_args = positional_args
        self.module_depends = module_depends

        # The initial Constraints for the output are built from the
        # output file pattern.
        self.pattern_constraints = FileCreator.constraints_from_pattern(output_pattern)

        # Fill the empty output constraints from the input DataSets.
        fixed_constraints = self.fill_empty_constraints(extra_constraints)
        final_constraints = self.apply_mappings(map_dict, fixed_constraints)

        module_logger.debug("Final output constraints are: {0}".format(final_constraints))

        # Make a file_creator from the new, fixed constraints.
        self.file_creator = FileCreator(output_pattern, final_constraints)

        self.shell_command = shell_command

    def fill_empty_constraints(self, extra_constraints):

        out_cons_names = [cons.key for cons in extra_constraints]
        # These are constraints that exist in the pattern but not in
        # the extra_constraints - they must be filled from the input
        # if they exist there.
        unfilled_constraints = set([cons for cons in self.pattern_constraints
                                    if cons.key not in out_cons_names])

        empty_given_constraints = set([cons for cons in extra_constraints
                                       if not cons.values])

        missing_constraints = unfilled_constraints.union(empty_given_constraints)
        module_logger.debug("Constraints that need to be filled from the input are: {0}".
                            format(missing_constraints))

        module_logger.debug("Before filling, extra_constraints is: {0}"
                            .format(extra_constraints))

        # Any open constraints in the output should be filled by
        # the values from the input dataset.
        for empty in missing_constraints:
            found_constraints = []
            module_logger.debug("{0} is a missing constraint".format(empty))
            for input_ds in self.inputlist:
                found_constraints.append(input_ds.get_constraint(empty.key))

            filtered_founds = [cons for cons in found_constraints
                               if cons is not None]

            if len(filtered_founds) == 1:
                module_logger.debug("Adding Constraint {0}"
                                    .format(filtered_founds[0]))
                extra_constraints.add(filtered_founds[0])
            elif not filtered_founds:
                module_logger.debug("Replacing empty Constraint! {0}"
                                    .format(empty))
                extra_constraints.add(empty)
            else:
                names = [cons.key for cons in filtered_founds]
                all_vals = [cons.values for cons in filtered_founds]
                name = set(names)
                extra_vals = set()
                for vals in all_vals:
                    extra_vals.add(vals)
                new_con = Constraint(name, extra_vals)
                extra_constraints.add(new_con)
                module_logger.debug("(multiple filtered founds) Adding Constraint {0}"
                                    .format(new_con))

        module_logger.debug("Full output Constraints: {0}"
                            .format(extra_constraints))

        new_dict = {}
        for cons in extra_constraints:
            new_dict[cons.key] = set([])

        for constraint in extra_constraints:
            new_dict[constraint.key] = new_dict[constraint.key].union(constraint.values)

        fixed_constraints = set([Constraint(key, new_dict[key])
                                 for key in new_dict])
        return fixed_constraints

    def apply_mappings(self, map_dict, fixed_constraints):

        # Apply any "mappings" from the map_dict.
        module_logger.debug("map_dict for this ProcessUnit: {0}".format(map_dict))
        to_remove = []
        to_add = []

        for mapping in map_dict:
            in_name = mapping
            out_name = map_dict[mapping]
            module_logger.debug("Mapping info: inname: {0} outname {1}".format(in_name, out_name))

            module_logger.debug("searching for constraint for mapping: {0}".format(in_name))

            for cons in fixed_constraints:
                print cons.key, mapping
                if cons.key == out_name:
                    module_logger.debug("Replacing Constraint: {0}".format(cons))

                    # Look for the matching key in the inputs.
                    found_cons = [inDS.get_constraint(in_name) for inDS in self.inputlist]

                    to_remove.append(cons)
                    merged_cons = set(*[cons.values for cons in found_cons])
                    to_add.append(Constraint(out_name, merged_cons))

        for cons in to_remove:
            fixed_constraints.remove(cons)
        for cons in to_add:
            fixed_constraints.add(cons)

        return fixed_constraints

    def execute(self, simulate=False):
        """ This method runs the actual process."""

        # List to store commands for testing purposes.
        commands = []

        # We now create a looper to compare all the input Datasets with
        # the output fileCreators.

        module_logger.debug("Now creating ArgumentCreator")
        
        this_looper = ArgumentCreator(self.inputlist, self.file_creator)
        module_logger.debug("Created ArgumentCreator: {0}".format(this_looper))

        #TODO determine sceduling options
        scheduler = SimpleExecManager(noexec=simulate)
        scheduler.add_module_deps(self.module_depends)

        # For every valid possible combination, add the command to the scheduler.
        for combination in this_looper:
            module_logger.debug("Combination: " + str(combination))
            if combination:
                in_files, out_files = self.get_fullnames((combination[0], combination[1]))
                this_dict = combination[2]
                                
                module_logger.info("in_files are:")
                module_logger.info(in_files)
                module_logger.info("out_files are:")
                module_logger.info(out_files)

                # Now apply any keyword arguments.
                modified_command = self.apply_keyword_args(this_dict)
                
                # The subprocess / queue submission is done here.
                # The scheduler handles positional arguments.
                scheduler.add_cmd(modified_command, in_files, out_files,
                                  constraint_dict=this_dict, positional_args=self.positional_args)

        scheduler.submit()

        # The scheduler is kept for testing purposes.
        self.scheduler = scheduler

        return self.file_creator

    def apply_keyword_args(self, cons_dict, prefix='--'):
        """ Using the dictionary of keyword arguments, construct a shell command."""
        command = self.shell_command

        for keyword in self.cons_keywords:
            associated_cons_name = self.cons_keywords[keyword]
            this_att_value = cons_dict[associated_cons_name]

            command += (' ' + prefix + keyword + ' ' + this_att_value)

        return command

    def get_fullnames(self, combination):
        required_atts = ["path_dir", "filename"]
        in_files = []

        for qs in combination[0]:
            try:
                in_files += [infile.full_path
                             for infile in qs.only(*required_atts)]
            except AttributeError:
                in_files += [infile.full_path for infile in qs]

        out_files = []
        for qs in combination[1]:
            try:
                out_files += [outfile.full_path
                              for outfile in qs.only(*required_atts)]
            except AttributeError:
                out_files += [outfile.full_path for outfile in qs]

        return in_files, out_files
